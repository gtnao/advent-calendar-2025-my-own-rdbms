use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::buffer_pool::BufferPoolManager;
use crate::executor::Rid;
use crate::page::Page;
use crate::wal::{CLRRedo, Lsn, WalManager, WalRecord, WalRecordType};

/// ARIES-style crash recovery manager.
///
/// Recovery phases:
/// 1. Analysis: Scan WAL to determine committed/uncommitted transactions
/// 2. Redo: Replay ALL logged operations (including CLRs)
/// 3. Undo: Rollback uncommitted transactions using CLRs
pub struct RecoveryManager;

impl RecoveryManager {
    /// Perform crash recovery.
    /// Returns the max LSN found in WAL and max transaction ID.
    pub fn recover(
        bpm: &Arc<Mutex<BufferPoolManager>>,
        wal_manager: &Arc<WalManager>,
        wal_records: &[WalRecord],
    ) -> Result<RecoveryStats> {
        if wal_records.is_empty() {
            return Ok(RecoveryStats::default());
        }

        println!("[Recovery] Starting ARIES crash recovery...");

        // Phase 1: Analysis
        let analysis = Self::analyze(wal_records);
        println!(
            "[Recovery] Analysis: {} committed, {} uncommitted transactions",
            analysis.committed_txns.len(),
            analysis.uncommitted_txns.len()
        );

        // Phase 2: Redo (replay ALL operations)
        let redo_count = Self::redo(bpm, wal_records)?;
        println!("[Recovery] Redo: replayed {redo_count} operations");

        // Phase 3: Undo (rollback uncommitted transactions)
        let undo_count = Self::undo(bpm, wal_manager, wal_records, &analysis)?;
        println!("[Recovery] Undo: rolled back {undo_count} operations");

        // Flush recovered data to disk
        bpm.lock().unwrap().flush_all()?;

        println!("[Recovery] Crash recovery completed.");

        Ok(RecoveryStats {
            committed_txns: analysis.committed_txns.len(),
            uncommitted_txns: analysis.uncommitted_txns.len(),
            redo_operations: redo_count,
            undo_operations: undo_count,
            max_lsn: analysis.max_lsn,
            max_txn_id: analysis.max_txn_id,
        })
    }

    /// Analysis phase: Scan WAL from beginning to determine transaction states
    fn analyze(wal_records: &[WalRecord]) -> AnalysisResult {
        let mut active_txns: HashSet<u64> = HashSet::new();
        let mut committed_txns: HashSet<u64> = HashSet::new();
        let mut last_lsn_map: HashMap<u64, Lsn> = HashMap::new();
        let mut max_lsn: Lsn = 0;
        let mut max_txn_id: u64 = 0;

        for record in wal_records {
            max_lsn = max_lsn.max(record.lsn);
            max_txn_id = max_txn_id.max(record.txn_id);
            last_lsn_map.insert(record.txn_id, record.lsn);

            match &record.record_type {
                WalRecordType::Begin => {
                    active_txns.insert(record.txn_id);
                }
                WalRecordType::Commit => {
                    active_txns.remove(&record.txn_id);
                    committed_txns.insert(record.txn_id);
                }
                WalRecordType::Abort => {
                    // Transaction already rolled back before crash
                    active_txns.remove(&record.txn_id);
                }
                WalRecordType::Insert { .. }
                | WalRecordType::Delete { .. }
                | WalRecordType::CLR { .. } => {
                    // Data operations - transaction should be active
                }
            }
        }

        AnalysisResult {
            committed_txns,
            uncommitted_txns: active_txns,
            last_lsn_map,
            max_lsn,
            max_txn_id,
        }
    }

    /// Redo phase: Replay ALL operations from WAL
    /// ARIES redoes everything, then undoes uncommitted transactions
    fn redo(bpm: &Arc<Mutex<BufferPoolManager>>, wal_records: &[WalRecord]) -> Result<usize> {
        let mut count = 0;

        for record in wal_records {
            match &record.record_type {
                WalRecordType::Insert { rid, data } => {
                    if Self::should_redo(bpm, rid.page_id, record.lsn)? {
                        Self::redo_insert(bpm, rid, data, record.lsn)?;
                        count += 1;
                    }
                }
                WalRecordType::Delete { rid, .. } => {
                    if Self::should_redo(bpm, rid.page_id, record.lsn)? {
                        Self::redo_delete(bpm, rid, record.lsn)?;
                        count += 1;
                    }
                }
                WalRecordType::CLR { redo, .. } => {
                    // Redo CLRs too
                    match redo {
                        CLRRedo::UndoInsert { rid } => {
                            if Self::should_redo(bpm, rid.page_id, record.lsn)? {
                                Self::redo_delete(bpm, rid, record.lsn)?;
                                count += 1;
                            }
                        }
                        CLRRedo::UndoDelete { rid, data } => {
                            if Self::should_redo(bpm, rid.page_id, record.lsn)? {
                                Self::redo_restore(bpm, rid, data, record.lsn)?;
                                count += 1;
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(count)
    }

    /// Check if we should redo this operation by comparing page LSN
    fn should_redo(bpm: &Arc<Mutex<BufferPoolManager>>, page_id: u32, record_lsn: Lsn) -> Result<bool> {
        let page_count = bpm.lock().unwrap().page_count();
        if page_id >= page_count {
            // Page doesn't exist yet, need to redo
            return Ok(true);
        }

        let page_arc = bpm.lock().unwrap().fetch_page(page_id)?;
        let page_lsn = {
            let page = page_arc.read().unwrap();
            page.page_lsn
        };
        bpm.lock().unwrap().unpin_page(page_id, false)?;

        // Redo if page_lsn < record_lsn (operation not yet applied)
        Ok(page_lsn < record_lsn)
    }

    fn redo_insert(bpm: &Arc<Mutex<BufferPoolManager>>, rid: &Rid, data: &[u8], lsn: Lsn) -> Result<()> {
        let mut bpm_guard = bpm.lock().unwrap();

        // Ensure page exists
        while bpm_guard.page_count() <= rid.page_id {
            bpm_guard.new_page()?;
        }

        let page_arc = bpm_guard.fetch_page_mut(rid.page_id)?;
        let mut page = page_arc.write().unwrap();

        // Insert at specific slot
        Self::insert_at_slot(&mut page, rid.slot_id, data)?;
        page.page_lsn = lsn;

        drop(page);
        bpm_guard.unpin_page(rid.page_id, true)?;

        Ok(())
    }

    fn redo_delete(bpm: &Arc<Mutex<BufferPoolManager>>, rid: &Rid, lsn: Lsn) -> Result<()> {
        let mut bpm_guard = bpm.lock().unwrap();

        if bpm_guard.page_count() <= rid.page_id {
            return Ok(());
        }

        let page_arc = bpm_guard.fetch_page_mut(rid.page_id)?;
        let mut page = page_arc.write().unwrap();

        // Delete if tuple exists
        if page.get_tuple(rid.slot_id).is_some() {
            page.delete(rid.slot_id)?;
        }
        page.page_lsn = lsn;

        drop(page);
        bpm_guard.unpin_page(rid.page_id, true)?;

        Ok(())
    }

    fn redo_restore(bpm: &Arc<Mutex<BufferPoolManager>>, rid: &Rid, data: &[u8], lsn: Lsn) -> Result<()> {
        let mut bpm_guard = bpm.lock().unwrap();

        // Ensure page exists
        while bpm_guard.page_count() <= rid.page_id {
            bpm_guard.new_page()?;
        }

        let page_arc = bpm_guard.fetch_page_mut(rid.page_id)?;
        let mut page = page_arc.write().unwrap();

        // Restore the tuple
        if page.get_tuple(rid.slot_id).is_none() {
            page.restore(rid.slot_id, data)?;
        }
        page.page_lsn = lsn;

        drop(page);
        bpm_guard.unpin_page(rid.page_id, true)?;

        Ok(())
    }

    /// Undo phase: Rollback uncommitted transactions
    /// Uses prev_lsn chain and undo_next_lsn from CLRs
    fn undo(
        bpm: &Arc<Mutex<BufferPoolManager>>,
        wal_manager: &Arc<WalManager>,
        wal_records: &[WalRecord],
        analysis: &AnalysisResult,
    ) -> Result<usize> {
        if analysis.uncommitted_txns.is_empty() {
            return Ok(0);
        }

        // Build LSN -> record index map for efficient lookup
        let lsn_to_record: HashMap<Lsn, &WalRecord> = wal_records
            .iter()
            .map(|r| (r.lsn, r))
            .collect();

        // For each uncommitted transaction, find the undo_next_lsn to start from
        // This could be last_lsn or undo_next_lsn from a CLR if we crashed during undo
        let mut undo_next: HashMap<u64, Lsn> = HashMap::new();
        for txn_id in &analysis.uncommitted_txns {
            if let Some(&last_lsn) = analysis.last_lsn_map.get(txn_id) {
                // Check if the last record is a CLR - if so, use its undo_next_lsn
                if let Some(record) = lsn_to_record.get(&last_lsn) {
                    match &record.record_type {
                        WalRecordType::CLR { undo_next_lsn, .. } => {
                            undo_next.insert(*txn_id, *undo_next_lsn);
                        }
                        _ => {
                            undo_next.insert(*txn_id, last_lsn);
                        }
                    }
                }
            }
        }

        let mut count = 0;
        let mut last_lsn_for_txn: HashMap<u64, Lsn> = analysis.last_lsn_map.clone();

        // Keep undoing until all uncommitted transactions are done
        loop {
            // Find the transaction with the highest undo_next LSN
            let next_to_undo = undo_next
                .iter()
                .filter(|(_, lsn)| **lsn > 0)
                .max_by_key(|(_, lsn)| *lsn)
                .map(|(txn_id, lsn)| (*txn_id, *lsn));

            let (txn_id, lsn) = match next_to_undo {
                Some(v) => v,
                None => break,
            };

            let record = match lsn_to_record.get(&lsn) {
                Some(r) => r,
                None => {
                    undo_next.remove(&txn_id);
                    continue;
                }
            };

            // Get current last_lsn for this transaction (for CLR prev_lsn)
            let current_last_lsn = *last_lsn_for_txn.get(&txn_id).unwrap_or(&0);

            match &record.record_type {
                WalRecordType::Insert { rid, .. } => {
                    // Undo INSERT by deleting
                    Self::undo_insert(bpm, rid)?;

                    // Write CLR
                    let clr_lsn = wal_manager.append(
                        txn_id,
                        current_last_lsn,
                        WalRecordType::CLR {
                            undo_next_lsn: record.prev_lsn,
                            redo: CLRRedo::UndoInsert { rid: *rid },
                        },
                    );

                    // Update page_lsn
                    {
                        let page_arc = bpm.lock().unwrap().fetch_page_mut(rid.page_id)?;
                        let mut page = page_arc.write().unwrap();
                        page.page_lsn = clr_lsn;
                        drop(page);
                        bpm.lock().unwrap().unpin_page(rid.page_id, true)?;
                    }

                    last_lsn_for_txn.insert(txn_id, clr_lsn);
                    undo_next.insert(txn_id, record.prev_lsn);
                    count += 1;
                }
                WalRecordType::Delete { rid, data } => {
                    // Undo DELETE by restoring
                    Self::undo_delete(bpm, rid, data)?;

                    // Write CLR
                    let clr_lsn = wal_manager.append(
                        txn_id,
                        current_last_lsn,
                        WalRecordType::CLR {
                            undo_next_lsn: record.prev_lsn,
                            redo: CLRRedo::UndoDelete {
                                rid: *rid,
                                data: data.clone(),
                            },
                        },
                    );

                    // Update page_lsn
                    {
                        let page_arc = bpm.lock().unwrap().fetch_page_mut(rid.page_id)?;
                        let mut page = page_arc.write().unwrap();
                        page.page_lsn = clr_lsn;
                        drop(page);
                        bpm.lock().unwrap().unpin_page(rid.page_id, true)?;
                    }

                    last_lsn_for_txn.insert(txn_id, clr_lsn);
                    undo_next.insert(txn_id, record.prev_lsn);
                    count += 1;
                }
                WalRecordType::CLR { undo_next_lsn, .. } => {
                    // Skip CLRs, just follow the undo_next_lsn chain
                    undo_next.insert(txn_id, *undo_next_lsn);
                }
                WalRecordType::Begin => {
                    // Reached the beginning of transaction, write Abort and done
                    wal_manager.append(
                        txn_id,
                        current_last_lsn,
                        WalRecordType::Abort,
                    );
                    undo_next.remove(&txn_id);
                }
                _ => {
                    undo_next.insert(txn_id, record.prev_lsn);
                }
            }
        }

        // Flush WAL after undo
        wal_manager.flush();

        Ok(count)
    }

    fn undo_insert(bpm: &Arc<Mutex<BufferPoolManager>>, rid: &Rid) -> Result<()> {
        let mut bpm_guard = bpm.lock().unwrap();

        if bpm_guard.page_count() <= rid.page_id {
            return Ok(());
        }

        let page_arc = bpm_guard.fetch_page_mut(rid.page_id)?;
        let mut page = page_arc.write().unwrap();

        if page.get_tuple(rid.slot_id).is_some() {
            page.delete(rid.slot_id)?;
        }

        drop(page);
        bpm_guard.unpin_page(rid.page_id, true)?;

        Ok(())
    }

    fn undo_delete(bpm: &Arc<Mutex<BufferPoolManager>>, rid: &Rid, data: &[u8]) -> Result<()> {
        let mut bpm_guard = bpm.lock().unwrap();

        // Ensure page exists
        while bpm_guard.page_count() <= rid.page_id {
            bpm_guard.new_page()?;
        }

        let page_arc = bpm_guard.fetch_page_mut(rid.page_id)?;
        let mut page = page_arc.write().unwrap();

        page.restore(rid.slot_id, data)?;

        drop(page);
        bpm_guard.unpin_page(rid.page_id, true)?;

        Ok(())
    }

    /// Insert data at a specific slot (for recovery)
    fn insert_at_slot(page: &mut Page, slot_id: u16, data: &[u8]) -> Result<()> {
        // If this is a new slot at the end, use normal insert
        if slot_id == page.tuple_count() {
            page.insert(data)?;
            return Ok(());
        }

        // If slot exists but is deleted, restore it
        if slot_id < page.tuple_count() && page.get_tuple(slot_id).is_none() {
            page.restore(slot_id, data)?;
            return Ok(());
        }

        // Slot already has data - this is fine in recovery (already applied)
        Ok(())
    }
}

struct AnalysisResult {
    committed_txns: HashSet<u64>,
    uncommitted_txns: HashSet<u64>,
    last_lsn_map: HashMap<u64, Lsn>,
    max_lsn: Lsn,
    max_txn_id: u64,
}

#[allow(dead_code)]
#[derive(Default, Debug)]
pub struct RecoveryStats {
    pub committed_txns: usize,
    pub uncommitted_txns: usize,
    pub redo_operations: usize,
    pub undo_operations: usize,
    pub max_lsn: Lsn,
    pub max_txn_id: u64,
}
