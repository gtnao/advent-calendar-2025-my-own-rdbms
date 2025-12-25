use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::buffer_pool::BufferPoolManager;
use crate::checkpoint;
use crate::executor::Rid;
use crate::page::HeapPage;
use crate::transaction_manager::{TransactionManager, TxnStatus};
use crate::wal::{CLRRedo, Lsn, WalManager, WalRecord, WalRecordType, read_wal_records_from};

/// ARIES-style crash recovery manager with Fuzzy Checkpoint support.
///
/// Recovery phases:
/// 1. Analysis: Scan WAL from checkpoint to determine ATT/DPT
/// 2. Redo: Replay operations from min(rec_lsn in DPT)
/// 3. Undo: Rollback uncommitted transactions using CLRs
pub struct RecoveryManager;

impl RecoveryManager {
    /// Perform crash recovery with checkpoint support.
    /// Returns the max LSN found in WAL and max transaction ID.
    pub fn recover(
        data_dir: &str,
        wal_dir: &str,
        bpm: &Arc<Mutex<BufferPoolManager>>,
        wal_manager: &Arc<WalManager>,
        txn_manager: &Arc<TransactionManager>,
    ) -> Result<RecoveryStats> {
        println!("[Recovery] Starting ARIES crash recovery with checkpoint support...");

        // Read checkpoint metadata (LSN and next_txn_id)
        let checkpoint_meta = checkpoint::read_checkpoint_meta(data_dir)?;
        println!("[Recovery] Checkpoint LSN: {:?}, next_txn_id: {}",
            checkpoint_meta.checkpoint_lsn, checkpoint_meta.next_txn_id);

        // CLOG is now page-based and loaded on demand by TransactionManager

        // Read WAL records from checkpoint position (or beginning if no checkpoint)
        let wal_records = read_wal_records_from(wal_dir, checkpoint_meta.checkpoint_lsn)?;

        if wal_records.is_empty() {
            println!("[Recovery] No WAL records to process.");
            // Set next_txn_id from checkpoint
            if checkpoint_meta.next_txn_id > 0 {
                txn_manager.set_next_txn_id(checkpoint_meta.next_txn_id);
            }
            return Ok(RecoveryStats::default());
        }

        // Phase 1: Analysis - find checkpoint record and build ATT/DPT
        let analysis = Self::analyze(&wal_records, checkpoint_meta.checkpoint_lsn);
        println!(
            "[Recovery] Analysis: {} committed, {} uncommitted transactions",
            analysis.committed_txns.len(),
            analysis.uncommitted_txns.len()
        );
        println!(
            "[Recovery] DPT has {} dirty pages, redo start LSN: {:?}",
            analysis.dpt.len(),
            analysis.dpt.values().min()
        );

        // Update CLOG with committed transactions found in WAL (after checkpoint)
        for txn_id in &analysis.committed_txns {
            txn_manager.set_txn_status(*txn_id, TxnStatus::Committed);
        }
        // Aborted transactions found in WAL
        for txn_id in &analysis.aborted_txns {
            txn_manager.set_txn_status(*txn_id, TxnStatus::Aborted);
        }

        // Phase 2: Redo - replay from min(rec_lsn in DPT)
        let redo_start_lsn = analysis.dpt.values().min().copied();
        let redo_count = Self::redo(bpm, &wal_records, redo_start_lsn)?;
        println!("[Recovery] Redo: replayed {redo_count} operations");

        // Phase 3: Undo - rollback uncommitted transactions
        let undo_count = Self::undo(bpm, wal_manager, &wal_records, &analysis)?;
        println!("[Recovery] Undo: rolled back {undo_count} operations");

        // Mark uncommitted transactions as aborted (they were rolled back in undo phase)
        for txn_id in &analysis.uncommitted_txns {
            txn_manager.set_txn_status(*txn_id, TxnStatus::Aborted);
        }

        // Restore TransactionManager state
        // Use max of WAL max_txn_id and checkpoint next_txn_id
        let max_txn_id = analysis.max_txn_id.max(checkpoint_meta.next_txn_id);
        txn_manager.set_next_txn_id(max_txn_id.max(1));

        // Restore DPT to BufferPoolManager
        bpm.lock().unwrap().restore_dpt(analysis.dpt.clone());

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

    /// Analysis phase: Scan WAL from checkpoint to determine ATT/DPT
    fn analyze(wal_records: &[WalRecord], checkpoint_lsn: Option<Lsn>) -> AnalysisResult {
        let mut att: HashMap<u64, Lsn> = HashMap::new();  // Active Transaction Table
        let mut dpt: HashMap<u32, Lsn> = HashMap::new();  // Dirty Page Table
        let mut committed_txns: HashSet<u64> = HashSet::new();
        let mut aborted_txns: HashSet<u64> = HashSet::new();
        let mut max_lsn: Lsn = 0;
        let mut max_txn_id: u64 = 0;

        for record in wal_records {
            max_lsn = max_lsn.max(record.lsn);
            max_txn_id = max_txn_id.max(record.txn_id);

            match &record.record_type {
                WalRecordType::Checkpoint { att: ckpt_att, dpt: ckpt_dpt } => {
                    // If this is the checkpoint we were looking for, restore ATT/DPT
                    if checkpoint_lsn == Some(record.lsn) {
                        att = ckpt_att.clone();
                        dpt = ckpt_dpt.clone();
                        println!("[Recovery] Restored ATT ({} entries) and DPT ({} entries) from checkpoint",
                            att.len(), dpt.len());
                    }
                }
                WalRecordType::Begin => {
                    att.insert(record.txn_id, record.lsn);
                }
                WalRecordType::Commit => {
                    att.remove(&record.txn_id);
                    committed_txns.insert(record.txn_id);
                }
                WalRecordType::Abort => {
                    att.remove(&record.txn_id);
                    aborted_txns.insert(record.txn_id);
                }
                WalRecordType::Insert { rid, .. }
                | WalRecordType::Delete { rid, .. } => {
                    // Update ATT
                    att.insert(record.txn_id, record.lsn);
                    // Update DPT - only record first LSN that dirtied the page
                    dpt.entry(rid.page_id).or_insert(record.lsn);
                }
                WalRecordType::CLR { redo, .. } => {
                    // Update ATT
                    att.insert(record.txn_id, record.lsn);
                    // Update DPT for CLR operations
                    let page_id = match redo {
                        CLRRedo::UndoInsert { rid } => rid.page_id,
                        CLRRedo::UndoDelete { rid, .. } => rid.page_id,
                    };
                    dpt.entry(page_id).or_insert(record.lsn);
                }
                WalRecordType::AllocatePage { page_id, prev_page_id, .. } => {
                    // Update ATT
                    att.insert(record.txn_id, record.lsn);
                    // Update DPT for the new page
                    dpt.entry(*page_id).or_insert(record.lsn);
                    // Also update DPT for prev page if it exists
                    if *prev_page_id != u32::MAX {
                        dpt.entry(*prev_page_id).or_insert(record.lsn);
                    }
                }
            }
        }

        // Build last_lsn_map from ATT for undo phase
        let last_lsn_map = att.clone();
        let uncommitted_txns: HashSet<u64> = att.keys().cloned().collect();

        AnalysisResult {
            committed_txns,
            aborted_txns,
            uncommitted_txns,
            last_lsn_map,
            dpt,
            max_lsn,
            max_txn_id,
        }
    }

    /// Redo phase: Replay operations from redo_start_lsn
    fn redo(
        bpm: &Arc<Mutex<BufferPoolManager>>,
        wal_records: &[WalRecord],
        redo_start_lsn: Option<Lsn>,
    ) -> Result<usize> {
        let mut count = 0;

        // If no dirty pages, nothing to redo
        let start_lsn = match redo_start_lsn {
            Some(lsn) => lsn,
            None => return Ok(0),
        };

        for record in wal_records {
            // Skip records before redo start point
            if record.lsn < start_lsn {
                continue;
            }

            match &record.record_type {
                WalRecordType::Insert { rid, data } => {
                    if Self::should_redo(bpm, rid.page_id, record.lsn)? {
                        Self::redo_insert(bpm, rid, data, record.lsn)?;
                        count += 1;
                    }
                }
                WalRecordType::Delete { rid, xmax } => {
                    // MVCC logical delete: set xmax
                    if Self::should_redo(bpm, rid.page_id, record.lsn)? {
                        Self::redo_set_xmax(bpm, rid, *xmax, record.lsn)?;
                        count += 1;
                    }
                }
                WalRecordType::CLR { redo, .. } => {
                    // Redo CLRs too
                    match redo {
                        CLRRedo::UndoInsert { rid } => {
                            // Undo INSERT: physically delete the tuple
                            if Self::should_redo(bpm, rid.page_id, record.lsn)? {
                                Self::redo_physical_delete(bpm, rid, record.lsn)?;
                                count += 1;
                            }
                        }
                        CLRRedo::UndoDelete { rid, old_xmax } => {
                            // Undo DELETE: reset xmax to restore visibility
                            if Self::should_redo(bpm, rid.page_id, record.lsn)? {
                                Self::redo_set_xmax(bpm, rid, *old_xmax, record.lsn)?;
                                count += 1;
                            }
                        }
                    }
                }
                WalRecordType::AllocatePage { page_id, prev_page_id, .. } => {
                    if Self::should_redo(bpm, *page_id, record.lsn)? {
                        Self::redo_allocate_page(bpm, *page_id, *prev_page_id, record.lsn)?;
                        count += 1;
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

    /// Redo physical deletion of a tuple (for UndoInsert CLR)
    fn redo_physical_delete(bpm: &Arc<Mutex<BufferPoolManager>>, rid: &Rid, lsn: Lsn) -> Result<()> {
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

    /// Redo setting xmax (for logical delete or undo delete)
    fn redo_set_xmax(bpm: &Arc<Mutex<BufferPoolManager>>, rid: &Rid, xmax: u64, lsn: Lsn) -> Result<()> {
        let mut bpm_guard = bpm.lock().unwrap();

        if bpm_guard.page_count() <= rid.page_id {
            return Ok(());
        }

        let page_arc = bpm_guard.fetch_page_mut(rid.page_id)?;
        let mut page = page_arc.write().unwrap();

        // Set xmax if tuple exists
        if page.get_tuple(rid.slot_id).is_some() {
            page.set_tuple_xmax(rid.slot_id, xmax)?;
        }
        page.page_lsn = lsn;

        drop(page);
        bpm_guard.unpin_page(rid.page_id, true)?;

        Ok(())
    }

    /// Redo page allocation
    fn redo_allocate_page(
        bpm: &Arc<Mutex<BufferPoolManager>>,
        page_id: u32,
        prev_page_id: u32,
        lsn: Lsn,
    ) -> Result<()> {
        let mut bpm_guard = bpm.lock().unwrap();

        // Ensure pages exist up to page_id
        while bpm_guard.page_count() <= page_id {
            bpm_guard.new_page()?;
        }

        // Initialize the new page
        {
            let page_arc = bpm_guard.fetch_page_mut(page_id)?;
            let mut page = page_arc.write().unwrap();
            page.set_next_page_id(crate::page::NO_NEXT_PAGE);
            page.page_lsn = lsn;
            drop(page);
            bpm_guard.unpin_page(page_id, true)?;
        }

        // Update prev_page's next_page_id if exists
        if prev_page_id != u32::MAX {
            let page_arc = bpm_guard.fetch_page_mut(prev_page_id)?;
            let mut page = page_arc.write().unwrap();
            page.set_next_page_id(page_id);
            page.page_lsn = lsn;
            drop(page);
            bpm_guard.unpin_page(prev_page_id, true)?;
        }

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
                WalRecordType::Delete { rid, .. } => {
                    // Undo DELETE by resetting xmax to 0
                    Self::undo_delete(bpm, rid)?;

                    // Write CLR
                    let clr_lsn = wal_manager.append(
                        txn_id,
                        current_last_lsn,
                        WalRecordType::CLR {
                            undo_next_lsn: record.prev_lsn,
                            redo: CLRRedo::UndoDelete {
                                rid: *rid,
                                old_xmax: 0, // Reset to not deleted
                            },
                        },
                    );

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

    /// Undo DELETE: reset xmax to 0 to restore tuple visibility
    fn undo_delete(bpm: &Arc<Mutex<BufferPoolManager>>, rid: &Rid) -> Result<()> {
        let mut bpm_guard = bpm.lock().unwrap();

        if bpm_guard.page_count() <= rid.page_id {
            return Ok(());
        }

        let page_arc = bpm_guard.fetch_page_mut(rid.page_id)?;
        let mut page = page_arc.write().unwrap();

        // Reset xmax to 0 (not deleted)
        if page.get_tuple(rid.slot_id).is_some() {
            page.set_tuple_xmax(rid.slot_id, 0)?;
        }

        drop(page);
        bpm_guard.unpin_page(rid.page_id, true)?;

        Ok(())
    }

    /// Insert data at a specific slot (for recovery)
    fn insert_at_slot(page: &mut HeapPage, slot_id: u16, data: &[u8]) -> Result<()> {
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
    aborted_txns: HashSet<u64>,
    uncommitted_txns: HashSet<u64>,
    last_lsn_map: HashMap<u64, Lsn>,
    dpt: HashMap<u32, Lsn>,
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
