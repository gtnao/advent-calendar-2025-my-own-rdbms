// Global Transaction Manager for ATT (Active Transaction Table)

use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

use crate::clog::Clog;
use crate::tuple::TxnId;
use crate::visibility::Snapshot;
use crate::wal::Lsn;

/// Transaction status for visibility check.
/// Persisted to CLOG file at checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnStatus {
    InProgress,
    Committed,
    Aborted,
}

pub struct TransactionManager {
    next_txn_id: AtomicU64,
    // Active Transaction Table: txn_id -> last_lsn
    att: Mutex<HashMap<u64, Lsn>>,
    // CLOG for transaction status persistence
    clog: Arc<Clog>,
}

impl TransactionManager {
    pub fn new(clog: Arc<Clog>) -> Self {
        TransactionManager {
            next_txn_id: AtomicU64::new(1),
            att: Mutex::new(HashMap::new()),
            clog,
        }
    }

    // Begin a new transaction: assign ID and register in ATT
    pub fn begin(&self) -> u64 {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let mut att = self.att.lock().unwrap();
        att.insert(txn_id, 0); // Initial last_lsn is 0
        // Status is InProgress (default in CLOG)
        txn_id
    }

    // Update last_lsn for a transaction
    pub fn update_last_lsn(&self, txn_id: u64, lsn: Lsn) {
        let mut att = self.att.lock().unwrap();
        if let Some(entry) = att.get_mut(&txn_id) {
            *entry = lsn;
        }
    }

    // Commit: remove from ATT, record status in CLOG
    pub fn commit(&self, txn_id: u64) {
        let mut att = self.att.lock().unwrap();
        att.remove(&txn_id);
        drop(att);
        // Record committed status in CLOG
        self.clog.set_status(txn_id, TxnStatus::Committed);
    }

    // Abort: remove from ATT, record status in CLOG
    pub fn abort(&self, txn_id: u64) {
        let mut att = self.att.lock().unwrap();
        att.remove(&txn_id);
        drop(att);
        // Record aborted status in CLOG
        self.clog.set_status(txn_id, TxnStatus::Aborted);
    }

    // Get snapshot of ATT for checkpoint
    pub fn get_att_snapshot(&self) -> HashMap<u64, Lsn> {
        let att = self.att.lock().unwrap();
        att.clone()
    }

    // Set next transaction ID (for recovery)
    pub fn set_next_txn_id(&self, id: u64) {
        self.next_txn_id.store(id, Ordering::SeqCst);
    }

    // Get current next_txn_id (for recovery max calculation)
    pub fn get_next_txn_id(&self) -> u64 {
        self.next_txn_id.load(Ordering::SeqCst)
    }

    /// Get a snapshot for visibility checking.
    /// The snapshot captures the current state of active transactions.
    pub fn get_snapshot(&self, txn_id: TxnId) -> Snapshot {
        let att = self.att.lock().unwrap();
        let active_txns: HashSet<TxnId> = att.keys().cloned().collect();
        let xmin = active_txns.iter().min().copied().unwrap_or(txn_id);
        let xmax = self.next_txn_id.load(Ordering::SeqCst);
        Snapshot::new(txn_id, xmin, xmax, active_txns)
    }

    /// Get transaction status for visibility checking (from CLOG).
    pub fn get_txn_status(&self, txn_id: TxnId) -> TxnStatus {
        self.clog.get_status(txn_id)
    }

    /// Set transaction status (called during WAL replay)
    pub fn set_txn_status(&self, txn_id: TxnId, status: TxnStatus) {
        self.clog.set_status(txn_id, status);
    }

    /// Get reference to CLOG (for checkpoint flush)
    pub fn clog(&self) -> &Arc<Clog> {
        &self.clog
    }
}
