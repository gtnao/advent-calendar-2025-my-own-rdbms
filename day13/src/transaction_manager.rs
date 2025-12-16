// Global Transaction Manager for ATT (Active Transaction Table)

use std::collections::HashMap;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

use crate::wal::Lsn;

pub struct TransactionManager {
    next_txn_id: AtomicU64,
    // Active Transaction Table: txn_id -> last_lsn
    att: Mutex<HashMap<u64, Lsn>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        TransactionManager {
            next_txn_id: AtomicU64::new(1),
            att: Mutex::new(HashMap::new()),
        }
    }

    // Begin a new transaction: assign ID and register in ATT
    pub fn begin(&self) -> u64 {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let mut att = self.att.lock().unwrap();
        att.insert(txn_id, 0); // Initial last_lsn is 0
        txn_id
    }

    // Update last_lsn for a transaction
    pub fn update_last_lsn(&self, txn_id: u64, lsn: Lsn) {
        let mut att = self.att.lock().unwrap();
        if let Some(entry) = att.get_mut(&txn_id) {
            *entry = lsn;
        }
    }

    // Commit: remove from ATT
    pub fn commit(&self, txn_id: u64) {
        let mut att = self.att.lock().unwrap();
        att.remove(&txn_id);
    }

    // Abort: remove from ATT
    pub fn abort(&self, txn_id: u64) {
        let mut att = self.att.lock().unwrap();
        att.remove(&txn_id);
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
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}
