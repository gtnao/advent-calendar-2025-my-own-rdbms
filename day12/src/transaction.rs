// Transaction management with Undo Log

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::executor::Rid;
use crate::wal::Lsn;

static NEXT_TXN_ID: AtomicU64 = AtomicU64::new(1);

fn generate_txn_id() -> u64 {
    NEXT_TXN_ID.fetch_add(1, Ordering::SeqCst)
}

pub fn set_next_txn_id(id: u64) {
    NEXT_TXN_ID.store(id, Ordering::SeqCst);
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TransactionState {
    Inactive,
    Active,
}

// Undo log entry for rollback
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum UndoLogEntry {
    // For INSERT: record the RID to delete on rollback
    Insert { lsn: Lsn, prev_lsn: Lsn, rid: Rid, data: Vec<u8> },
    // For DELETE: record the RID and original data to restore on rollback
    Delete { lsn: Lsn, prev_lsn: Lsn, rid: Rid, data: Vec<u8> },
}

// Transaction context for each connection
#[derive(Debug)]
pub struct Transaction {
    pub id: u64,
    pub state: TransactionState,
    pub undo_log: Vec<UndoLogEntry>,
    pub held_locks: HashSet<Rid>,
    pub last_lsn: Lsn,  // Most recent LSN for this transaction
}

impl Transaction {
    pub fn new() -> Self {
        Transaction {
            id: 0,
            state: TransactionState::Inactive,
            undo_log: Vec::new(),
            held_locks: HashSet::new(),
            last_lsn: 0,
        }
    }

    pub fn begin(&mut self) {
        self.id = generate_txn_id();
        self.state = TransactionState::Active;
        self.undo_log.clear();
        self.held_locks.clear();
        self.last_lsn = 0;
    }

    pub fn commit(&mut self) {
        // Clear undo log (changes are now permanent)
        self.undo_log.clear();
        self.state = TransactionState::Inactive;
        self.last_lsn = 0;
        // held_locks is cleared by take_held_locks before this
    }

    pub fn is_active(&self) -> bool {
        self.state == TransactionState::Active
    }

    pub fn add_undo_entry(&mut self, entry: UndoLogEntry) {
        self.undo_log.push(entry);
    }

    pub fn set_last_lsn(&mut self, lsn: Lsn) {
        self.last_lsn = lsn;
    }

    // Take undo log for rollback (transfers ownership)
    pub fn take_undo_log(&mut self) -> Vec<UndoLogEntry> {
        self.state = TransactionState::Inactive;
        self.last_lsn = 0;
        std::mem::take(&mut self.undo_log)
    }

    pub fn add_lock(&mut self, rid: Rid) {
        self.held_locks.insert(rid);
    }

    // Take held locks for unlock_all (transfers ownership)
    pub fn take_held_locks(&mut self) -> HashSet<Rid> {
        std::mem::take(&mut self.held_locks)
    }
}

impl Default for Transaction {
    fn default() -> Self {
        Self::new()
    }
}
