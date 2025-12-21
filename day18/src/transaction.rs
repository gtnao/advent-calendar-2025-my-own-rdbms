// Transaction management with Undo Log

use std::collections::HashSet;

use crate::executor::Rid;
use crate::visibility::Snapshot;
use crate::wal::Lsn;

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
    // For MVCC DELETE: reset xmax on rollback
    Delete { lsn: Lsn, prev_lsn: Lsn, rid: Rid, old_xmax: u64 },
}

// Transaction context for each connection
#[derive(Debug)]
pub struct Transaction {
    pub id: u64,
    pub state: TransactionState,
    pub undo_log: Vec<UndoLogEntry>,
    pub held_locks: HashSet<Rid>,
    pub last_lsn: Lsn,  // Most recent LSN for this transaction
    // Snapshot for REPEATABLE READ isolation level
    // Taken at transaction start and used for all reads within the transaction
    pub snapshot: Option<Snapshot>,
}

impl Transaction {
    pub fn new() -> Self {
        Transaction {
            id: 0,
            state: TransactionState::Inactive,
            undo_log: Vec::new(),
            held_locks: HashSet::new(),
            last_lsn: 0,
            snapshot: None,
        }
    }

    // Begin with transaction ID and snapshot for REPEATABLE READ
    pub fn begin_with_id(&mut self, txn_id: u64, snapshot: Snapshot) {
        self.id = txn_id;
        self.state = TransactionState::Active;
        self.undo_log.clear();
        self.held_locks.clear();
        self.last_lsn = 0;
        self.snapshot = Some(snapshot);
    }

    pub fn commit(&mut self) {
        // Clear undo log (changes are now permanent)
        self.undo_log.clear();
        self.state = TransactionState::Inactive;
        self.last_lsn = 0;
        self.snapshot = None;
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
        self.snapshot = None;
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
