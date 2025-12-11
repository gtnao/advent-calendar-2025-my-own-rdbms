// Transaction management with Undo Log

use crate::executor::Rid;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TransactionState {
    Inactive,
    Active,
}

// Undo log entry for rollback
#[derive(Debug, Clone)]
pub enum UndoLogEntry {
    // For INSERT: record the RID to delete on rollback
    Insert { rid: Rid },
    // For DELETE: record the RID and original data to restore on rollback
    Delete { rid: Rid, data: Vec<u8> },
}

// Transaction context for each connection
#[derive(Debug)]
pub struct Transaction {
    pub state: TransactionState,
    pub undo_log: Vec<UndoLogEntry>,
}

impl Transaction {
    pub fn new() -> Self {
        Transaction {
            state: TransactionState::Inactive,
            undo_log: Vec::new(),
        }
    }

    pub fn begin(&mut self) {
        self.state = TransactionState::Active;
        self.undo_log.clear();
    }

    pub fn commit(&mut self) {
        // Clear undo log (changes are now permanent)
        self.undo_log.clear();
        self.state = TransactionState::Inactive;
    }

    pub fn is_active(&self) -> bool {
        self.state == TransactionState::Active
    }

    pub fn add_undo_entry(&mut self, entry: UndoLogEntry) {
        self.undo_log.push(entry);
    }

    // Take undo log for rollback (transfers ownership)
    pub fn take_undo_log(&mut self) -> Vec<UndoLogEntry> {
        self.state = TransactionState::Inactive;
        std::mem::take(&mut self.undo_log)
    }
}

impl Default for Transaction {
    fn default() -> Self {
        Self::new()
    }
}
