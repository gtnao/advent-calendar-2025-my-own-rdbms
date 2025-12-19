use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use crate::executor::Rid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    Shared,    // For SELECT
    Exclusive, // For INSERT/UPDATE/DELETE
}

#[derive(Debug)]
pub enum LockError {
    Timeout,
}

impl std::fmt::Display for LockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockError::Timeout => write!(f, "lock acquisition timeout (possible deadlock)"),
        }
    }
}

impl std::error::Error for LockError {}

struct LockRequest {
    txn_id: u64,
    mode: LockMode,
}

struct LockState {
    holders: HashMap<u64, LockMode>,
    wait_queue: VecDeque<LockRequest>,
}

impl LockState {
    fn new() -> Self {
        LockState {
            holders: HashMap::new(),
            wait_queue: VecDeque::new(),
        }
    }

    // Check if a lock mode is compatible with current holders
    fn is_compatible(&self, mode: LockMode) -> bool {
        if self.holders.is_empty() {
            return true;
        }
        match mode {
            LockMode::Shared => {
                // Shared is compatible only with other Shared locks
                self.holders.values().all(|m| *m == LockMode::Shared)
            }
            LockMode::Exclusive => {
                // Exclusive is not compatible with any lock
                false
            }
        }
    }

    // Check if lock can be granted to txn_id
    fn can_grant(&self, txn_id: u64, mode: LockMode) -> bool {
        // If already holding a lock
        if let Some(held_mode) = self.holders.get(&txn_id) {
            // Already holding Exclusive or requesting Shared (which current lock covers)
            if *held_mode == LockMode::Exclusive || mode == LockMode::Shared {
                return true;
            }
            // Upgrade from Shared to Exclusive: only if we're the only holder
            if self.holders.len() == 1 {
                return true;
            }
            return false;
        }

        // Not holding any lock - check compatibility and wait queue
        // Must also check wait queue is empty (FIFO ordering)
        self.is_compatible(mode) && self.wait_queue.is_empty()
    }
}

pub struct LockManager {
    lock_table: Mutex<HashMap<Rid, LockState>>,
    cond: Condvar,
    timeout: Duration,
}

impl LockManager {
    pub fn new() -> Self {
        LockManager {
            lock_table: Mutex::new(HashMap::new()),
            cond: Condvar::new(),
            timeout: Duration::from_secs(30),
        }
    }

    pub fn lock(&self, txn_id: u64, rid: Rid, mode: LockMode) -> Result<(), LockError> {
        let mut table = self.lock_table.lock().unwrap();

        let state = table.entry(rid).or_insert_with(LockState::new);

        // Already holding an adequate lock
        if let Some(held_mode) = state.holders.get(&txn_id) {
            if *held_mode == LockMode::Exclusive || mode == LockMode::Shared {
                return Ok(());
            }
            // Need to upgrade from Shared to Exclusive
        }

        // Can grant immediately
        if state.can_grant(txn_id, mode) {
            state.holders.insert(txn_id, mode);
            return Ok(());
        }

        // Must wait - add to queue
        state.wait_queue.push_back(LockRequest { txn_id, mode });

        // Wait for lock to be granted
        let mut result = self
            .cond
            .wait_timeout_while(table, self.timeout, |table| {
                let state = table.get(&rid).unwrap();
                !state.holders.contains_key(&txn_id)
            })
            .unwrap();

        if result.1.timed_out() {
            // Remove from wait queue on timeout
            let table = &mut *result.0;
            if let Some(state) = table.get_mut(&rid) {
                state.wait_queue.retain(|r| r.txn_id != txn_id);
            }
            return Err(LockError::Timeout);
        }

        Ok(())
    }

    pub fn unlock_all(&self, txn_id: u64, held_locks: &HashSet<Rid>) {
        let mut table = self.lock_table.lock().unwrap();

        for rid in held_locks {
            if let Some(state) = table.get_mut(rid) {
                state.holders.remove(&txn_id);
                self.grant_waiting_locks(state);

                // Cleanup empty entries
                if state.holders.is_empty() && state.wait_queue.is_empty() {
                    table.remove(rid);
                }
            }
        }

        self.cond.notify_all();
    }

    fn grant_waiting_locks(&self, state: &mut LockState) {
        let mut i = 0;
        while i < state.wait_queue.len() {
            let request = &state.wait_queue[i];
            let txn_id = request.txn_id;
            let mode = request.mode;

            let can_grant = if state.holders.is_empty() {
                true
            } else {
                match mode {
                    LockMode::Shared => state.holders.values().all(|m| *m == LockMode::Shared),
                    LockMode::Exclusive => {
                        // Can upgrade if already holding Shared and is the only holder
                        state.holders.len() == 1 && state.holders.contains_key(&txn_id)
                    }
                }
            };

            if can_grant {
                state.holders.insert(txn_id, mode);
                state.wait_queue.remove(i);
            } else {
                // If Exclusive lock is waiting, stop granting further locks (FIFO)
                if mode == LockMode::Exclusive {
                    break;
                }
                i += 1;
            }
        }
    }
}
