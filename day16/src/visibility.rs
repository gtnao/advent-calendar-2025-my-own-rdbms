use std::collections::HashSet;

use crate::transaction_manager::{TransactionManager, TxnStatus};
use crate::tuple::{TxnId, INVALID_TXN_ID};

/// Snapshot of the database state at transaction start time.
///
/// Used for Snapshot Isolation to determine which tuples are visible
/// to a transaction based on when they were created/deleted.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Transaction ID that owns this snapshot
    pub txn_id: TxnId,
    /// Minimum active transaction ID at snapshot time.
    /// Transactions with ID < xmin that have committed are visible.
    #[allow(dead_code)]
    pub xmin: TxnId,
    /// Next transaction ID at snapshot time.
    /// Transactions with ID >= xmax are not visible.
    pub xmax: TxnId,
    /// Set of transaction IDs that were active at snapshot time.
    /// Changes made by these transactions are not visible.
    pub active_txns: HashSet<TxnId>,
}

impl Snapshot {
    pub fn new(txn_id: TxnId, xmin: TxnId, xmax: TxnId, active_txns: HashSet<TxnId>) -> Self {
        Snapshot {
            txn_id,
            xmin,
            xmax,
            active_txns,
        }
    }
}

/// Check if a tuple is visible to the given snapshot.
///
/// Visibility rules (PostgreSQL-style):
/// 1. Check if xmin is visible (the transaction that created this tuple)
/// 2. Check if xmax is visible (the transaction that deleted this tuple)
///
/// A tuple is visible if:
/// - xmin is committed and visible to this snapshot
/// - xmax is either 0 (not deleted) or not visible to this snapshot
pub fn is_tuple_visible(
    xmin: TxnId,
    xmax: TxnId,
    snapshot: &Snapshot,
    txn_manager: &TransactionManager,
) -> bool {
    // 1. Check xmin visibility (who created this tuple?)
    if !is_xmin_visible(xmin, snapshot, txn_manager) {
        return false;
    }

    // 2. Check xmax (has this tuple been deleted?)
    if xmax == INVALID_TXN_ID {
        // Not deleted
        return true;
    }

    // 3. If we deleted it ourselves, it's not visible
    if xmax == snapshot.txn_id {
        return false;
    }

    // 4. If xmax is beyond our snapshot, deletion is not visible
    if xmax >= snapshot.xmax {
        return true;
    }

    // 5. If xmax is in active transactions, deletion is not committed yet
    if snapshot.active_txns.contains(&xmax) {
        return true;
    }

    // 6. Check xmax transaction status
    // TODO: Day15 will use CLOG for persistent status
    match txn_manager.get_txn_status(xmax) {
        TxnStatus::Committed => false, // Deletion is committed -> not visible
        TxnStatus::Aborted => true,    // Deletion was aborted -> visible
        TxnStatus::InProgress => true, // Should not happen normally
    }
}

/// Check if the creating transaction (xmin) is visible to the snapshot.
fn is_xmin_visible(xmin: TxnId, snapshot: &Snapshot, txn_manager: &TransactionManager) -> bool {
    // We created it ourselves
    if xmin == snapshot.txn_id {
        return true;
    }

    // xmin is beyond our snapshot (created after we started)
    if xmin >= snapshot.xmax {
        return false;
    }

    // xmin was active when we took our snapshot
    if snapshot.active_txns.contains(&xmin) {
        return false;
    }

    // Check transaction status
    // TODO: Day15 will use CLOG for persistent status
    match txn_manager.get_txn_status(xmin) {
        TxnStatus::Committed => true,   // Created by committed transaction -> visible
        TxnStatus::Aborted => false,    // Created by aborted transaction -> not visible
        TxnStatus::InProgress => false, // Should not happen normally
    }
}
