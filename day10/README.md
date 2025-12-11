# Day 10: Strict Two-Phase Locking (S2PL)

## Setup

```sql
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
```

## Anomalies Prevented by S2PL

### 1. Dirty Read (Prevented)

Reading uncommitted data from another transaction.

```
T1: BEGIN;
T1: UPDATE users SET name = 'Modified' WHERE id = 1;
T2: BEGIN;
T2: SELECT * FROM users WHERE id = 1;  -- BLOCKED (waits for T1's exclusive lock)
T1: ROLLBACK;
T2: -- Now proceeds, reads original 'Alice' (not the rolled-back 'Modified')
```

### 2. Non-Repeatable Read (Prevented)

Reading the same row twice yields different results.

```
T1: BEGIN;
T1: SELECT * FROM users WHERE id = 1;  -- Reads 'Alice'
T2: BEGIN;
T2: UPDATE users SET name = 'Bob' WHERE id = 1;  -- BLOCKED (T1 holds shared lock)
T1: SELECT * FROM users WHERE id = 1;  -- Still reads 'Alice'
T1: COMMIT;
T2: -- Now proceeds with UPDATE
```

### 3. Lost Update (Prevented)

Two transactions overwrite each other's changes.

```
T1: BEGIN;
T1: UPDATE users SET name = 'Alice v1' WHERE id = 1;  -- Acquires exclusive lock
T2: BEGIN;
T2: UPDATE users SET name = 'Alice v2' WHERE id = 1;  -- BLOCKED
T1: COMMIT;
T2: -- Now proceeds, overwrites T1's change (but T1 already committed)
```

### 4. Write Skew (Prevented by row-level exclusive locks)

Two transactions read overlapping data and make disjoint updates.

```
T1: BEGIN;
T1: SELECT * FROM users WHERE id = 1;  -- Reads row
T1: UPDATE users SET name = 'T1 Modified' WHERE id = 1;  -- Exclusive lock
T2: BEGIN;
T2: SELECT * FROM users WHERE id = 1;  -- BLOCKED (T1 has exclusive lock)
T1: COMMIT;
T2: -- Now proceeds with consistent view
```

### 5. Phantom Read (NOT fully prevented)

New rows appear between reads. S2PL with row-level locks does not prevent phantoms
because locks are only on existing rows, not on "gaps" in the index.

```
T1: BEGIN;
T1: SELECT * FROM users WHERE id > 0;  -- Reads Alice, Bob
T2: BEGIN;
T2: INSERT INTO users VALUES (3, 'Charlie');  -- Succeeds (no lock conflict)
T2: COMMIT;
T1: SELECT * FROM users WHERE id > 0;  -- May see Alice, Bob, Charlie (Phantom!)
T1: COMMIT;
```

Note: Preventing phantom reads requires predicate locking or gap locking (not implemented).

## Deadlock Example

Two transactions waiting for each other's locks.

```
T1: BEGIN;
T1: UPDATE users SET name = 'T1' WHERE id = 1;  -- Lock on row 1
T2: BEGIN;
T2: UPDATE users SET name = 'T2' WHERE id = 2;  -- Lock on row 2
T1: UPDATE users SET name = 'T1' WHERE id = 2;  -- BLOCKED (waiting for T2)
T2: UPDATE users SET name = 'T2' WHERE id = 1;  -- BLOCKED (waiting for T1) -> DEADLOCK!
-- After 30 seconds, one transaction times out with "lock acquisition timeout"
```
