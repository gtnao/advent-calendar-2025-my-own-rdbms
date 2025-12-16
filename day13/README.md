# Day 13: Fuzzy Checkpoint

## Usage

```bash
# Initialize (delete existing data)
cargo run -- --init

# Normal start (runs recovery if needed)
cargo run
```

## Testing Crash Recovery

### Test 1: Committed Transaction Recovery

```bash
# Terminal 1: Start server
cargo run -- --init

# Terminal 2: Run queries
psql -h localhost -p 5433

BEGIN;
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
COMMIT;
\q

# Terminal 1: Kill server with Ctrl+C

# Terminal 1: Restart server (runs recovery)
cargo run

# Terminal 2: Verify data persisted
psql -h localhost -p 5433
SELECT * FROM users;
-- Should show Alice and Bob
```

### Test 2: Uncommitted Transaction Recovery (Undo)

```bash
# Terminal 1: Start server
cargo run -- --init

# Terminal 2: Begin transaction but don't commit
psql -h localhost -p 5433

BEGIN;
INSERT INTO users VALUES (1, 'Alice');
COMMIT;
SELECT * FROM users;

BEGIN;
INSERT INTO users VALUES (2, 'Bob');
SELECT * FROM users;
-- Shows both Alice and Bob
-- DON'T COMMIT - close psql with Ctrl+C

# Terminal 1: Kill server with Ctrl+C

# Terminal 1: Restart server (runs recovery with undo)
cargo run

# Terminal 2: Verify uncommitted data rolled back
psql -h localhost -p 5433
SELECT * FROM users;
-- Should show only Alice (Bob was rolled back)
```

### Test 3: Normal Rollback with CLR

```bash
# Terminal 2:
psql -h localhost -p 5433

BEGIN;
INSERT INTO users VALUES (3, 'Charlie');
SELECT * FROM users;
ROLLBACK;
SELECT * FROM users;
-- Charlie should be gone

# Terminal 1: Restart server
# Kill with Ctrl+C, then:
cargo run

# Terminal 2: Verify rollback persisted
psql -h localhost -p 5433
SELECT * FROM users;
-- Charlie should still be gone
```

### Test 4: Fuzzy Checkpoint

```bash
# Terminal 1: Start server
cargo run -- --init

# Terminal 2: Insert data and create checkpoint
psql -h localhost -p 5433

BEGIN;
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
COMMIT;

-- Create a checkpoint (saves ATT/DPT snapshot to WAL)
CHECKPOINT;

BEGIN;
INSERT INTO users VALUES (3, 'Charlie');
COMMIT;
\q

# Terminal 1: Observe checkpoint log output
# [Checkpoint] Starting fuzzy checkpoint...
# [Checkpoint] ATT has 0 active transactions
# [Checkpoint] DPT has N dirty pages
# [Checkpoint] Checkpoint completed at LSN X

# Terminal 1: Kill server with Ctrl+C

# Terminal 1: Restart server (recovery uses checkpoint)
cargo run

# Terminal 2: Verify all data persisted
psql -h localhost -p 5433
SELECT * FROM users;
-- Should show Alice, Bob, and Charlie
```

### Test 5: Checkpoint with Active Transaction

```bash
# Terminal 1: Start server
cargo run -- --init

# Terminal 2: Create checkpoint while transaction is active
psql -h localhost -p 5433

BEGIN;
INSERT INTO users VALUES (1, 'Alice');
COMMIT;

-- Start another transaction but don't commit yet
BEGIN;
INSERT INTO users VALUES (2, 'Bob');

-- In another psql session (Terminal 3):
psql -h localhost -p 5433
CHECKPOINT;
-- Should show: [Checkpoint] ATT has 1 active transactions
\q

-- Back to Terminal 2: Don't commit, just disconnect
-- Ctrl+C to close psql

# Terminal 1: Kill server with Ctrl+C

# Terminal 1: Restart server (recovery uses checkpoint, undoes Bob)
cargo run

# Terminal 2: Verify only committed data persisted
psql -h localhost -p 5433
SELECT * FROM users;
-- Should show only Alice (Bob was rolled back during recovery)
```
