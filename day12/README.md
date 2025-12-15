# Day 12: Crash Recovery (ARIES)

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
