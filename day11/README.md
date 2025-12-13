# Day 11: Write-Ahead Logging (WAL)

## Usage

```bash
# Initialize (delete existing data)
cargo run -- --init

# Normal start (preserve existing data)
cargo run
```

## Testing WAL

### Terminal 1: Start server

```bash
cargo run -- --init
```

### Terminal 2: Run queries

```bash
psql -h localhost -p 5433
```

```sql
-- Test 1: INSERT with COMMIT
BEGIN;
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
SELECT * FROM users;
COMMIT;

-- Test 2: DELETE with COMMIT
BEGIN;
DELETE FROM users WHERE id = 1;
SELECT * FROM users;
COMMIT;

-- Test 3: INSERT with ROLLBACK
BEGIN;
INSERT INTO users VALUES (3, 'Charlie');
SELECT * FROM users;
ROLLBACK;
SELECT * FROM users;

-- Test 4: UPDATE with COMMIT
BEGIN;
UPDATE users SET name = 'Bobby' WHERE id = 2;
SELECT * FROM users;
COMMIT;
```

### Terminal 1: Restart server to see WAL

```bash
# Ctrl+C to stop server, then:
cargo run
```

Expected output shows WAL records:

```
[Instance] Existing WAL records:
  LSN=1 txn=1 Begin
  LSN=2 txn=1 Insert { rid: Rid { page_id: 0, slot_id: 0 }, data: [...] }
  LSN=3 txn=1 Insert { rid: Rid { page_id: 0, slot_id: 1 }, data: [...] }
  LSN=4 txn=1 Commit
  ...
```
