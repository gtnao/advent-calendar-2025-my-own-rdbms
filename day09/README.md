# Day 09: Transaction (BEGIN/COMMIT/ROLLBACK)

## Running the Server

```bash
cargo run
```

## Connecting with psql

```bash
psql -h localhost -p 5433
```

## Example Queries

```sql
-- Insert data
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');

-- Select all
SELECT * FROM users;

-- Transaction: INSERT with ROLLBACK
BEGIN;
INSERT INTO users VALUES (3, 'Charlie');
SELECT * FROM users;  -- Shows Alice, Bob, Charlie
ROLLBACK;
SELECT * FROM users;  -- Shows Alice, Bob (Charlie is gone)

-- Transaction: DELETE with ROLLBACK
BEGIN;
DELETE FROM users WHERE id = 1;
SELECT * FROM users;  -- Shows only Bob
ROLLBACK;
SELECT * FROM users;  -- Shows Alice, Bob (Alice is restored)

-- Transaction: UPDATE with ROLLBACK
BEGIN;
UPDATE users SET name = 'Alice Updated' WHERE id = 1;
SELECT * FROM users;  -- Shows Alice Updated, Bob
ROLLBACK;
SELECT * FROM users;  -- Shows Alice, Bob (original name restored)

-- Transaction: COMMIT
BEGIN;
INSERT INTO users VALUES (3, 'Charlie');
DELETE FROM users WHERE id = 2;
COMMIT;
SELECT * FROM users;  -- Shows Alice, Charlie (changes are permanent)
```
