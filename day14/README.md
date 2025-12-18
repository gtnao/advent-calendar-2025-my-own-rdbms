# Day 14

## Usage

```bash
# Initialize (delete existing data)
cargo run -- --init

# Normal start
cargo run
```

## Testing

### MVCC Visibility - INSERT

```bash
# Terminal 1: Start server
cargo run -- --init

# Terminal 2: Start a transaction and insert data
psql -h localhost -p 5433 <<'EOF'
BEGIN;
INSERT INTO users VALUES (1, 'Alice');
SELECT * FROM users;
EOF
# Output: Alice is visible (own transaction)

# Terminal 3: Query from another session
psql -h localhost -p 5433 -c "SELECT * FROM users;"
# Output: Empty (uncommitted data is not visible)

# Terminal 2: Commit the transaction
psql -h localhost -p 5433 -c "COMMIT;"

# Terminal 3: Query again
psql -h localhost -p 5433 -c "SELECT * FROM users;"
# Output: Alice is now visible (committed)
```

### MVCC Visibility - DELETE

```bash
# Terminal 1: Start server
cargo run -- --init

# Terminal 2: Insert data
psql -h localhost -p 5433 <<'EOF'
BEGIN;
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
COMMIT;
EOF

# Terminal 2: Start a transaction and delete
psql -h localhost -p 5433 <<'EOF'
BEGIN;
DELETE FROM users WHERE id = 1;
SELECT * FROM users;
EOF
# Output: Only Bob (Alice deleted in own transaction)

# Terminal 3: Query from another session
psql -h localhost -p 5433 -c "SELECT * FROM users;"
# Output: Alice and Bob (uncommitted delete is not visible)

# Terminal 2: Commit
psql -h localhost -p 5433 -c "COMMIT;"

# Terminal 3: Query again
psql -h localhost -p 5433 -c "SELECT * FROM users;"
# Output: Only Bob (delete committed)
```

### Snapshot Isolation (REPEATABLE READ)

```bash
# Terminal 1: Start server
cargo run -- --init

# Terminal 2: Insert initial data
psql -h localhost -p 5433 <<'EOF'
BEGIN;
INSERT INTO users VALUES (1, 'Alice');
COMMIT;
EOF

# Terminal 2: Start T1 and take snapshot
psql -h localhost -p 5433 <<'EOF'
BEGIN;
SELECT * FROM users;
EOF
# Output: Alice

# Terminal 3: T2 updates Alice to Bob and commits
psql -h localhost -p 5433 <<'EOF'
BEGIN;
UPDATE users SET name = 'Bob' WHERE id = 1;
COMMIT;
EOF

# Terminal 3: Verify update is committed
psql -h localhost -p 5433 -c "SELECT * FROM users;"
# Output: Bob (T2's update is committed)

# Terminal 2: T1 still sees Alice (REPEATABLE READ)
psql -h localhost -p 5433 <<'EOF'
SELECT * FROM users;
COMMIT;
EOF
# Output: Alice (snapshot taken at BEGIN, not affected by T2's commit)

# Terminal 2: After commit, new transaction sees Bob
psql -h localhost -p 5433 -c "SELECT * FROM users;"
# Output: Bob
```

### ROLLBACK

```bash
# Terminal 2: Insert and rollback
psql -h localhost -p 5433 <<'EOF'
BEGIN;
INSERT INTO users VALUES (3, 'Charlie');
SELECT * FROM users;
ROLLBACK;
EOF

psql -h localhost -p 5433 -c "SELECT * FROM users;"
# Output: Charlie is gone (rolled back)

# Terminal 2: Delete and rollback
psql -h localhost -p 5433 <<'EOF'
BEGIN;
DELETE FROM users WHERE id = 2;
SELECT * FROM users;
ROLLBACK;
EOF

psql -h localhost -p 5433 -c "SELECT * FROM users;"
# Output: Bob is back (delete rolled back)
```
