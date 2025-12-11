# Day 08: DELETE/UPDATE Statements + Multi-threaded Connections

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
INSERT INTO users VALUES (3, 'Charlie');

-- Select all
SELECT * FROM users;

-- Delete a specific row
DELETE FROM users WHERE id = 2;

-- Verify deletion
SELECT * FROM users;

-- Update a row
UPDATE users SET name = 'Alice Updated' WHERE id = 1;

-- Verify update
SELECT * FROM users;
```
