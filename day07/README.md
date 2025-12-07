# Day 07: PostgreSQL Wire Protocol

This day introduces PostgreSQL wire protocol support, allowing you to connect using standard PostgreSQL clients like `psql`.

## Running the Server

```bash
cargo run
```

The server listens on port 5433.

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

-- Select specific columns
SELECT name FROM users;

-- Select with WHERE clause
SELECT * FROM users WHERE id > 1;

-- Select with expression
SELECT id + 1 FROM users;

-- Select with complex WHERE
SELECT id, name FROM users WHERE id >= 1 AND id <= 2;
```

## Supported Features

- Simple query protocol (Q message)
- INSERT statements
- SELECT statements with:
  - Column selection
  - WHERE clause (comparison, AND, OR)
  - Arithmetic expressions (+, -, *, /)
- Fixed schema: `users(id INT, name VARCHAR)`
