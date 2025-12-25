# Day20

```bash
cargo run -- --init

# Setup: Create table and insert data
psql -h localhost -p 5433 -c "CREATE TABLE users (id INT, name VARCHAR, age INT);"
psql -h localhost -p 5433 -c "INSERT INTO users VALUES (1, 'Alice', 25);"
psql -h localhost -p 5433 -c "INSERT INTO users VALUES (2, 'Bob', 30);"
psql -h localhost -p 5433 -c "INSERT INTO users VALUES (3, 'Charlie', 35);"

# Create B-Tree index on id column
psql -h localhost -p 5433 -c "CREATE INDEX idx_users_id ON users (id);"

# Verify index exists in pg_index
psql -h localhost -p 5433 -c "SELECT * FROM pg_index;"

# Insert more data (index is automatically maintained)
psql -h localhost -p 5433 -c "INSERT INTO users VALUES (4, 'David', 40);"
psql -h localhost -p 5433 -c "INSERT INTO users VALUES (5, 'Eve', 28);"

# Query all data (uses SeqScan - no WHERE clause)
psql -h localhost -p 5433 -c "SELECT * FROM users;"

# Query with equality condition on indexed column (uses IndexScan!)
# Server will print: [Optimizer] Using IndexScan with index 'idx_users_id'
psql -h localhost -p 5433 -c "SELECT * FROM users WHERE id = 3;"

# Update data (index is automatically maintained)
psql -h localhost -p 5433 -c "UPDATE users SET age = 26 WHERE id = 1;"

# Verify update (uses IndexScan)
psql -h localhost -p 5433 -c "SELECT * FROM users WHERE id = 1;"

# ============================================
# Bulk insert test (triggers B-Tree splits)
# ============================================

# Insert 200 rows to trigger multiple B-Tree splits
for i in $(seq 100 299); do
  psql -h localhost -p 5433 -c "INSERT INTO users VALUES ($i, 'User$i', $((i % 50 + 20)));"
done

# Verify total count
psql -h localhost -p 5433 -c "SELECT COUNT(*) FROM users;"

# Test IndexScan still works after many splits
psql -h localhost -p 5433 -c "SELECT * FROM users WHERE id = 150;"
psql -h localhost -p 5433 -c "SELECT * FROM users WHERE id = 250;"

# Verify first and last inserted rows
psql -h localhost -p 5433 -c "SELECT * FROM users WHERE id = 100;"
psql -h localhost -p 5433 -c "SELECT * FROM users WHERE id = 299;"
```
