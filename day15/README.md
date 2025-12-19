# Day15

```bash
cargo run -- --init &
sleep 2

# clog.db does not exist yet (not flushed)
ls clog.db 2>&1 || echo "clog.db not found"

# Commit transaction 1 (autocommit)
psql -h localhost -p 5433 -c "INSERT INTO users VALUES (1, 'Alice');"

# Still not flushed (only in memory)
ls clog.db 2>&1 || echo "clog.db still not found"

# Commit transaction 2 (autocommit)
psql -h localhost -p 5433 -c "INSERT INTO users VALUES (2, 'Bob');"

# CHECKPOINT flushes CLOG to disk
psql -h localhost -p 5433 -c "CHECKPOINT;"

# Now clog.db exists
ls -la clog.db

# Check CLOG content: txn1=committed(01), txn2=committed(01)
# First byte should be 0x14 (binary: 00 01 01 00 = txn3:00, txn2:01, txn1:01, txn0:00)
xxd clog.db | head -1

# Verify visibility
psql -h localhost -p 5433 -c "SELECT * FROM users;"

# Kill server (simulate crash)
pkill -9 -f "target/debug/day15"
sleep 1

# Restart without --init (recovery reads CLOG from disk)
cargo run &
sleep 2

# After recovery, CLOG is loaded and visibility still correct
psql -h localhost -p 5433 -c "SELECT * FROM users;"

pkill -f "target/debug/day15"
```
