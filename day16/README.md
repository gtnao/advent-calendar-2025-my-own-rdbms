# Day16

```bash
cargo run -- --init

psql -h localhost -p 5433 -c "CREATE TABLE users (id INT, name VARCHAR);"
psql -h localhost -p 5433 -c "INSERT INTO users VALUES (1, 'Alice');"
psql -h localhost -p 5433 -c "INSERT INTO users VALUES (2, 'Bob');"
psql -h localhost -p 5433 -c "SELECT * FROM users;"
psql -h localhost -p 5433 -c "UPDATE users SET name = 'Charlie' WHERE id = 2;"
psql -h localhost -p 5433 -c "DELETE FROM users WHERE id = 1;"
psql -h localhost -p 5433 -c "SELECT * FROM users;"
psql -h localhost -p 5433 -c "SELECT * FROM pg_class;"
psql -h localhost -p 5433 -c "SELECT * FROM pg_attribute;"
```
