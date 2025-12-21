# Day18

```bash
cargo run -- --init

# Setup
psql -h localhost -p 5433 -c "CREATE TABLE sales (region VARCHAR, product VARCHAR, quantity INT, price INT);"
psql -h localhost -p 5433 -c "INSERT INTO sales VALUES ('east', 'apple', 10, 100);"
psql -h localhost -p 5433 -c "INSERT INTO sales VALUES ('east', 'apple', 5, 100);"
psql -h localhost -p 5433 -c "INSERT INTO sales VALUES ('east', 'banana', 8, 50);"
psql -h localhost -p 5433 -c "INSERT INTO sales VALUES ('west', 'apple', 3, 100);"
psql -h localhost -p 5433 -c "INSERT INTO sales VALUES ('west', 'banana', 12, 50);"
psql -h localhost -p 5433 -c "INSERT INTO sales VALUES ('west', 'banana', 7, 50);"

# COUNT(*)
psql -h localhost -p 5433 -c "SELECT COUNT(*) FROM sales;"

# SUM / AVG / MIN / MAX
psql -h localhost -p 5433 -c "SELECT SUM(quantity), AVG(quantity), MIN(quantity), MAX(quantity) FROM sales;"

# GROUP BY (single column)
psql -h localhost -p 5433 -c "SELECT product, COUNT(*), SUM(quantity) FROM sales GROUP BY product;"

# GROUP BY (multiple columns)
psql -h localhost -p 5433 -c "SELECT region, product, SUM(quantity) FROM sales GROUP BY region, product;"

# GROUP BY with HAVING
psql -h localhost -p 5433 -c "SELECT product, SUM(quantity) FROM sales GROUP BY product HAVING SUM(quantity) > 10;"
```
