# Day17

```bash
cargo run -- --init


# JOIN
psql -h localhost -p 5433 -c "CREATE TABLE users (id INT, name VARCHAR);"
psql -h localhost -p 5433 -c "INSERT INTO users VALUES (1, 'Alice');"
psql -h localhost -p 5433 -c "INSERT INTO users VALUES (2, 'Bob');"
psql -h localhost -p 5433 -c "CREATE TABLE orders (id INT, user_id INT, product VARCHAR);"
psql -h localhost -p 5433 -c "INSERT INTO orders VALUES (1, 1, 'Book');"
psql -h localhost -p 5433 -c "INSERT INTO orders VALUES (2, 1, 'Pen');"
psql -h localhost -p 5433 -c "INSERT INTO orders VALUES (3, 2, 'Notebook');"
psql -h localhost -p 5433 -c "INSERT INTO users VALUES (3, 'Charlie');"
psql -h localhost -p 5433 -c "INSERT INTO orders VALUES (4, 99, 'Ghost Item');"

# INNER JOIN
psql -h localhost -p 5433 -c "SELECT u.id, u.name, o.product FROM users u INNER JOIN orders o ON u.id = o.user_id;"

# LEFT JOIN (Charlie appears with NULL product)
psql -h localhost -p 5433 -c "SELECT u.id, u.name, o.product FROM users u LEFT JOIN orders o ON u.id = o.user_id;"

# 3-table JOIN
psql -h localhost -p 5433 -c "CREATE TABLE categories (id INT, name VARCHAR);"
psql -h localhost -p 5433 -c "INSERT INTO categories VALUES (1, 'Stationery');"
psql -h localhost -p 5433 -c "INSERT INTO categories VALUES (2, 'Books');"
psql -h localhost -p 5433 -c "CREATE TABLE order_items (order_id INT, category_id INT);"
psql -h localhost -p 5433 -c "INSERT INTO order_items VALUES (1, 2);"
psql -h localhost -p 5433 -c "INSERT INTO order_items VALUES (2, 1);"
psql -h localhost -p 5433 -c "INSERT INTO order_items VALUES (3, 1);"
psql -h localhost -p 5433 -c "SELECT o.id, o.product, c.name FROM orders o JOIN order_items oi ON o.id = oi.order_id JOIN categories c ON oi.category_id = c.id;"
```
