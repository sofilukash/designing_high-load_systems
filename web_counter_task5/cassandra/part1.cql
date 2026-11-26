-- Частина 1: таблиці items і orders (виконання: docker exec -i cass1 cqlsh < cassandra/part1.cql)
CREATE KEYSPACE shop WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
CREATE TABLE shop.items (category text, price decimal, id int, name text, producer text, props map<text, text>, PRIMARY KEY ((category), price, id)) WITH CLUSTERING ORDER BY (price ASC, id ASC);
CREATE INDEX items_props_idx ON shop.items (ENTRIES(props));
CREATE MATERIALIZED VIEW shop.items_by_name AS SELECT * FROM shop.items WHERE category IS NOT NULL AND name IS NOT NULL AND price IS NOT NULL AND id IS NOT NULL PRIMARY KEY ((category), name, price, id);
CREATE MATERIALIZED VIEW shop.items_by_producer AS SELECT * FROM shop.items WHERE category IS NOT NULL AND producer IS NOT NULL AND price IS NOT NULL AND id IS NOT NULL PRIMARY KEY ((category), producer, price, id);
DESCRIBE TABLE shop.items;

INSERT INTO shop.items (category, price, id, name, producer, props) VALUES ('phone', 899, 1, 'Galaxy S24', 'Samsung', {'color': 'black', 'os': 'Android', 'ram': '8GB'});
INSERT INTO shop.items (category, price, id, name, producer, props) VALUES ('phone', 799, 2, 'iPhone 15', 'Apple', {'color': 'blue', 'os': 'iOS', 'ram': '6GB'});
INSERT INTO shop.items (category, price, id, name, producer, props) VALUES ('phone', 699, 3, 'Pixel 8', 'Google', {'color': 'black', 'os': 'Android', 'ram': '8GB'});
INSERT INTO shop.items (category, price, id, name, producer, props) VALUES ('phone', 249, 4, 'Redmi Note 13', 'Xiaomi', {'color': 'black', 'os': 'Android', 'ram': '8GB'});
INSERT INTO shop.items (category, price, id, name, producer, props) VALUES ('phone', 449, 5, 'Galaxy A55', 'Samsung', {'color': 'blue', 'os': 'Android', 'ram': '8GB'});
INSERT INTO shop.items (category, price, id, name, producer, props) VALUES ('phone', 699, 6, 'Galaxy S23', 'Samsung', {'color': 'green', 'os': 'Android', 'ram': '8GB'});
INSERT INTO shop.items (category, price, id, name, producer, props) VALUES ('laptop', 1199, 7, 'MacBook Air M3', 'Apple', {'cpu': 'M3', 'ram': '16GB', 'screen': '13.6'});
INSERT INTO shop.items (category, price, id, name, producer, props) VALUES ('laptop', 1599, 8, 'ThinkPad X1', 'Lenovo', {'cpu': 'i7', 'ram': '32GB', 'screen': '14'});
INSERT INTO shop.items (category, price, id, name, producer, props) VALUES ('laptop', 1299, 9, 'XPS 13', 'Dell', {'cpu': 'i7', 'ram': '16GB', 'screen': '13.4'});
INSERT INTO shop.items (category, price, id, name, producer, props) VALUES ('laptop', 549, 10, 'IdeaPad 3', 'Lenovo', {'cpu': 'i5', 'ram': '8GB', 'screen': '15.6'});
INSERT INTO shop.items (category, price, id, name, producer, props) VALUES ('laptop', 499, 11, 'Vivobook 15', 'Asus', {'cpu': 'i5', 'ram': '8GB', 'screen': '15.6'});
INSERT INTO shop.items (category, price, id, name, producer, props) VALUES ('tv', 1399, 12, 'OLED C3', 'LG', {'diag': '55', 'panel': 'OLED', 'smart': 'yes'});
INSERT INTO shop.items (category, price, id, name, producer, props) VALUES ('tv', 599, 13, 'QLED Q60', 'Samsung', {'diag': '50', 'panel': 'QLED', 'smart': 'yes'});
INSERT INTO shop.items (category, price, id, name, producer, props) VALUES ('tv', 799, 14, 'Bravia X80L', 'Sony', {'diag': '55', 'panel': 'LED', 'smart': 'yes'});
INSERT INTO shop.items (category, price, id, name, producer, props) VALUES ('tv', 349, 15, 'Hisense A6', 'Hisense', {'diag': '43', 'panel': 'LED', 'smart': 'yes'});

SELECT * FROM shop.items;
SELECT * FROM shop.items WHERE category = 'phone';
SELECT * FROM shop.items WHERE category = 'phone' ORDER BY price DESC;
SELECT * FROM shop.items_by_name WHERE category = 'phone' AND name = 'Pixel 8';
SELECT * FROM shop.items WHERE category = 'phone' AND price >= 400 AND price <= 800;
SELECT * FROM shop.items_by_producer WHERE category = 'phone' AND producer = 'Samsung' AND price = 699;
SELECT * FROM shop.items_by_producer WHERE category = 'phone' AND producer = 'Samsung' AND price >= 400 AND price <= 800;
SELECT * FROM shop.items WHERE category = 'phone' AND props['color'] = 'black';
SELECT * FROM shop.items WHERE props['panel'] = 'LED';

CREATE TABLE shop.orders (customer text, order_date timestamp, order_id int, item_ids list<int>, total decimal, PRIMARY KEY ((customer), order_date, order_id)) WITH CLUSTERING ORDER BY (order_date ASC, order_id ASC);
INSERT INTO shop.orders (customer, order_date, order_id, item_ids, total) VALUES ('Ivan Petrenko', '2026-11-01 10:15:00+0000', 1, [4, 11], 748);
INSERT INTO shop.orders (customer, order_date, order_id, item_ids, total) VALUES ('Ivan Petrenko', '2026-11-05 18:40:00+0000', 2, [2], 799);
INSERT INTO shop.orders (customer, order_date, order_id, item_ids, total) VALUES ('Ivan Petrenko', '2026-11-12 09:05:00+0000', 3, [15, 13], 948);
INSERT INTO shop.orders (customer, order_date, order_id, item_ids, total) VALUES ('Olena Shevchenko', '2026-11-03 12:00:00+0000', 4, [7], 1199);
INSERT INTO shop.orders (customer, order_date, order_id, item_ids, total) VALUES ('Olena Shevchenko', '2026-11-10 20:30:00+0000', 5, [5, 10], 998);
INSERT INTO shop.orders (customer, order_date, order_id, item_ids, total) VALUES ('Maksym Koval', '2026-11-08 14:25:00+0000', 6, [12, 14], 2198);
INSERT INTO shop.orders (customer, order_date, order_id, item_ids, total) VALUES ('Maksym Koval', '2026-11-15 11:11:00+0000', 7, [3], 699);
INSERT INTO shop.orders (customer, order_date, order_id, item_ids, total) VALUES ('Maksym Koval', '2026-11-20 16:50:00+0000', 8, [1, 6], 1598);
DESCRIBE TABLE shop.orders;
SELECT * FROM shop.orders WHERE customer = 'Maksym Koval';
SELECT * FROM shop.orders WHERE customer = 'Maksym Koval' ORDER BY order_date DESC;
SELECT customer, COUNT(*) AS orders, SUM(total) AS total_sum FROM shop.orders GROUP BY customer;
SELECT customer, order_id, total, WRITETIME(total) FROM shop.orders;
