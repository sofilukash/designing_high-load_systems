# Web-counter (Task 5)

Робота з Apache Cassandra 5.0 (кластер із 3 нод у Docker, мережа 172.30.0.0/24). Частина 1: CQL — таблиці `items` (partition key `category`, `map` з індексом, materialized views) і `orders`. Частина 2: keyspace з RF 1, 2, 3, розподіл даних, `getendpoints`, відмова ноди й рівні consistency, розрив мережі та конфлікт записів (last-write-wins). Частина 3: Cassandra (тип `counter`, RF=3) як сховище web-counter, CL ONE і QUORUM, порівняно з in-memory, disk, PostgreSQL, Hazelcast і MongoDB. Мова: Python (cassandra-driver).

## Запуск

~~~bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# кластер: 3 ноди Cassandra 5.0 (materialized views увімкнено), IP 172.30.0.11-13
bash cassandra/cluster_up.sh
source cassandra/cass_env.sh        # функції cq (CQL) і nt (nodetool); NODE=2 cq "..." виконує на cass2

# Частина 1
docker exec -i cass1 cqlsh < cassandra/part1.cql

# Частина 2
cq "CREATE KEYSPACE ks_rf3 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};"   # так само ks_rf1, ks_rf2
cq "CREATE TABLE ks_rf3.kv (id int PRIMARY KEY, val text);"
cq "COPY ks_rf3.kv (id, val) FROM '/tmp/kv.csv' WITH NUMPROCESSES=2;"
nt status ks_rf3; nt getendpoints ks_rf3 kv 1; NODE=2 nt tablestats ks_rf3.kv
docker stop cass3; try_cl ks_rf3 TWO 1; docker start cass3                 # відмова ноди й рівні consistency
for n in 1 2 3; do docker network disconnect cassnet cass$n; done          # розрив мережі між нодами
NODE=1 cq "CONSISTENCY ONE; INSERT INTO ks_rf3.kv (id, val) VALUES (100, 'value from cass1');"
for n in 1 2 3; do docker network connect --ip 172.30.0.1$n cassnet cass$n; done
cq "CONSISTENCY ALL; SELECT id, val, WRITETIME(val) FROM ks_rf3.kv WHERE id = 100;"

# Частина 3
python -m unittest -v tests.test_counters
python task5_part3.py --quick       # швидка перевірка
python task5_part3.py --repeats 3   # 10 клієнтів x 10 000 запитів, CL ONE і QUORUM, медіана з 3 повторів
~~~

Результати: `results/`. Перші п'ять рядків підсумкової таблиці Частини 3 взято з повного прогону Завдання 4.
