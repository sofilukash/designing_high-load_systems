"""
Автотести ядра. Запуск з кореня проєкту:
    python -m unittest -v tests.test_counters
"""
import http.client
import os
import sys
import tempfile
import threading
import unittest

from app.counters import (CASSANDRA_HOSTS, DEFAULT_DSN, MONGO_URI, CassandraCounter, DiskCounter, MemoryCounter,
                          MongoCounter, PostgresCounter)
from app.server import CounterServer, make_handler


def hammer(counter, threads: int, per_thread: int) -> None: #Багато потоків одночасно викликають inc()

    def work():
        for _ in range(per_thread):
            counter.inc()

    ts = [threading.Thread(target=work) for _ in range(threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()


class CounterTests(unittest.TestCase):
    def setUp(self):
        # Інтерпретатор перемикає потоки кожні 5 мс. Ставимо 1 мкс, щоб перемикання
        self._old_interval = sys.getswitchinterval()
        sys.setswitchinterval(1e-6)

    def tearDown(self):
        sys.setswitchinterval(self._old_interval)

    def test_memory_no_lost_update(self):
        c = MemoryCounter()
        hammer(c, threads=20, per_thread=10_000)
        self.assertEqual(c.get(), 200_000)

    def test_disk_no_lost_update(self):
        with tempfile.TemporaryDirectory() as d:
            c = DiskCounter(os.path.join(d, "c.dat"), fsync=False)  # перевіряємо атомарність
            hammer(c, threads=20, per_thread=500)
            self.assertEqual(c.get(), 10_000)
            c.close()

    def test_disk_value_is_really_in_the_file(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "c.dat")
            c = DiskCounter(path)  # з fsync
            for _ in range(7):
                c.inc()
            with open(path, "rb") as f:  # читаємо файл незалежно від лічильника
                self.assertEqual(int(f.read()), 7)
            c.close()

    def test_disk_survives_restart(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "c.dat")
            c = DiskCounter(path, fsync=False)
            hammer(c, threads=4, per_thread=250)
            c.close()
            c2 = DiskCounter(path)  # перезапуск сервера
            self.assertEqual(c2.get(), 1000)
            c2.inc()
            self.assertEqual(c2.get(), 1001)
            c2.close()

    @unittest.skipIf(os.name == "nt", "flock на Windows працює інакше")
    def test_disk_file_is_locked_against_second_opener(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "c.dat")
            c = DiskCounter(path, fsync=False)
            with self.assertRaises(RuntimeError):
                DiskCounter(path, fsync=False)
            c.close()

    def test_reset(self):
        for c in (MemoryCounter(),):
            c.inc()
            c.reset()
            self.assertEqual(c.get(), 0)


def pg_available() -> bool:
    try:
        import psycopg2
        psycopg2.connect(DEFAULT_DSN, connect_timeout=2).close()
        return True
    except Exception:
        return False


@unittest.skipUnless(pg_available(), "PostgreSQL недоступний (задайте PG_DSN)")
class PostgresCounterTests(unittest.TestCase):
    def test_no_lost_update(self):
        c = PostgresCounter(pool_size=10)
        c.reset()
        hammer(c, threads=10, per_thread=200)
        self.assertEqual(c.get(), 2000)
        c.close()

    def test_value_survives_reconnect(self):
        c = PostgresCounter()
        c.reset()
        c.inc()
        c.inc()
        c.close()
        c2 = PostgresCounter()  # нове підключення
        self.assertEqual(c2.get(), 2)
        c2.close()


def mongo_available() -> bool:
    try:
        from pymongo import MongoClient
        c = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        c.admin.command("ping")
        c.close()
        return True
    except Exception:
        return False


@unittest.skipUnless(mongo_available(), "MongoDB недоступна (задайте MONGO_URI)")
class MongoCounterTests(unittest.TestCase):
    def test_no_lost_update(self):
        c = MongoCounter(db="counterdb_test")
        c.reset()
        hammer(c, threads=10, per_thread=200)
        self.assertEqual(c.get(), 2000)
        c.close()

    def test_value_survives_reconnect(self):
        c = MongoCounter(db="counterdb_test")
        c.reset()
        c.inc()
        c.inc()
        c.close()
        c2 = MongoCounter(db="counterdb_test")  # нове підключення
        self.assertEqual(c2.get(), 2)
        c2.close()


def cassandra_available() -> bool:
    try:
        from cassandra.cluster import Cluster
        cluster = Cluster(CASSANDRA_HOSTS.split(","), protocol_version=4, connect_timeout=3)
        try:
            cluster.connect()
        finally:
            cluster.shutdown()
        return True
    except Exception:
        return False


@unittest.skipUnless(cassandra_available(), "Cassandra недоступна (задайте CASSANDRA_HOSTS)")
class CassandraCounterTests(unittest.TestCase):
    def test_no_lost_update_one(self):
        c = CassandraCounter(consistency="ONE", keyspace="counterdb_test")
        c.reset()
        hammer(c, threads=10, per_thread=200)
        self.assertEqual(c.get(), 2000)
        c.close()

    def test_no_lost_update_quorum(self):
        c = CassandraCounter(consistency="QUORUM", keyspace="counterdb_test")
        c.reset()
        hammer(c, threads=10, per_thread=200)
        self.assertEqual(c.get(), 2000)
        c.close()

    def test_reset_works_repeatedly(self):  # обнулення відніманням має працювати багато разів поспіль
        c = CassandraCounter(keyspace="counterdb_test")
        for n in (5, 3, 0, 7):
            c.reset()
            self.assertEqual(c.get(), 0)
            for _ in range(n):
                c.inc()
            self.assertEqual(c.get(), n)
        c.close()

    def test_value_survives_reconnect(self):
        c = CassandraCounter(keyspace="counterdb_test")
        c.reset()
        c.inc()
        c.inc()
        c.close()
        c2 = CassandraCounter(keyspace="counterdb_test")  # нове підключення
        self.assertEqual(c2.get(), 2)
        c2.close()


class HttpTests(unittest.TestCase): #Піднімаємо справжній сервер у потоці й б'ємо по ньому з кількох клієнтів

    def _check(self, counter):
        server = CounterServer(("127.0.0.1", 0), make_handler(counter))  # порт 0 = будь-який вільний
        port = server.server_address[1]
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            def client():
                conn = http.client.HTTPConnection("127.0.0.1", port)
                for _ in range(200):
                    conn.request("GET", "/inc")
                    r = conn.getresponse()
                    r.read()
                    self.assertEqual(r.status, 200)
                conn.close()

            ts = [threading.Thread(target=client) for _ in range(5)]
            [t.start() for t in ts]
            [t.join() for t in ts]
            conn = http.client.HTTPConnection("127.0.0.1", port)
            conn.request("GET", "/count")
            self.assertEqual(int(conn.getresponse().read()), 1000)
            conn.request("GET", "/nothing")
            r = conn.getresponse()
            r.read()
            self.assertEqual(r.status, 404)
            conn.close()
        finally:
            server.shutdown()
            server.server_close()

    def test_http_memory(self):
        self._check(MemoryCounter())

    @unittest.skipUnless(pg_available(), "PostgreSQL недоступний (задайте PG_DSN)")
    def test_http_postgres(self):
        c = PostgresCounter(pool_size=8)
        c.reset()
        self._check(c)
        c.close()

    @unittest.skipUnless(mongo_available(), "MongoDB недоступна (задайте MONGO_URI)")
    def test_http_mongodb(self):
        c = MongoCounter(db="counterdb_test")
        c.reset()
        self._check(c)
        c.close()

    @unittest.skipUnless(cassandra_available(), "Cassandra недоступна (задайте CASSANDRA_HOSTS)")
    def test_http_cassandra(self):
        c = CassandraCounter(consistency="QUORUM", keyspace="counterdb_test")
        c.reset()
        self._check(c)
        c.close()

    def test_http_disk(self):
        with tempfile.TemporaryDirectory() as d:
            c = DiskCounter(os.path.join(d, "c.dat"), fsync=False)
            self._check(c)
            c.close()


if __name__ == "__main__":
    unittest.main()
