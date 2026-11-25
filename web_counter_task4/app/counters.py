"""
Варіанти лічильника з однаковим інтерфейсом: inc() / get() / reset() / close().
 MemoryCounter   — число в оперативній пам'яті;
 DiskCounter     — число у файлі, кожен інкремент записується на диск (fsync);
 PostgresCounter — число в таблиці PostgreSQL, in-place update (Завдання 2, Частина II);
 HazelcastCounter — IAtomicLong із CP Subsystem кластера Hazelcast (Завдання 3, Частина II);
 MongoCounter    — документ у MongoDB replica set, findOneAndUpdate з $inc і writeConcern (Завдання 4, Частина III).
Усі класи потокобезпечні: «прочитати -> +1 -> записати» виконується як одна неподільна
операція (у Memory/Disk — через threading.Lock, у PostgreSQL — через блокування рядка в UPDATE,
у MongoDB — через атомарність findOneAndUpdate над одним документом).
"""
import os
import queue
import threading
from contextlib import contextmanager

try:  # Linux / macOS
    import fcntl
except ImportError:  # Windows
    fcntl = None
try:  # Windows
    import msvcrt
except ImportError:  # Linux / macOS
    msvcrt = None

_HAS_PREAD = hasattr(os, "pread") and hasattr(os, "pwrite")  # є на Linux/macOS, немає на Windows


class MemoryCounter: #Лічильник у пам'яті

    def __init__(self):
        self._lock = threading.Lock()
        self._value = 0

    def inc(self) -> int:
        with self._lock:
            self._value += 1
            return self._value

    def get(self) -> int:
        with self._lock:
            return self._value

    def reset(self) -> None:
        with self._lock:
            self._value = 0

    def close(self) -> None:
        pass


class DiskCounter: #Лічильник у файлі з гарантією запису на диск

    RECORD_LEN = 21  # 20 цифр + символ нового рядка

    def __init__(self, path: str, fsync: bool = True):
        self._lock = threading.Lock()
        self._fsync = fsync  # False використовуємо для експериментів
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0)
        self._fd = os.open(path, flags, 0o644)
        try:
            self._lock_file()
        except OSError as e:
            os.close(self._fd)
            raise RuntimeError(f"Файл {path} уже використовує інший процес") from e
        if os.fstat(self._fd).st_size < self.RECORD_LEN:
            self._write(0)

    # внутрішні методи
    def _lock_file(self) -> None:
        if fcntl is not None:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        elif msvcrt is not None:
            os.lseek(self._fd, 0, os.SEEK_SET)
            msvcrt.locking(self._fd, msvcrt.LK_NBLCK, 1)

    def _read(self) -> int:
        if _HAS_PREAD:  # POSIX: читання за позицією
            data = os.pread(self._fd, self.RECORD_LEN, 0)
        else:  # Windows: переміщуємо курсор вручну
            os.lseek(self._fd, 0, os.SEEK_SET)
            data = os.read(self._fd, self.RECORD_LEN)
        try:
            return int(data)
        except ValueError as e:
            raise RuntimeError(f"Файл лічильника пошкоджено: {data!r}") from e

    def _write(self, value: int) -> None:
        record = b"%020d\n" % value
        if _HAS_PREAD:
            os.pwrite(self._fd, record, 0)
        else:
            os.lseek(self._fd, 0, os.SEEK_SET)
            os.write(self._fd, record)
        if self._fsync:
            os.fsync(self._fd)  # <-- гарантія запису на диск

    # публічний інтерфейс
    def inc(self) -> int:
        with self._lock:
            value = self._read() + 1
            self._write(value)
            return value

    def get(self) -> int:
        with self._lock:  # щоб не прочитати значення посеред чужого запису
            return self._read()

    def reset(self) -> None:
        with self._lock:
            self._write(0)

    def close(self) -> None:
        if msvcrt is not None:  # Windows: явно зняти блокування, не чекаючи на ОС
            try:
                os.lseek(self._fd, 0, os.SEEK_SET)
                msvcrt.locking(self._fd, msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
        os.close(self._fd)


# ---------------------------------------------------------------- PostgreSQL ---
DEFAULT_DSN = os.environ.get("PG_DSN", "host=localhost dbname=postgres user=postgres password=postgres")
USER_ID = 1


def init_table(conn) -> None: #Створює таблицю user_counter і рядок user_id=1
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS user_counter ("
                    "user_id INTEGER PRIMARY KEY, "
                    "counter INTEGER NOT NULL DEFAULT 0, "
                    "version INTEGER NOT NULL DEFAULT 0)")
        cur.execute("INSERT INTO user_counter (user_id) VALUES (%s) ON CONFLICT DO NOTHING", (USER_ID,))
    conn.commit()


def reset_row(conn) -> None: #Обнуляє counter і version у рядка user_id=1
    with conn.cursor() as cur:
        cur.execute("UPDATE user_counter SET counter = 0, version = 0 WHERE user_id = %s", (USER_ID,))
    conn.commit()


class PostgresCounter: #Лічильник у PostgreSQL: in-place update `counter = counter + 1

    def __init__(self, dsn: str = DEFAULT_DSN, pool_size: int = 16):
        import psycopg2  # імпортуємо, щоб memory/disk працювали без psycopg2
        self._conns = [psycopg2.connect(dsn) for _ in range(pool_size)]
        self._pool = queue.Queue()
        for c in self._conns:
            self._pool.put(c)
        init_table(self._conns[0])

    @contextmanager
    def _conn(self):
        conn = self._pool.get()
        try:
            yield conn
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.put(conn)

    def inc(self) -> int:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = %s RETURNING counter",
                        (USER_ID,))
            value = cur.fetchone()[0]
            conn.commit()
            return value

    def get(self) -> int:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT counter FROM user_counter WHERE user_id = %s", (USER_ID,))
            value = cur.fetchone()[0]
            conn.commit()  # завершити транзакцію, щоб з'єднання не висіло «idle in transaction»
            return value

    def reset(self) -> None:
        with self._conn() as conn:
            reset_row(conn)

    def close(self) -> None:
        for c in self._conns:
            c.close()


# ----------------------------------------------------------------- Hazelcast ---
HAZELCAST_MEMBERS = os.environ.get("HZ_MEMBERS", "127.0.0.1:5701,127.0.0.1:5702,127.0.0.1:5703")
HAZELCAST_CLUSTER = os.environ.get("HZ_CLUSTER", "dev")
HAZELCAST_COUNTER = "web-counter"


class HazelcastCounter: #Лічильник у Hazelcast: IAtomicLong із CP Subsystem (протокол Raft)

    def __init__(self, members=HAZELCAST_MEMBERS, cluster_name: str = HAZELCAST_CLUSTER,
                 name: str = HAZELCAST_COUNTER):
        import hazelcast  # імпортуємо, щоб memory/disk/postgres працювали без hazelcast
        if isinstance(members, str):
            members = [m.strip() for m in members.split(",") if m.strip()]
        # один клієнт на весь сервер: клієнт Hazelcast потокобезпечний, потоки сервера користуються ним спільно
        self._client = hazelcast.HazelcastClient(cluster_members=members, cluster_name=cluster_name)
        self._counter = self._client.cp_subsystem.get_atomic_long(name).blocking()

    def inc(self) -> int:
        return self._counter.increment_and_get()  # атомарний інкремент на боці кластера

    def get(self) -> int:
        return self._counter.get()

    def reset(self) -> None:
        self._counter.set(0)

    def close(self) -> None:
        self._client.shutdown()


# -------------------------------------------------------------------- MongoDB ---
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019/?replicaSet=rs0")
MONGO_DB = "counterdb"
MONGO_COLLECTION = "likes"
MONGO_DOC_ID = 1


class MongoCounter: #Лічильник у MongoDB: findOneAndUpdate({$inc}) із заданим writeConcern (за замовчуванням majority)

    def __init__(self, uri: str = MONGO_URI, w="majority", db: str = MONGO_DB, collection: str = MONGO_COLLECTION):
        from pymongo import MongoClient, ReturnDocument  # імпортуємо тут, щоб інші режими працювали без pymongo
        from pymongo.read_concern import ReadConcern
        from pymongo.write_concern import WriteConcern
        if isinstance(w, str) and w.isdigit():
            w = int(w)
        self._after = ReturnDocument.AFTER
        # один клієнт на весь сервер: MongoClient потокобезпечний, у нього власний пул з'єднань
        self._client = MongoClient(uri)
        coll = self._client[db][collection]
        self._write = coll.with_options(write_concern=WriteConcern(w=w))
        self._read = coll.with_options(read_concern=ReadConcern("majority"))  # читаємо лише підтверджене більшістю
        self._write.update_one({"_id": MONGO_DOC_ID}, {"$setOnInsert": {"counter": 0}}, upsert=True)

    def inc(self) -> int:
        doc = self._write.find_one_and_update({"_id": MONGO_DOC_ID}, {"$inc": {"counter": 1}},
                                              return_document=self._after)
        return doc["counter"]

    def get(self) -> int:
        return self._read.find_one({"_id": MONGO_DOC_ID})["counter"]

    def reset(self) -> None:
        self._write.update_one({"_id": MONGO_DOC_ID}, {"$set": {"counter": 0}}, upsert=True)

    def close(self) -> None:
        self._client.close()
