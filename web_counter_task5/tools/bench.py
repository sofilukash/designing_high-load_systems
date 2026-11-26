"""
Автоматичний замір одного режиму сервера (memory / disk / postgres / hazelcast / mongodb / cassandra).
Використовує task5_part3.py.
"""
import csv
import os
import subprocess
import sys
import time

from tools.loadgen import format_run, http_get, run_load

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_DIR = os.path.join(ROOT, "results")
CSV_FIELDS = ["mode", "clients", "run", "requests_per_client", "total_requests", "wall_seconds", "rps",
              "avg_ms", "p50_ms", "p99_ms", "errors", "final_count", "expected_count", "store_value", "ok"]


def start_server(mode: str, port: int, counter_file: str, dsn: str, cass_cl: str = "ONE",
                 label: str = "") -> subprocess.Popen:
    cmd = [sys.executable, "-m", "app.server", "--mode", mode, "--port", str(port)]
    if mode == "disk":
        cmd += ["--file", counter_file]
    if mode == "cassandra":
        cmd += ["--cass-cl", cass_cl]
    label = label or mode
    os.makedirs(RESULTS_DIR, exist_ok=True)
    log = open(os.path.join(RESULTS_DIR, f"server_{label}.log"), "w")
    proc = subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, env={**os.environ, "PG_DSN": dsn})
    deadline = time.time() + 40  # Cassandra: підключення драйвера і перевірка схеми можуть тривати кілька секунд
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"Сервер не запустився (див. results/server_{label}.log)")
        try:
            http_get("127.0.0.1", port, "/count")
            return proc
        except OSError:
            time.sleep(0.1)
    proc.kill()
    raise RuntimeError("Сервер не відповів за відведений час")


def stop_server(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


def read_store(mode: str, counter_file: str, dsn: str): #Значення лічильника, прочитане напряму зі сховища
    if mode == "disk":
        with open(counter_file, "rb") as f:
            return int(f.read())
    if mode == "hazelcast":
        from app.counters import HazelcastCounter
        counter = HazelcastCounter()
        try:
            return counter.get()
        finally:
            counter.close()
    if mode == "mongodb":
        from app.counters import MongoCounter
        counter = MongoCounter()
        try:
            return counter.get()
        finally:
            counter.close()
    if mode == "cassandra":
        from app.counters import CassandraCounter
        counter = CassandraCounter()  # читає з consistency ALL, окремим з'єднанням, а не через сервер
        try:
            return counter.get()
        finally:
            counter.close()
    if mode == "postgres":
        import psycopg2
        conn = psycopg2.connect(dsn)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT counter FROM user_counter WHERE user_id = 1")
                return cur.fetchone()[0]
        finally:
            conn.close()
    return ""


def read_file_after_stop(path: str, timeout: float = 10.0):
    deadline = time.time() + timeout
    while True:
        try:
            return read_store("disk", path, "")
        except PermissionError:
            if time.time() > deadline:
                return None
            time.sleep(0.2)


def run_mode(mode: str, dsn: str, clients: int = 10, requests: int = 10_000, repeats: int = 3,
             port: int = 8080, cass_cl: str = "ONE") -> list:
    """Повертає список замірів (по одному на повтор)"""
    label = f"{mode}_{cass_cl}" if mode == "cassandra" else mode
    counter_file = os.path.join(RESULTS_DIR, "counter.dat")
    if mode == "disk" and os.path.exists(counter_file):
        os.remove(counter_file)  # чистий старт

    print(f"\n=== {label}: {clients} клієнтів x {requests} запитів, повторів: {repeats} ===", flush=True)
    server = start_server(mode, port, counter_file, dsn, cass_cl, label)
    rows = []
    try:
        for run in range(1, repeats + 1):
            r = run_load(f"http://127.0.0.1:{port}", clients, requests)
            store = "" if mode == "disk" else read_store(mode, counter_file, dsn)
            r.update(mode=label, run=run, store_value=store)
            if mode in ("postgres", "hazelcast", "mongodb", "cassandra"):
                r["ok"] = r["ok"] and store == r["expected_count"]
            print(format_run(f"{label}#{run}", r), flush=True)
            rows.append(r)
            if not r["ok"]:
                raise RuntimeError("Лічильник показує неправильне значення — оновлення губляться або дублюються!")
    finally:
        stop_server(server)
    if mode == "disk":
        value = read_file_after_stop(counter_file)
        expected = rows[-1]["expected_count"]
        if value is None:
            print("файл усе ще заблоковано ОС — пряму перевірку файлу пропущено")
        else:
            rows[-1]["store_value"] = value
            print(f"файл {counter_file}: {value} (очікувалось {expected})")
            if value != expected:
                raise RuntimeError("Значення у файлі не збігається з очікуваним!")
    return rows


def median_run(rows: list) -> dict:
    """Медіанний за RPS замір (стійкий до викидів)"""
    return sorted(rows, key=lambda r: r["rps"])[len(rows) // 2]


def save_csv(rows: list, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
