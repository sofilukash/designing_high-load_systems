"""
Частина II: продуктивність і цілісність каунтера у MongoDB replica set (Primary + 2 Secondary).
10 клієнтів (окремі процеси, кожен зі своїм MongoClient) одночасно роблять по 10 000 викликів
findOneAndUpdate({_id: 1}, {$inc: {counter: 1}}) з заданим writeConcern. Скрипт міряє час і перевіряє,
чи кінцеве значення дорівнює 100 000.

З --kill-primary-after N скрипт через N секунд після старту сам вимикає поточний primary
(docker kill -> раптова зупинка процесу), стежить за перевиборами, після завершення запускає вимкнену
ноду знову і показує значення лічильника на кожній ноді.

Запуск (replica set rs0 з контейнерів mongo1..mongo3 на портах 27017-27019 має працювати):
    python task4_part2.py --w 1                           # 1) writeConcern = 1
    python task4_part2.py --w majority                    # 2) writeConcern = majority
    python task4_part2.py --w 1 --kill-primary-after 8    # 3) w = 1 + відключення primary під час роботи
    python task4_part2.py --w majority --kill-primary-after 8   # 4) majority + відключення primary
    python task4_part2.py --w 1 --quick                   # швидка перевірка (1000 запитів на клієнта)
Результати накопичуються в results/task4_part2.json і results/task4_part2.md
(у режимі --quick — у results/task4_part2_quick.*).
"""
import argparse
import json
import multiprocessing as mp
import os
import subprocess
import sys
import threading
import time
from collections import Counter

from pymongo import MongoClient, ReadPreference
from pymongo.errors import PyMongoError
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

DEFAULT_URI = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019/?replicaSet=rs0")
DB, COLL, DOC_ID = "counterdb", "likes", 1
PORTS = (27017, 27018, 27019)
MAX_CLIENT_SECONDS = 20 * 60  # страховка: клієнт не крутиться вічно, якщо кластер зовсім недоступний

SCENARIOS = {  # ключ: назва в таблиці
    "w1": "writeConcern = 1",
    "wmajority": "writeConcern = majority",
    "w1_failover": "writeConcern = 1, відключення primary",
    "wmajority_failover": "writeConcern = majority, відключення primary",
}


def parse_w(text: str):
    return int(text) if text.isdigit() else text


def container_of(addr) -> str:
    """('127.0.0.1', 27018) -> 'mongo2' (контейнери названо mongo1..mongo3 за портами 27017..27019)"""
    return f"mongo{addr[1] - 27016}"


def fmt(addr) -> str:
    return "—" if addr is None else f"{addr[0]}:{addr[1]}"


# ------------------------------------------------------------------ клієнт ---
def worker(idx, uri, w, ops, barrier, progress, results):
    """Один клієнт: окремий процес і окремий MongoClient. Виконує ops УСПІШНИХ інкрементів.
    Якщо виклик завершився помилкою (наприклад, під час перевиборів), інкремент повторюється."""
    errors = Counter()
    acked = 0
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=30_000, appname=f"client{idx}")
        coll = client[DB].get_collection(COLL, write_concern=WriteConcern(w=w))
        client.admin.command("ping")  # з'єднання встановлено ще до старту
    except Exception as e:
        results.put((idx, 0, {"startup:" + type(e).__name__: 1}, time.perf_counter()))
        barrier.abort()
        return
    barrier.wait()  # усі клієнти стартують одночасно
    deadline = time.perf_counter() + MAX_CLIENT_SECONDS
    while acked < ops and time.perf_counter() < deadline:
        try:
            coll.find_one_and_update({"_id": DOC_ID}, {"$inc": {"counter": 1}})
            acked += 1
            progress[idx] = acked
        except PyMongoError as e:
            errors[type(e).__name__] += 1
            time.sleep(0.05)
    results.put((idx, acked, dict(errors), time.perf_counter()))
    client.close()


# --------------------------------------------------------------- допоміжне ---
def members_state(admin):
    status = admin.admin.command("replSetGetStatus")
    return [(m["name"], m["stateStr"]) for m in status["members"]]


def print_members(admin, title):
    print(f"{title}:")
    try:
        for name, state in members_state(admin):
            print(f"    {name:<18} {state}")
    except PyMongoError as e:
        print(f"    недоступно ({type(e).__name__})")


def node_value(port):
    """Значення лічильника, прочитане напряму з конкретної ноди (directConnection)"""
    c = MongoClient(f"mongodb://127.0.0.1:{port}/?directConnection=true", serverSelectionTimeoutMS=3000,
                    read_preference=ReadPreference.SECONDARY_PREFERRED)
    try:
        doc = c[DB][COLL].find_one({"_id": DOC_ID})
        return doc["counter"] if doc else None
    except PyMongoError as e:
        return f"недоступна ({type(e).__name__})"
    finally:
        c.close()


def node_states(admin):
    try:
        return {name: state for name, state in members_state(admin)}
    except PyMongoError:
        return {}


def format_table(rows) -> str:
    lines = ["| Сценарій | Час, с | Підтверджено клієнтами | Інкрементів/с | Кінцеве значення | Втрачено | Зайвих "
             "| Помилок клієнтів | Вибори, с |", "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for r in rows:
        gap = "—" if r.get("failover_gap") is None else f"{r['failover_gap']:.1f}"
        lines.append(f"| {r['name']} | {r['seconds']:.2f} | {r['acked']} | {r['rate']:.0f} | {r['final']} "
                     f"| {r['lost']} | {r['extra']} | {r['client_errors']} | {gap} |")
    return "\n".join(lines)


def save_results(row, quick) -> str:
    suffix = "_quick" if quick else ""
    json_path = os.path.join("results", f"task4_part2{suffix}.json")
    md_path = os.path.join("results", f"task4_part2{suffix}.md")
    os.makedirs("results", exist_ok=True)
    saved = {}
    if os.path.exists(json_path):
        with open(json_path, encoding="utf-8") as f:
            saved = json.load(f)
    saved[row["key"]] = row
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(saved, f, ensure_ascii=False, indent=2)
    table = format_table([saved[k] for k in SCENARIOS if k in saved])
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(table + "\n")
    return table


# ------------------------------------------------------------------- запуск ---
def run(args) -> dict:
    w = parse_w(args.w)
    key = f"w{args.w}" + ("_failover" if args.kill_primary_after else "")
    name = SCENARIOS.get(key, key)
    total = args.clients * args.ops
    print(f"=== {name}: {args.clients} клієнтів x {args.ops} findOneAndUpdate ===", flush=True)

    admin = MongoClient(args.uri, serverSelectionTimeoutMS=15_000)
    states = [s for _, s in members_state(admin)]
    print_members(admin, "Стан replica set перед запуском")
    if sorted(states) != ["PRIMARY", "SECONDARY", "SECONDARY"]:
        sys.exit("Replica set не в робочому стані (потрібні 1 PRIMARY і 2 SECONDARY). Запустіть усі ноди: docker start mongo1 mongo2 mongo3")

    # чистий старт: документ із каунтером; запис із majority, щоб нуль був на всіх нодах
    admin[DB].get_collection(COLL, write_concern=WriteConcern(w="majority")).replace_one(
        {"_id": DOC_ID}, {"_id": DOC_ID, "counter": 0}, upsert=True)

    ctx = mp.get_context("spawn")
    barrier = ctx.Barrier(args.clients + 1)
    progress = ctx.Array("i", args.clients, lock=False)
    results = ctx.Queue()
    procs = [ctx.Process(target=worker, args=(i, args.uri, w, args.ops, barrier, progress, results), daemon=True)
             for i in range(args.clients)]
    for p in procs:
        p.start()
    try:
        barrier.wait(timeout=120)
    except threading.BrokenBarrierError:
        for p in procs:
            p.terminate()
        sys.exit("Клієнти не змогли підключитися до MongoDB (див. рядок підключення --uri)")
    t0 = time.perf_counter()

    def log(msg):
        print(f"[{time.perf_counter() - t0:6.1f} с] {msg}", flush=True)

    primary = admin.primary
    first_primary = primary
    log(f"старт; primary = {fmt(primary)}")
    killed = new_primary = None
    kill_t = failover_gap = None
    last_print = 0.0
    while any(p.is_alive() for p in procs):
        time.sleep(0.25)
        t = time.perf_counter() - t0
        current = admin.primary
        if current != primary:
            if current is None:
                log("primary недоступний, тривають вибори")
            else:
                log(f"новий primary = {fmt(current)}")
                if killed and current != first_primary and failover_gap is None:
                    new_primary, failover_gap = current, t - kill_t
            primary = current
        if args.kill_primary_after and killed is None and t >= args.kill_primary_after and primary:
            killed, kill_t = container_of(primary), t
            r = subprocess.run(["docker", "kill", killed], capture_output=True, text=True)
            log(f"docker kill {killed} (primary {fmt(primary)}): {(r.stdout + r.stderr).strip()}")
        if t - last_print >= 10:
            last_print = t
            log(f"виконано {sum(progress)} / {total}")

    got = [results.get(timeout=60) for _ in procs]
    elapsed = max(r[3] for r in got) - t0
    acked = sum(r[1] for r in got)
    errs = Counter()
    for r in got:
        errs.update(r[2])
    if args.kill_primary_after and killed is None:
        print("УВАГА: запуск завершився раніше, ніж primary було вимкнено; зменшіть --kill-primary-after")
    log(f"усі клієнти завершили; підтверджено інкрементів: {acked}")

    # кінцеве значення: читання з readConcern majority (підтверджене більшістю нод)
    final = None
    for _ in range(30):
        try:
            doc = admin[DB].get_collection(COLL, read_concern=ReadConcern("majority")).find_one({"_id": DOC_ID})
            final = doc["counter"]
            break
        except PyMongoError:
            time.sleep(2)
    lost = max(acked - final, 0) if final is not None else None
    extra = max(final - acked, 0) if final is not None else None

    if killed:  # повертаємо вимкнену ноду й чекаємо, поки вона наздожене кластер
        r = subprocess.run(["docker", "start", killed], capture_output=True, text=True)
        log(f"docker start {killed}: {(r.stdout + r.stderr).strip()}")
        for _ in range(45):
            st = node_states(admin)
            if len(st) == 3 and all(s in ("PRIMARY", "SECONDARY") for s in st.values()):
                break
            time.sleep(2)
    print_members(admin, "Стан replica set після запуску")
    print("Значення лічильника на кожній ноді (пряме підключення; чекаємо до 30 с, поки нода наздожене):")
    for port in PORTS:
        v = node_value(port)
        for _ in range(15):
            if v == final:
                break
            time.sleep(2)
            v = node_value(port)
        print(f"    127.0.0.1:{port}  {v}")

    row = {"key": key, "name": name, "w": args.w, "kill": bool(args.kill_primary_after), "seconds": elapsed,
           "rate": acked / elapsed, "acked": acked, "final": final, "expected": total, "lost": lost,
           "extra": extra, "client_errors": sum(errs.values()), "error_kinds": dict(errs),
           "killed": killed, "first_primary": fmt(first_primary), "new_primary": fmt(new_primary),
           "failover_gap": failover_gap}
    ok = "OK" if final == total else "значення відрізняється від очікуваного"
    print(f"\n{name}: час {elapsed:.2f} с, {row['rate']:.0f} інкрементів/с; підтверджено {acked}, кінцеве значення "
          f"{final} (очікується {total}), втрачено {lost}, зайвих {extra}, помилок клієнтів {sum(errs.values())} "
          f"{dict(errs) if errs else ''} [{ok}]")
    if failover_gap is not None:
        print(f"Новий primary {fmt(new_primary)} з'явився через {failover_gap:.1f} с після вимкнення {killed}")
    admin.close()
    return row


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--w", default="majority", help="writeConcern для findOneAndUpdate: 1, 2, 3 або majority")
    ap.add_argument("--uri", default=DEFAULT_URI, help="рядок підключення (мають бути перелічені всі ноди)")
    ap.add_argument("--clients", type=int, default=10)
    ap.add_argument("--ops", type=int, default=10_000, help="інкрементів НА КОЖНОГО клієнта")
    ap.add_argument("--kill-primary-after", type=float, default=0, metavar="СЕК",
                    help="через стільки секунд після старту вимкнути поточний primary (docker kill)")
    ap.add_argument("--quick", action="store_true", help="1000 інкрементів на клієнта; результати окремо від повних")
    args = ap.parse_args()
    if args.quick:
        args.ops = 1000
    row = run(args)
    print("\n" + save_results(row, args.quick))


if __name__ == "__main__":
    main()
