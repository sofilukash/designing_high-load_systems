"""
Частина I: способи оновлення лічильника в Hazelcast + вимірювання часу.
Кожен варіант: 10 потоків, кожен зі СВОЇМ клієнтом Hazelcast, по 10 000 ітерацій над одним і тим самим
ключем. Потоки стартують одночасно (Barrier), час міряється від спільного старту до завершення останнього потоку.

Варіанти (--variant):
    nolock        Distributed Map без блокувань: get -> +1 -> put (оновлення губляться)
    pessimistic   Distributed Map + песимістичне блокування: lock(key) ... unlock(key)
    optimistic    Distributed Map + оптимістичне блокування: replace_if_same (повтор, якщо значення змінилось)
    atomic        IAtomicLong з CP Subsystem (потрібен кластер із cp-member-count: 3)
    all           усі чотири підряд (за замовчуванням)

Запуск (кластер Hazelcast 5.4.0 має працювати на 127.0.0.1:5701-5703):
    python task3_part1.py --variant nolock --quick        # швидка перевірка: 1000 ітерацій на потік
    python task3_part1.py --variant nolock                # повний прогін одного варіанта
    python task3_part1.py                                 # усі варіанти
Результати накопичуються в results/hz_part1.json і results/hz_part1.md
(у режимі --quick — у results/hz_part1_quick.*), тому варіанти можна запускати по одному.
"""
import argparse
import json
import logging
import os
import threading
import time

import hazelcast

MAP_NAME = "counter-map"
ATOMIC_NAME = "counter-atomic"
KEY = "counter"
DEFAULT_MEMBERS = "127.0.0.1:5701,127.0.0.1:5702,127.0.0.1:5703"
PROGRESS_EVERY = 15  # секунд між рядками прогресу


# ----------------------------------------------------------- варіанти оновлення ---
# Кожна функція виконує n ітерацій і повертає (виконано_ітерацій, повторів).
# tick() — лише лічильник прогресу для консолі, на вимірювання не впливає.

def map_no_lock(client, n, tick):
    """Без блокувань: між get і put інший потік встигає змінити значення -> оновлення губляться"""
    counter = client.get_map(MAP_NAME).blocking()
    for _ in range(n):
        value = counter.get(KEY)
        counter.put(KEY, value + 1)
        tick()
    return n, 0


def map_pessimistic(client, n, tick):
    """lock(key) блокує ключ для всього кластера; «прочитати -> +1 -> записати» стає неподільним"""
    counter = client.get_map(MAP_NAME).blocking()
    for _ in range(n):
        counter.lock(KEY)
        try:
            value = counter.get(KEY)
            counter.put(KEY, value + 1)
        finally:
            counter.unlock(KEY)
        tick()
    return n, 0


def map_optimistic(client, n, tick):
    """Без блокувань: replace_if_same(key, old, new) замінює значення лише якщо воно досі дорівнює old;
    інакше (False) хтось встиг змінити ключ — повторюємо з новим значенням"""
    counter = client.get_map(MAP_NAME).blocking()
    retries = 0
    for _ in range(n):
        while True:
            old = counter.get(KEY)
            if counter.replace_if_same(KEY, old, old + 1):
                break
            retries += 1
        tick()
    return n, retries


def atomic_long(client, n, tick):
    """IAtomicLong із CP Subsystem (Raft): інкремент виконується атомарно на боці кластера"""
    counter = client.cp_subsystem.get_atomic_long(ATOMIC_NAME).blocking()
    for _ in range(n):
        counter.increment_and_get()
        tick()
    return n, 0


def reset_map(client):
    client.get_map(MAP_NAME).blocking().put(KEY, 0)


def read_map(client):
    return client.get_map(MAP_NAME).blocking().get(KEY)


def reset_atomic(client):
    client.cp_subsystem.get_atomic_long(ATOMIC_NAME).blocking().set(0)


def read_atomic(client):
    return client.cp_subsystem.get_atomic_long(ATOMIC_NAME).blocking().get()


VARIANTS = {  # ключ: (назва в таблиці, функція, скинути, прочитати)
    "nolock": ("Map без блокувань", map_no_lock, reset_map, read_map),
    "pessimistic": ("Map + песимістичне блокування", map_pessimistic, reset_map, read_map),
    "optimistic": ("Map + оптимістичне блокування", map_optimistic, reset_map, read_map),
    "atomic": ("IAtomicLong (CP Subsystem)", atomic_long, reset_atomic, read_atomic),
}


# -------------------------------------------------------------------- запуск ---
def connect(members, cluster_name):
    return hazelcast.HazelcastClient(cluster_members=members, cluster_name=cluster_name)


def run_variant(key, members, cluster_name, admin, threads, iterations) -> dict:
    name, worker, reset, read = VARIANTS[key]
    reset(admin)
    clients = [connect(members, cluster_name) for _ in range(threads)]  # окремий клієнт на потік

    barrier = threading.Barrier(threads + 1)
    results = [None] * threads
    progress = [0] * threads

    def run(i):
        def tick():
            progress[i] += 1
        try:
            barrier.wait(timeout=120)
            results[i] = worker(clients[i], iterations, tick)
        except Exception as e:  # не губити помилку потоку
            results[i] = e

    pool = [threading.Thread(target=run, args=(i,), daemon=True) for i in range(threads)]
    for t in pool:
        t.start()
    barrier.wait(timeout=120)
    t0 = time.perf_counter()
    total = threads * iterations
    while any(t.is_alive() for t in pool):
        for t in pool:
            t.join(timeout=PROGRESS_EVERY / threads)
        if any(t.is_alive() for t in pool):
            print(f"    ... {time.perf_counter() - t0:6.0f} с: {sum(progress)} / {total}", flush=True)
    seconds = time.perf_counter() - t0
    final = read(admin)
    for c in clients:
        c.shutdown()

    for r in results:
        if isinstance(r, Exception):
            raise r
    done = sum(r[0] for r in results)
    return {"key": key, "name": name, "seconds": seconds, "rate": done / seconds, "final": final,
            "expected": total, "lost": total - final, "retries": sum(r[1] for r in results)}


def format_table(rows) -> str:
    lines = ["| Варіант | Час, с | Інкрементів/с | Кінцеве значення | Очікується | Втрачено оновлень | Повторів |",
             "|---|---:|---:|---:|---:|---:|---:|"]
    for r in rows:
        lines.append(f"| {r['name']} | {r['seconds']:.2f} | {r['rate']:.0f} | {r['final']} "
                     f"| {r['expected']} | {r['lost']} | {r['retries']} |")
    return "\n".join(lines)


def save_results(new_rows, quick) -> str:
    """Додає нові заміри до тих, що вже збережені, і перебудовує таблицю у порядку VARIANTS"""
    suffix = "_quick" if quick else ""
    json_path = os.path.join("results", f"hz_part1{suffix}.json")
    md_path = os.path.join("results", f"hz_part1{suffix}.md")
    os.makedirs("results", exist_ok=True)
    saved = {}
    if os.path.exists(json_path):
        with open(json_path, encoding="utf-8") as f:
            saved = json.load(f)
    for r in new_rows:
        saved[r["key"]] = r
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(saved, f, ensure_ascii=False, indent=2)
    table = format_table([saved[k] for k in VARIANTS if k in saved])
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(table + "\n")
    return table


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--variant", choices=[*VARIANTS, "all"], default="all")
    ap.add_argument("--members", default=DEFAULT_MEMBERS, help="адреси нод через кому")
    ap.add_argument("--cluster-name", default="dev", help="ім'я кластера (у Docker-образі за замовчуванням dev)")
    ap.add_argument("--threads", type=int, default=10)
    ap.add_argument("--iterations", type=int, default=10_000, help="ітерацій на потік")
    ap.add_argument("--quick", action="store_true", help="1000 ітерацій на потік; результати окремо від повних")
    ap.add_argument("--verbose", action="store_true", help="показувати лог клієнта Hazelcast (INFO)")
    args = ap.parse_args()
    if args.quick:
        args.iterations = 1000
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING)
    members = [m.strip() for m in args.members.split(",") if m.strip()]
    keys = list(VARIANTS) if args.variant == "all" else [args.variant]

    admin = connect(members, args.cluster_name)  # службовий клієнт: скидання й перевірка значення
    rows = []
    try:
        for key in keys:
            print(f"=== {VARIANTS[key][0]}: {args.threads} потоків x {args.iterations} ітерацій ===", flush=True)
            r = run_variant(key, members, args.cluster_name, admin, args.threads, args.iterations)
            ok = "OK" if r["lost"] == 0 else "значення не збігається з очікуваним"
            print(f"{r['name']:<32} {r['seconds']:8.2f} с  final={r['final']}  "
                  f"втрачено={r['lost']}  повторів={r['retries']}  [{ok}]", flush=True)
            rows.append(r)
    finally:
        admin.shutdown()

    print("\n" + save_results(rows, args.quick))


if __name__ == "__main__":
    main()
