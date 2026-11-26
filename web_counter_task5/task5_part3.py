"""
Частина III: web-counter із Завдання 1 на Cassandra — колонка типу counter у keyspace з RF=3
(UPDATE ... SET cnt = cnt + 1), consistency level запису ONE і QUORUM.
Для кожного рівня скрипт сам запускає сервер, 10 клієнтів одночасно роблять по 10 000 викликів /inc,
перевіряється кінцеве значення (/count і напряму у Cassandra, читання з consistency ALL = 100 000),
сервер зупиняється. Далі друкується підсумкова таблиця: перші п'ять рядків — результати Завдання 4
(повний прогін, сховища НЕ перезапускаються), останні два — Cassandra з цього запуску.
Запуск (кластер Cassandra з 3 нод cass1..cass3 у мережі 172.30.0.0/24 має працювати):
    python task5_part3.py --quick             # швидка перевірка: 1000 запитів на клієнта, 1 повтор
    python task5_part3.py --repeats 1         # повний прогін: 10 x 10 000 для ONE і QUORUM
    python task5_part3.py --only QUORUM       # лише один рівень (таблиця результатів перезапишеться!)
Результати: results/task5_part3.csv і results/task5_part3.md (у режимі --quick — з суфіксом _quick).
"""
import argparse
import os

from app.counters import DEFAULT_DSN
from tools.bench import RESULTS_DIR, median_run, run_mode, save_csv

# Перші п'ять рядків — із повного прогону Завдання 4 (10 клієнтів x 10 000, 1 повтор, 8 vCPU).
PREVIOUS = (("In-memory", 79.67, 1255),
            ("Disk (fsync)", 973.68, 103),
            ("PostgreSQL (in-place update)", 220.81, 453),
            ("Hazelcast (IAtomicLong, CP Subsystem)", 77.85, 1285),
            ("MongoDB (findOneAndUpdate, w=majority)", 696.46, 144))

LEVELS = (("ONE", "Cassandra (counter, RF=3, CL=ONE)"), ("QUORUM", "Cassandra (counter, RF=3, CL=QUORUM)"))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--clients", type=int, default=10)
    ap.add_argument("--requests", type=int, default=10_000, help="запитів НА КОЖНОГО клієнта")
    ap.add_argument("--repeats", type=int, default=1, help="повторів на кожний рівень (береться медіана)")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--quick", action="store_true", help="1000 запитів, 1 повтор; результати окремо від повних")
    ap.add_argument("--only", choices=[lvl for lvl, _ in LEVELS], help="запустити лише один рівень consistency")
    args = ap.parse_args()
    if args.quick:
        args.requests, args.repeats = 1000, 1
    suffix = "_quick" if args.quick else ""

    all_rows, lines = [], ["| Test scenarios | Total time | Throughput |", "|---|---:|---:|"]
    if not args.only:
        lines += [f"| {title} | {seconds:.2f} s | {rps:.0f} req/s |" for title, seconds, rps in PREVIOUS]
    for level, title in LEVELS:
        if args.only and level != args.only:
            continue
        runs = run_mode("cassandra", DEFAULT_DSN, args.clients, args.requests, args.repeats, args.port, level)
        all_rows += runs
        m = median_run(runs)
        lines.append(f"| {title} | {m['wall_seconds']:.2f} s | {m['rps']:.0f} req/s |")

    table = "\n".join(lines)
    print(f"\n{args.clients} клієнтів x {args.requests} запитів = {args.clients * args.requests} (медіана за RPS):\n")
    print(table)
    if not args.only:
        print("\nПерші п'ять рядків узято з повного прогону Завдання 4 (інше фонове навантаження, не перезапускались).")
    save_csv(all_rows, os.path.join(RESULTS_DIR, f"task5_part3{suffix}.csv"))
    with open(os.path.join(RESULTS_DIR, f"task5_part3{suffix}.md"), "w", encoding="utf-8") as f:
        f.write(table + "\n")


if __name__ == "__main__":
    main()
