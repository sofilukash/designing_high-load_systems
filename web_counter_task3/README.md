# Web-counter (Task 3)

Лічильник на Hazelcast 5.4.0 (кластер із 3 нод у Docker, CP Subsystem). Частина I: Distributed Map без блокувань, з песимістичним і оптимістичним блокуванням та IAtomicLong. Частина II: Hazelcast як сховище для web-counter порівняно з in-memory, disk і PostgreSQL. Мова: Python (hazelcast-python-client 5.4.0).

## Запуск

~~~bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# кластер із CP Subsystem: 3 ноди (Docker, --network host)
for i in 1 2 3; do
  docker run -d --name member$i --network host -m 1g \
    -v "$PWD/hazelcast":/opt/hazelcast/config_ext \
    -e JAVA_OPTS="-Dhazelcast.config=/opt/hazelcast/config_ext/hz-cp.yaml" \
    hazelcast/hazelcast:5.4.0
  sleep 8
done

python task3_part1.py --quick     # Частина I, швидка перевірка
python task3_part1.py             # Частина I, повний прогін (10 потоків x 10 000)
python task3_part2.py             # Частина II (потрібні PostgreSQL і кластер)
~~~

Конфіг без CP Subsystem: `hazelcast/hz-plain.yaml`. Логи нод: `logs/`, результати: `results/`.
