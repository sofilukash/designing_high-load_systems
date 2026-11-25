# Web-counter (Task 4)

Реплікація та відмовостійкість MongoDB 4.4 (replica set rs0 із 3 нод у Docker). Частина I: writeConcern w:3 без wtimeout і з wtimeout, readConcern majority, вибори primary. Частина II: лічильник у MongoDB (10 клієнтів x 10 000 findOneAndUpdate; w=1, w=majority, з вимкненням primary). Частина III: MongoDB як сховище web-counter порівняно з in-memory, disk, PostgreSQL і Hazelcast. Мова: Python (pymongo).

## Запуск

~~~bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# replica set: 3 ноди (Docker, --network host)
for i in 1 2 3; do
  docker run -d --name mongo$i --network host -m 1g mongo:4.4 \
    mongod --replSet rs0 --port $((27016+i)) --bind_ip 127.0.0.1 \
    --wiredTigerCacheSizeGB 0.25 --oplogSize 512
done
docker exec mongo1 mongo --port 27017 --quiet --eval 'rs.initiate({_id:"rs0", members:[{_id:0,host:"127.0.0.1:27017"},{_id:1,host:"127.0.0.1:27018"},{_id:2,host:"127.0.0.1:27019"}]}).ok'

source mongo_env.sh
bash part1/exp1_w3_no_timeout.sh   # Частина I, експеримент 1
bash part1/exp2_w3_timeout.sh      # Частина I, експеримент 2
bash part1/exp3_election.sh        # Частина I, експеримент 3

python task4_part2.py --w 1                                 # Частина II
python task4_part2.py --w majority
python task4_part2.py --w 1 --kill-primary-after 8
python task4_part2.py --w majority --kill-primary-after 8

python task4_part3.py --quick     # Частина III, швидка перевірка (потрібні PostgreSQL і кластер Hazelcast)
python task4_part3.py --repeats 1 # Частина III, повний прогін
~~~

MongoDB 4.4 обрано, бо версії 5.0+ вимагають процесор з AVX. Результати: `results/`.
