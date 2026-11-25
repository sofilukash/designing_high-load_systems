#!/bin/bash
# Частина I, експеримент 1: запис з w:3 при вимкненій ноді, без таймауту
cd "$(dirname "$0")/.." && source ./mongo_env.sh
now() { date +%T; }
cleanup() {
  docker ps --filter ancestor=mongo:4.4 --format '{{.ID}} {{.Names}}' | grep -v -E ' mongo[123]$' | cut -d' ' -f1 | xargs -r docker rm -f >/dev/null 2>&1
  docker start mongo3 >/dev/null 2>&1
}
trap cleanup INT TERM
show() { ms --eval 'rs.status().members.forEach(function(m){ print("   ", m.name, m.stateStr) })'; }

ms --eval 'db.getSiblingDB("lab4").dropDatabase()' >/dev/null
echo "=== Експеримент 1: writeConcern w:3, одна нода вимкнена, wtimeout не заданий ==="
echo "$(now) стан replica set перед експериментом:"; show
echo "$(now) вимикаємо mongo3"
docker stop mongo3 >/dev/null

T0=$(date +%s)
echo "$(now) запускаємо запис insertOne з writeConcern {w:3} (у фоні)"
( ms --eval 'var r = db.getSiblingDB("lab4").exp.insertOne({exp:"w3-no-timeout"}, {writeConcern:{w:3}}); print("    результат:", tojson(r, "", true))'
  echo "$(now) <- запис підтверджено клієнту (чекав $(( $(date +%s) - T0 )) с)" ) &
BG=$!

sleep 20
if kill -0 $BG 2>/dev/null; then echo "$(now) минуло 20 с: запис ще НЕ підтверджено, клієнт чекає на 3-тю ноду"
else echo "УВАГА: запис завершився до старту mongo3"; fi
echo "$(now) стан replica set зараз:"; show
echo "$(now) запускаємо mongo3"
docker start mongo3 >/dev/null
wait $BG

echo "$(now) документи в колекції lab4.exp:"
ms --eval 'db.getSiblingDB("lab4").exp.find({}, {_id:0}).forEach(function(d){ print("   ", tojson(d, "", true)) })'
echo "$(now) стан replica set наприкінці:"; show
