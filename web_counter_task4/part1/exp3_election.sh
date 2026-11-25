#!/bin/bash
# Частина I, експеримент 3: вибори при вимкненні primary + реплікація на повернену ноду
cd "$(dirname "$0")/.." && source ./mongo_env.sh
now() { date +%T; }
show() { ms --eval 'rs.status().members.forEach(function(m){ print("   ", m.name, m.stateStr) })'; }
read_majority() { msd $1 --eval 'rs.secondaryOk(); db.getSiblingDB("lab4").exp.find({}, {_id:0}).readConcern("majority").forEach(function(d){ print("   ", tojson(d, "", true)) })'; }

ms --eval 'db.getSiblingDB("lab4").dropDatabase()' >/dev/null
echo "=== Експеримент 3: вимкнення primary, вибори, повернення старого primary ==="
echo "$(now) стан replica set перед експериментом:"; show

PP=$(ms --eval 'print(db.isMaster().primary.split(":")[1])')
OLD=mongo$((PP-27016))
for i in 1 2 3; do [ "mongo$i" != "$OLD" ] && { S1=mongo$i; break; }; done
S1PORT=$((27016 + ${S1#mongo}))
trap 'docker start '$OLD' >/dev/null 2>&1' INT TERM

echo "$(now) поточний PRIMARY: 127.0.0.1:$PP (контейнер $OLD)"
echo "$(now) записуємо документ до відмови (w:majority):"
ms --eval 'printjson(db.getSiblingDB("lab4").exp.insertOne({exp:"before-failover"}, {writeConcern:{w:"majority"}}))'

echo "$(now) ВИМИКАЄМО primary: docker kill $OLD (аварійна зупинка процесу)"
docker kill $OLD >/dev/null
TK=$(date +%s)
while true; do
  NP=$(docker exec $S1 mongo --port $S1PORT --quiet --eval 'print(db.isMaster().primary || "-")' 2>/dev/null)
  EL=$(( $(date +%s) - TK ))
  if [ -n "$NP" ] && [ "$NP" != "-" ] && [ "$NP" != "127.0.0.1:$PP" ]; then
    echo "$(now) +${EL} с: обрано новий PRIMARY = $NP"; break
  elif [ "$NP" = "-" ]; then echo "$(now) +${EL} с: primary немає, тривають вибори"
  else echo "$(now) +${EL} с: ноди ще чекають heartbeat-таймаут (10 с) і вважають primary старий"; fi
  sleep 1
done

echo "$(now) пишемо новий документ, поки стара нода вимкнена (w:majority):"
ms --eval 'printjson(db.getSiblingDB("lab4").exp.insertOne({exp:"during-failover"}, {writeConcern:{w:"majority", wtimeout:10000}}))'
echo "$(now) стан replica set зараз:"; show

echo "$(now) запускаємо стару ноду $OLD"
docker start $OLD >/dev/null
until [ "$(ms --eval 'print(rs.status().members.filter(function(m){return m.stateStr=="PRIMARY"||m.stateStr=="SECONDARY"}).length)')" = "3" ]; do sleep 1; done
for i in $(seq 1 30); do
  n=$(msd $PP --eval 'rs.secondaryOk(); print(db.getSiblingDB("lab4").exp.count())' 2>/dev/null); [ "$n" = "2" ] && break; sleep 1
done
echo "$(now) документи на кожній ноді (readConcern \"majority\"):"
for P in 27017 27018 27019; do
  tag=""; [ "$P" = "$PP" ] && tag="  <- колишній PRIMARY"
  echo "  нода $P$tag:"; read_majority $P
done
echo "$(now) стан replica set наприкінці:"; show
