#!/bin/bash
# Частина I, експеримент 2: запис з w:3 і скінченним wtimeout при вимкненій ноді
cd "$(dirname "$0")/.." && source ./mongo_env.sh
now() { date +%T; }
trap 'docker start mongo3 >/dev/null 2>&1' INT TERM
show() { ms --eval 'rs.status().members.forEach(function(m){ print("   ", m.name, m.stateStr) })'; }
read_majority() { msd $1 --eval 'rs.secondaryOk(); db.getSiblingDB("lab4").exp.find({}, {_id:0}).readConcern("majority").forEach(function(d){ print("   ", tojson(d, "", true)) })'; }

ms --eval 'db.getSiblingDB("lab4").dropDatabase()' >/dev/null
echo "=== Експеримент 2: writeConcern {w:3, wtimeout:5000}, одна нода вимкнена ==="
echo "$(now) стан replica set перед експериментом:"; show
echo "$(now) вимикаємо mongo3"
docker stop mongo3 >/dev/null

echo "$(now) insertOne з writeConcern {w:3, wtimeout:5000}:"
ms --eval '
var t0 = new Date();
try {
  printjson(db.getSiblingDB("lab4").exp.insertOne({exp:"w3-timeout"}, {writeConcern:{w:3, wtimeout:5000}}));
} catch (e) {
  print("    ПОМИЛКА: code=" + e.code + ", " + e.errmsg + ", wtimeout=" + (e.errInfo && e.errInfo.wtimeout));
}
print("    тривалість виклику, мс: " + (new Date() - t0));'

echo "$(now) клієнт отримав помилку, але чи записався документ? Читаємо з readConcern \"majority\":"
echo "  нода 27017 (PRIMARY):";   read_majority 27017
echo "  нода 27018 (SECONDARY):"; read_majority 27018
echo "$(now) стан replica set зараз:"; show

echo "$(now) запускаємо mongo3"
docker start mongo3 >/dev/null
until [ "$(ms --eval 'print(rs.status().members.filter(function(m){return m.stateStr=="PRIMARY"||m.stateStr=="SECONDARY"}).length)')" = "3" ]; do sleep 1; done
for i in $(seq 1 30); do
  n=$(msd 27019 --eval 'rs.secondaryOk(); print(db.getSiblingDB("lab4").exp.count())' 2>/dev/null); [ "$n" = "1" ] && break; sleep 1
done
echo "$(now) нода 27019 (SECONDARY) після повернення, readConcern \"majority\":"; read_majority 27019
echo "$(now) стан replica set наприкінці:"; show
