#!/usr/bin/env bash
# Піднімає кластер Cassandra 5.0 із 3 нод у docker-мережі cassnet (фіксовані IP).
IMAGE=cassandra:5.0
NET=cassnet
SEED=172.30.0.11

docker network inspect "$NET" >/dev/null 2>&1 || docker network create --subnet 172.30.0.0/24 "$NET"

start_node() {   # $1 = номер ноди (1..3)
  if docker inspect "cass$1" >/dev/null 2>&1; then docker start "cass$1"; return; fi
  docker run -d --name "cass$1" --hostname "cass$1" --network "$NET" --ip "172.30.0.1$1" \
    -e MAX_HEAP_SIZE=512M -e HEAP_NEWSIZE=128M \
    -e CASSANDRA_CLUSTER_NAME=lab5 -e CASSANDRA_SEEDS="$SEED" \
    --entrypoint bash "$IMAGE" -c \
    'sed -i "s/^materialized_views_enabled:.*/materialized_views_enabled: true/" "$CASSANDRA_CONF/cassandra.yaml"; exec docker-entrypoint.sh cassandra -f'
}

wait_up() {      # $1 = скільки нод мають бути у стані UN (макс. 6 хв)
  for i in $(seq 1 72); do
    [ "$(docker exec cass1 nodetool status 2>/dev/null | grep -c '^UN')" -ge "$1" ] && return 0
    sleep 5
  done
  echo "Таймаут очікування: нод у стані UN менше $1"; docker logs --tail 20 "cass$1"; exit 1
}

for n in 1 2 3; do
  echo "== Запуск cass$n"
  start_node "$n"
  wait_up "$n"
  echo "== cass$n у кластері"
done
