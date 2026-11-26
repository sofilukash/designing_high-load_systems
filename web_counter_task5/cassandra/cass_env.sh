# Допоміжні функції для роботи з кластером Cassandra (cass1..cass3).
# cq "<CQL>" виконує CQL на cass1; NODE=2 cq "<CQL>" – на cass2.
# nt <команда> виконує nodetool на вибраній ноді.
cq() { docker exec -i "cass${NODE:-1}" cqlsh -e "$1"; }
nt() { docker exec "cass${NODE:-1}" nodetool "$@"; }
# try_cl <keyspace> <CL> <id>: запис і читання рядка з вказаним рівнем consistency (через cass1)
try_cl() {
  echo "== $1, CL=$2, id=$3"
  cq "CONSISTENCY $2; INSERT INTO $1.kv (id, val) VALUES ($3, 'written at $2');"
  cq "CONSISTENCY $2; SELECT * FROM $1.kv WHERE id = $3;"
}
