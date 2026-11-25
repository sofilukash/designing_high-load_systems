RS="mongodb://127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019/?replicaSet=rs0"
# ms  - клієнт до всього replica set (драйвер сам знаходить primary)
ms()  { docker run --rm --network host --entrypoint mongo mongo:4.4 --quiet "$RS" "$@"; }
# msd - підключення напряму до однієї ноди, наприклад: msd 27018 --eval '...'
msd() { local p=$1; shift; docker run --rm --network host --entrypoint mongo mongo:4.4 --quiet "mongodb://127.0.0.1:$p/" "$@"; }
