#!/bin/bash
# Повторний запуск сценаріїв 1 і 2 Частини II, після кожного пауза для скріншота
cd "$(dirname "$0")" && source .venv/bin/activate
run() { clear; python task4_part2.py "$@"; echo; read -r -p ">>> Зробіть скріншот результату, потім натисніть Enter... " _; }
run --w 1
run --w majority
