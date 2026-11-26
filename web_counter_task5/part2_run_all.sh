#!/bin/bash
# Частина II: усі чотири сценарії по черзі, після кожного пауза для скріншота
cd "$(dirname "$0")" && source .venv/bin/activate
run() {
  clear
  python task4_part2.py "$@"
  echo
  read -r -p ">>> Зробіть скріншот результату, потім натисніть Enter... " _
}
run --w 1
run --w majority
run --w 1 --kill-primary-after 8
run --w majority --kill-primary-after 8
