#!/bin/bash
set -euo pipefail
docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic alerts --from-beginning
