#!/bin/bash
set -euo pipefail
pkill -f flink-job.jar || true
pkill -f data-gen.jar || true
docker compose down
