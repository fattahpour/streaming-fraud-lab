#!/bin/bash
set -euo pipefail
SCENARIOS="$@"
docker compose up -d
# submit flink job
flink run -c com.fraudlab.StreamingJob flink-job/target/flink-job.jar &
FLINK_PID=$!
# wait for JM to be ready
sleep 5
# run data generator
java -jar data-gen/target/data-gen.jar $SCENARIOS &
GEN_PID=$!
wait $FLINK_PID $GEN_PID
