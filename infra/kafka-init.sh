#!/bin/bash
set -euo pipefail
kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic txns --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic alerts --partitions 1 --replication-factor 1
