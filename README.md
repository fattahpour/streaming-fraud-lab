# Streaming Fraud Lab

This repository demonstrates a tiny streaming fraud detection lab using Apache Flink 1.18, Kafka 3 and Java 17.  It shows how to run a Flink job that reads synthetic transaction data from Kafka and emits alerts.  The project is drastically simplified in comparison to the requested full implementation but provides a skeleton that can be extended.

## Quick start

```bash
# build modules
./scripts/build.sh

# start docker services and run job + generator
./scripts/run.sh --card-testing --impossible-travel

# view alerts
./scripts/tail.sh
```

## Modules

* `flink-job` – Flink streaming job containing a few rule based detectors (velocity rule, impossible travel CEP, z-score rule) and a simple risk blender that outputs alerts.
* `data-gen` – Kafka producer emitting fake transactions.  Flags can enable scenarios that trigger the implemented rules.

## Status

Only a subset of the detectors and scenarios described in the task are implemented due to time limitations.  The repository serves as a starting point for further work.
