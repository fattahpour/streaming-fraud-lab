#!/bin/bash
set -euo pipefail
mvn -q -e -pl flink-job,data-gen -am package
