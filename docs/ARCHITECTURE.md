# Architecture

```
Kafka(txns) -> Flink job -> Kafka(alerts)
```

Synthetic transactions are written to the `txns` topic.  The Flink job reads these events, applies several detectors and publishes resulting `Alert` objects to the `alerts` topic.
