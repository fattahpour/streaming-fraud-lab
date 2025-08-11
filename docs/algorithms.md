# Algorithms

Currently implemented detectors:

* **Velocity rule** – sliding window count of transactions per card.
* **Impossible travel** – CEP pattern detecting a user appearing in distant locations within a short period.
* **Amount z-score** – monitors rolling mean and standard deviation of amounts per user.

The architecture leaves space for additional detectors such as EWMA, CUSUM, sketches and machine learning approaches.
