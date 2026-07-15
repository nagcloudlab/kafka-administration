# Lab 03 — Broker Lifecycle & Availability

Failure-mode training. Each scenario is a standalone `.md` — reproduces one real incident on the Lab-01 cluster in **Problem → Solution → Demo** form.

## Scenarios (do in order)

- [3.1 — Controlled shutdown vs `kill -9`](Scenario-3.1-Controlled-Shutdown.md)
- [3.2 — Stale broker epoch (the incident, on purpose)](Scenario-3.2-Stale-Broker-Epoch.md)
- [3.3 — Under-replicated partitions (URP is not a page)](Scenario-3.3-Under-Replicated-Partitions.md)

## Common prereqs (used by every scenario)

- Lab-01 done.
- 3-broker ZK cluster up: `./cluster.sh start` in `kafka-lab/`.

Paste at the top of every new terminal:

```bash
cd ~/kafka-administration/kafka-lab
export KAFKA=./kafka/bin
export BS=localhost:9092,localhost:9093,localhost:9094
export ZK=localhost:2181
```

## Full cluster reset (only if a scenario left it weird)

```bash
./cluster.sh stop
rm -rf ./data/*
mkdir -p ./data/zookeeper ./data/broker-101 ./data/broker-102 ./data/broker-103
./cluster.sh start
```

## Not covered in Lab 3

- **KRaft controllers** — same graceful/ungraceful distinction, different fencing. Separate lab.
- **Rolling upgrade orchestration** — separate lab (uses concepts from 1.1–1.3).
- **`min.insync.replicas` / durability triangle** — Lab 4.1.
- **Client-side retry tuning** (`retries`, `retry.backoff.ms`, `delivery.timeout.ms`) — Lab 4.
