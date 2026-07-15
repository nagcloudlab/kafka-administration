# Lab 05 — Data Lifecycle

Failure-mode training. Each scenario is a standalone `.md` — reproduces one storage / movement behaviour on the Lab-01 cluster in **Problem → Solution → Demo** form.

## Scenarios (do in order)

- [5.1 — Retention vs compaction (what stays on disk, and when)](Scenario-5.1-Retention-vs-Compaction.md)
- [5.2 — Partition reassignment without saturating the network](Scenario-5.2-Partition-Reassignment.md)

## Common prereqs

- Labs 3 and 4 complete.
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

## Not covered in Lab 5

- **Tiered storage (KIP-405)** — offloading older segments to object storage. Separate lab.
- **Log cleaner internals** — dirty ratio, worker threads, memory sizing. Covered briefly in 3.1 solution.
- **Cross-cluster replication** (MirrorMaker 2, Cluster Linking) — separate lab.
