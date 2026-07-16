# Lab 04 — Replication & Durability

Failure-mode training. Each scenario is a standalone `.md` — reproduces one durability trade-off on the Lab-01 cluster in **Problem → Solution → Demo** form.

## Scenarios (do in order)

- [4.1 — `min.insync.replicas` + `acks` (the durability triangle)](Scenario-4.1-MinISR-Acks.md)
- [4.2 — Unclean leader election (silent data loss)](Scenario-4.2-Unclean-Election.md)
- [4.3 — Rack awareness (placing replicas across failure domains)](Scenario-4.3-Rack-Awareness.md)

## Common prereqs

- Lab 3 complete.
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

## Not covered in Lab 4

- KRaft-specific replication behaviour — separate lab.
- Cross-cluster replication (MirrorMaker 2 / Cluster Linking) — separate lab.
- Consumer-side durability (offset semantics, group commits) — Lab 6.
