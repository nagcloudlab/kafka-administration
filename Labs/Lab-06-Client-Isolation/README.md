# Lab 06 — Client Isolation

Failure-mode training. Each scenario is a standalone `.md` — reproduces one client-side pathology on the Lab-01 cluster in **Problem → Solution → Demo** form.

## Scenarios (do in order)

- [6.1 — Consumer group rebalance storm](Scenario-6.1-Rebalance-Storm.md)
- [6.2 — Quotas (protecting a shared cluster from one runaway client)](Scenario-6.2-Quotas.md)

## Common prereqs

- Labs 3, 4, and 5 complete.
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

## Not covered in Lab 6

- **SASL / mTLS authentication** — required for user-level quotas; separate lab.
- **Kafka ACLs** — separate lab.
- **Multi-tenancy at the topic level** (naming conventions, RBAC) — separate lab.
- **Consumer group internals** (offset commit protocol, coordinator election) — Chapter 5 (planned).
