# Lab 09 — Cross-Cluster Replication (MirrorMaker 2)

Copy data (and consumer offsets, and topic configs) from one Kafka cluster to another. The DR pattern most Ops teams inherit.

## Scenarios (do in order)

- [9.1 — MM2 active-passive replication (primary → DR)](Scenario-9.1-MM2-Active-Passive.md)
- [9.2 — Consumer failover with offset translation](Scenario-9.2-Consumer-Failover.md)

## Common prereqs

- Labs 1 and 3 done.
- Source cluster (from Lab-01) up: `./cluster.sh start-monitoring` in `kafka-lab/`.
- Ports free: **22181** (target ZK), **29092–29094** (target brokers). MM2 process reuses no dedicated port.

## Where things live

MM2 doesn't need much infrastructure — it's a Kafka Connect app under the hood. This lab uses a second, minimal cluster as the target:

```
kafka-lab-dr/
├── cluster-dr.sh              # start/stop/status helper (generated in 9.1)
├── config/
│   ├── zookeeper.properties   # ZK on 22181
│   └── broker-201.properties
│   └── broker-202.properties
│   └── broker-203.properties
├── data/                      # target cluster's data
├── logs/
├── pids/
└── kafka -> ../kafka-lab/kafka   # symlink to shared distribution

Lab-09-Cross-Cluster-Replication/
├── mm2.properties             # MirrorMaker 2 config
├── mm2.sh                     # start/stop helper for the MM2 process
└── logs/mm2.log
```

## Full reset

```bash
cd Lab-09-Cross-Cluster-Replication && ./mm2.sh stop 2>/dev/null || true
cd ~/kafka-administration
[[ -d kafka-lab-dr ]] && ( cd kafka-lab-dr && ./cluster-dr.sh stop 2>/dev/null || true ) && rm -rf kafka-lab-dr
rm -rf Lab-09-Cross-Cluster-Replication/logs
```

Doesn't touch the source cluster.

## Not covered in Lab 09

- **Active-active (two-way) replication** — same MM2 mechanism but with the loop-avoidance rules (`replication.policy.class` set to a custom no-prefix policy, or careful topic naming). Explained inline in 9.1, not demoed.
- **Confluent Cluster Linking** — Confluent-only alternative to MM2; wire-level replication rather than Connect-based. Different lab.
- **ACL / user / quota replication** — MM2 can replicate these too (`sync.topic.acls.enabled=true`) but adds complexity; skipped in scenarios but flagged in 9.1's Solution.
- **Cross-region latency tuning** — production concern (batch sizes, compression, throttles) — mentioned briefly, not scoped as a demo.
