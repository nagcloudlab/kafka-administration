# Lab 08 — KRaft mode & Migration

The modern replacement for the ZK-based control plane. Where Lab 03 taught the operational quirks of ZK-mode brokers (stale ephemerals, session semantics), this lab shows the same problems dissolving under KRaft — and the operational procedure to migrate an existing ZK cluster over.

## Scenarios (do in order)

- [8.1 — Stand up a KRaft cluster (side-by-side with the ZK cluster)](Scenario-8.1-KRaft-Cluster-Setup.md)
- [8.2 — ZK → KRaft migration (dual-write mode + cutover)](Scenario-8.2-ZK-to-KRaft-Migration.md)

## Common prereqs

- Labs 1 and 3 done (you know how to run brokers and read their logs).
- Java 17+ (KRaft has the same JVM requirements as ZK-mode).
- **Port range clear:** 9095-9097 (KRaft controller quorum), 19092-19094 (KRaft broker client ports). We use non-standard ports so a running ZK cluster on 9092-9094 can coexist during Sc 8.2.

## Where things live

Lab-08 uses a **separate runtime folder** — `kafka-lab-kraft/` — parallel to `kafka-lab/`. The two clusters share the extracted Kafka distribution (symlinked) but have independent configs, data dirs, logs, and helper scripts.

```
kafka-lab-kraft/
├── cluster-kraft.sh            # start/stop/status helper (generated in 8.1)
├── config/
│   ├── kraft-broker-1.properties  # combined-mode nodes (broker + controller)
│   ├── kraft-broker-2.properties
│   └── kraft-broker-3.properties
├── data/                       # storage dirs (format'd once per node)
├── logs/                       # per-node logs
├── pids/
└── kafka -> ../kafka-lab/kafka # symlink to shared distribution
```

## Full KRaft-off reset

```bash
cd kafka-lab-kraft
./cluster-kraft.sh stop 2>/dev/null || true
cd ..
rm -rf kafka-lab-kraft
```

Doesn't touch the ZK cluster.

## Not covered in Lab 08

- **Snapshot / restore of the metadata log** — real Ops procedure but not part of first-time training.
- **Rolling upgrade of a KRaft cluster across metadata versions** — needs a longer runway.
- **Migration rollback** — dual-write mode is reversible, but the cutover in Sc 8.2 is one-way. Rollback is a separate lab.
- **KRaft on containers/K8s** — the Confluent CFK operator, Strimzi, MSK — each has its own KRaft opinions.
