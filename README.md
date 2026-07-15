# Kafka Administration Training

Hands-on labs for **Ops / SRE admins**. Each lab is a self-contained folder with a `README.md` (the walkthrough) and, for the chapter-style labs, one or more `Scenario-N.M-*.md` files that reproduce a specific failure or teach one admin concept end-to-end.

Everything runs locally on a single host (WSL2 tested). No cloud, no CI. Kafka brokers run outside Docker so log tails, `kill -9`, and JMX all work as students would see them in production.

---

## Prerequisites (host)

| Tool | Why |
|------|-----|
| Java 17+ | Kafka brokers, JMX agent |
| Docker + `docker compose` v2 | Prometheus + Grafana (Lab 02) |
| `curl`, `tar`, `wget` | downloads |
| `python3` | JSON parsing in a few scripts |
| ~4 GB free RAM, ~3 GB free disk | 3-broker cluster + Prom TSDB |

---

## Quick reference

- **[scenarios.html](scenarios.html)** — one-page summary of all 24 scenarios across 12 labs. Card per scenario with Problem / Demo / Takeaway and jump-link to the full doc. Open in a browser; `Ctrl+P` for a printable handout.

## Teaching sequence (do labs in order)

| # | Lab | What it teaches | Duration |
|---|-----|-----------------|----------|
| **01** | [Cluster Setup](Lab-01-Cluster-Setup/README.md) | Build 1 x ZK + 3 x broker + Kafka UI from the Apache tarball | 60 min |
| **02** | [Monitoring Setup](Lab-02-Monitoring-Setup/README.md) | Attach JMX exporter, bring up Prometheus + Grafana, load the training dashboards | 30 min |
| **03** | [Broker Lifecycle](Lab-03-Broker-Lifecycle/README.md) | SIGTERM vs `kill -9`, stale broker epoch, under-replicated partitions | 60–90 min |
| **04** | [Replication & Durability](Lab-04-Replication-Durability/README.md) | `min.insync.replicas` + `acks`, unclean leader election, rack awareness | 60–90 min |
| **05** | [Data Lifecycle](Lab-05-Data-Lifecycle/README.md) | Retention vs compaction, partition reassignment throttling | 45–60 min |
| **06** | [Client Isolation](Lab-06-Client-Isolation/README.md) | Consumer rebalance storms, producer/consumer quotas | 45–60 min |
| **07** | [Security](Lab-07-Security/README.md) | SASL/SCRAM, ACLs, mTLS (encryption + cert-based auth) | 90 min |
| **08** | [KRaft Migration](Lab-08-KRaft-Migration/README.md) | KRaft cluster stand-up, ZK → KRaft migration (dual-write + cutover) | 90 min |
| **09** | [Cross-Cluster Replication (MM2)](Lab-09-Cross-Cluster-Replication/README.md) | Active-passive replication, consumer failover with offset translation | 60 min |
| **10** | [Tiered Storage (KIP-405)](Lab-10-Tiered-Storage/README.md) | Local vs remote tier, segment offload, cost model | 45 min |
| **11** | [Observability at Scale](Lab-11-Observability-At-Scale/README.md) | Prometheus alerting + AlertManager, consumer lag monitoring | 60 min |

**Labs 01 and 02 are setup** — do them once. Labs 03 onwards are the failure-mode / concept scenarios, each independent enough to skip or repeat.

---

## Scenario shape (Labs 03+)

Every scenario file follows the same seven-block structure:

```
Problem   Symptom   Setup   Trigger   Observe   Solution   Verify   Takeaway   Instructor notes   Teardown
```

- **Problem / Solution** carry the *why*.
- **Setup / Trigger / Observe / Verify** are all copy-paste-runnable commands.
- **Instructor notes** are the "read this before you teach it" cues — polls, common wrong answers, bridges to the next scenario.
- Every scenario ends with a **Teardown** that leaves the cluster clean for the next one.

Scenario numbering: `Scenario-N.M` where `N` matches the parent `Lab-N` folder. So Lab-03 contains `Scenario-3.1`, `3.2`, `3.3`; Lab-06 contains `Scenario-6.1`, `6.2`.

---

## Recommended layout for teaching

- One projector with Grafana → **Kafka Admin Training** dashboard visible throughout.
- 4–6 terminal panes in a tiling window manager. Each scenario's Setup section says which panes to open.
- Trainer runs commands live; students follow on their own hosts.

---

## Repo layout

```
.
├── README.md                       (this file)
├── Lab-01-Cluster-Setup/           setup — build the cluster
├── Lab-02-Monitoring-Setup/        setup — Prom + Grafana + dashboards
│   ├── README.md
│   ├── up.sh / down.sh             stack lifecycle
│   ├── docker-compose.yml
│   ├── kafka-jmx-config.yaml       what JMX MBeans to export
│   ├── prometheus.yml.template     source of truth; up.sh renders prometheus.yml
│   └── grafana-provisioning/       auto-loaded datasource + 5 dashboards
├── Lab-03-Broker-Lifecycle/           chapter — scenarios 3.1 / 3.2 / 3.3
├── Lab-04-Replication-Durability/     chapter — scenarios 4.1 / 4.2 / 4.3
├── Lab-05-Data-Lifecycle/             chapter — scenarios 5.1 / 5.2
├── Lab-06-Client-Isolation/           chapter — scenarios 6.1 / 6.2
├── Lab-07-Security/                   chapter — scenarios 7.1 / 7.2 / 7.3
├── Lab-08-KRaft-Migration/            chapter — scenarios 8.1 / 8.2
├── Lab-09-Cross-Cluster-Replication/  chapter — scenarios 9.1 / 9.2
├── Lab-10-Tiered-Storage/             chapter — scenario  10.1
├── Lab-11-Observability-At-Scale/     chapter — scenarios 11.1 / 11.2
├── kafka-lab/                         RUNTIME dir — the cluster's binaries, configs, data, pids, logs
│   ├── cluster.sh                  start/stop/status/monitoring helper
│   ├── config/                     broker + zookeeper .properties (per-node)
│   ├── data/                       broker log dirs (grows during use)
│   ├── logs/                       broker log tails
│   ├── pids/                       one file per broker/zk pid
│   └── kafka/                      extracted Apache Kafka distribution
├── kafka-clients/                  Java producer/consumer/streams samples (Maven)
├── reference-links.txt             external reading list
└── .archive/                       superseded scratch notes (kept for history)
```

---

## Quick start for a new machine

```bash
# 1. Build the cluster (one-off, ~15 min including downloads)
open Lab-01-Cluster-Setup/README.md          # follow steps

# 2. Enable monitoring (one-off, ~5 min)
open Lab-02-Monitoring-Setup/README.md       # follow steps
# result: Grafana at http://localhost:3000, dashboards loaded

# 3. Any scenario, any time
open Lab-03-Broker-Lifecycle/Scenario-3.1-Controlled-Shutdown.md
```

---

## Cluster helper (`kafka-lab/cluster.sh`)

The runtime folder ships a small helper — use it, don't run `kafka-server-start.sh` by hand.

```bash
cd kafka-lab
./cluster.sh start                             # zk + 3 brokers (no monitoring)
./cluster.sh start-monitoring                  # same, with JMX exporter attached
./cluster.sh stop
./cluster.sh status
./cluster.sh restart-broker 102                # single broker (graceful)
./cluster.sh restart-broker-monitoring 102     # single broker, with JMX
./cluster.sh help                              # full command list
```

`start-monitoring` is what the failure-mode labs assume — start with it once you've done Lab 02.

---

## Conventions

- **Broker IDs**: `101 / 102 / 103` → Kafka ports `9092 / 9093 / 9094` → JMX exporter ports `7071 / 7072 / 7073`.
- **Scenario topics**: prefix by scenario, e.g. `a1-*` for 3.1, `c1-*` for 4.2, so they're easy to spot and clean up.
- **Teardown is mandatory** at the end of each scenario. The cluster stays live between scenarios; only topics and per-scenario configs get removed.
- **Full reset**: each chapter README documents the "full cluster reset" (stop, wipe data dirs, restart) for when things get wedged.

---

## What's not here (and why)

- **Managed cloud walkthroughs** (Confluent Cloud, MSK, ...) — this course is about the internals; managed platforms hide too much.
- **Confluent Cluster Linking** — Confluent-only alternative to MM2 (Lab 09 covers the OSS path).
- **OAuth / OIDC (SASL/OAUTHBEARER) & Kerberos** — SCRAM (Lab 07) covers the common case; OAuth needs an IdP; Kerberos needs a KDC — both are their own labs.
- **Distributed tracing across producers/consumers** — client-side (OpenTelemetry SDK), not an admin lab.
- **SLO / burn-rate multi-window alerting** — flagged as the next step at the end of Lab 11.1.
- **Programming labs** (producer client tuning, streams DSL) — `kafka-clients/` has starter Java code, but it's not part of the admin curriculum.
