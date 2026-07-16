# Kafka Administrator Native Hands-on Curriculum

## Training design

- Ten incremental workshops.
- Native Linux/WSL processes; no Docker.
- One local `./lab.sh` entry point in every workshop.
- Real Apache Kafka 3.9.1 tools shown behind every helper.
- Repeatable proof records instead of subjective “it looks okay.”
- Every operational workshop follows:
  - predict;
  - establish baseline;
  - change or inject failure;
  - collect independent evidence;
  - recover;
  - prove baseline restored;
  - explain production differences.

## Workshop sequence

1. [Build, prove, break, and recover the cluster](workshop1/README.md)
2. [Consumer groups, lag, rebalancing, and replay](workshop2/README.md)
3. [Topic lifecycle, retention, compaction, and quotas](workshop3/README.md)
4. [Capacity addition and partition reassignment](workshop4/README.md)
5. [Monitoring, capacity, and incident diagnosis](workshop5/README.md)
6. [Rolling maintenance, upgrades, backup, and DR](workshop6/README.md)
7. [MirrorMaker 2 replication and failover](workshop7/README.md)
8. [JMX Exporter, Prometheus, Grafana, and alerts](workshop8/README.md)
9. [KRaft controller and broker administration](workshop9/README.md)
10. [TLS, SASL, ACLs, and rotation](workshop10/README.md)

## Course preparation

```bash
cd /path/to/kafka-admin
./workshop1/lab.sh setup
./workshop8/lab.sh setup-observability
./workshop10/lab.sh setup-security
./workshop1/lab.sh preflight all
```

- Kafka distribution: `common/kafka_2.13-3.9.1`.
- Shared command engine: `common/native-lab.sh`.
- Runtime data: `common/data`.
- Generated plans/backups: `common/work` and `common/backups`.
- Workshop-local scripts are wrappers; participants always run `./lab.sh` from the current workshop.

## How `lab.sh` relates to the original tools

- Every workshop-local `lab.sh` contains only a thin handoff to `common/native-lab.sh`.
- `common/native-lab.sh` calls the original executables from `common/kafka_2.13-3.9.1/bin/`.
- The helper adds:
  - consistent paths and bootstrap lists;
  - input validation;
  - destructive confirmation words;
  - repeatable proof records;
  - readable health output.
- The helper does not replace Kafka administration APIs or invent simulated results.
- Every workshop README includes an **Original tools used by `lab.sh`** section.
- Example trace:

```text
./lab.sh describe orders
  -> common/native-lab.sh describe orders
  -> kafka-topics.sh --bootstrap-server BROKERS --describe --topic orders
```

- Participants may inspect both layers at any time:

```bash
sed -n '1,20p' workshop1/lab.sh
less common/native-lab.sh
```

## Delivery recommendations

- Use a large font and named terminal tabs.
- Keep the administration terminal visually separate from server logs.
- Ask for a prediction before each failure.
- Never hide an error; use it as diagnosis evidence.
- Use `proof-001`, `proof-002`, … records so replication/replay is undeniable.
- Pause after every recovery until `health` is clean.
- Repeat production limitations:
  - one laptop is one failure domain;
  - plaintext workshops are not production security;
  - combined KRaft target nodes are for resource-efficient DR training;
  - replication is not backup;
  - asynchronous replication does not guarantee zero RPO.

## Common proof commands

```bash
./lab.sh status
./lab.sh health
./lab.sh create-topic orders 6
./lab.sh produce-sequence orders 10
./lab.sh consume-count orders 10
./lab.sh offsets source orders
```

## Reset policy

- Do not reset between Workshops 1–6 unless a clean rebuild is intentional.
- Workshop 7 target cleanup: `./lab.sh reset-mm-target`.
- Workshop 9 KRaft cleanup: `./lab.sh reset-kraft`.
- Full destructive reset: stop all processes, then `./lab.sh reset`.
- Security assets are training-only and can be regenerated with `setup-security`.

## Training-day readiness checklist

- Hardware:
  - 64-bit Linux or WSL2;
  - Java 17;
  - at least 8 GB available RAM; 12 GB is more comfortable for MirrorMaker;
  - at least 3 GB free disk before participants generate data;
  - `bash`, `curl`, `tar`, `openssl`, `keytool`, `timeout`, `ss`, and `jps` available.
- Files:
  - keep the complete directory structure unchanged;
  - confirm `common/kafka_2.13-3.9.1` exists;
  - confirm `common/observability` exists before Workshop 8;
  - keep every `lab.sh` executable after copying or extracting the archive.
- Readiness commands:

```bash
chmod +x common/native-lab.sh workshop*/lab.sh
./workshop1/lab.sh validate-files
./workshop1/lab.sh preflight all
```

- Browser checks for Workshop 8:
  - participant browser must reach the same machine running the lab;
  - Prometheus uses `127.0.0.1:9090`;
  - Grafana uses `127.0.0.1:3000`;
  - default `admin/admin` is training-only.
- Delivery safeguards:
  - stop unrelated Kafka, ZooKeeper, Prometheus, and Grafana processes;
  - disable laptop sleep during live demos;
  - download everything before class and avoid depending on venue internet;
  - keep a clean copy of the workshop directory for quick recovery;
  - do not run Workshop 7 target, Workshop 8 monitoring, Workshop 9 KRaft, and Workshop 10 secured brokers simultaneously;
  - run the workshop-specific preflight before each topology change.
- Participant expectations:
  - these labs bind to localhost and are intended for one participant per machine;
  - one machine simulates logical nodes but not physical failure domains;
  - visible credentials and plaintext listeners are training assets only;
  - Kafka 3.9 is used because it is the final ZooKeeper-capable release line.
