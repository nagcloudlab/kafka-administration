# Lab 01 — Kafka Cluster Setup (1 ZooKeeper + 3 Brokers + Kafka UI)

**Duration:** ~60 minutes
**Audience:** Ops / SRE admins — first hands-on lab
**Goal:** Build a local Kafka cluster from the Apache Kafka distribution, run each node in its own terminal, and browse it with Kafka UI.

---

## 1. Objective

By the end of this lab you will:

1. Download and lay out an Apache Kafka distribution manually
2. Run a **ZooKeeper** node and **three Kafka brokers**, each in its own terminal
3. Verify the cluster with the built-in CLI tools
4. Attach **Kafka UI** (web console) and inspect the cluster in a browser
5. Produce and consume a first message

Running each node in its own terminal (instead of a background service or Docker Compose) makes the startup order, logs, and shutdown behaviour visible — which is the point of an admin lab.

---

## 2. Prerequisites

| Item | Check |
|------|-------|
| Java 17+ installed | `java -version` |
| ~2 GB free disk | `df -h .` |
| ~4 GB free RAM | `free -h` |
| `wget` or `curl` | `which wget` |
| `tar` | `which tar` |
| 6 terminal windows/tabs open | Windows Terminal split panes work great |

> **Tip:** In Windows Terminal, split panes with `Alt+Shift+D` (auto) or `Alt+Shift+-` / `Alt+Shift++`. You'll want 6 panes: **ZK**, **Broker 1**, **Broker 2**, **Broker 3**, **UI**, **Admin/CLI**.

If Java is missing on Ubuntu/WSL:

```bash
sudo apt update && sudo apt install -y openjdk-17-jdk
java -version
```

---

## 3. Pick a working directory

Everything in this lab is relative to the **current directory** — no hardcoded home paths. Pick a folder and stay there for all steps.

```bash
# Example: create a lab folder wherever you like, then cd into it
mkdir -p kafka-lab && cd kafka-lab

# Confirm your absolute path — you'll see it baked into config files later
pwd
```

From here on, `pwd` is your **lab root**. Every command in this document assumes you are in that folder unless stated otherwise.

### Target layout

After the setup steps you'll have:

```
./                             <- your lab root (pwd)
├── kafka/                     # extracted Apache Kafka distribution
│   ├── bin/                   # kafka-*.sh scripts
│   ├── config/                # shipped sample configs (we won't edit these)
│   └── libs/
├── config/                    # OUR per-node configs (edit these)
│   ├── zookeeper.properties
│   ├── broker-1.properties
│   ├── broker-2.properties
│   └── broker-3.properties
└── data/                      # data + logs (grows over time)
    ├── zookeeper/
    ├── broker-1/
    ├── broker-2/
    └── broker-3/
```

Rule of thumb: **never edit files under `kafka/config/`** — keep your configs in a separate folder so you can wipe and redeploy Kafka without losing them.

---

## 4. Step 1 — Download & extract Kafka

We're using Kafka **3.9.0** (Scala 2.13 build) — the latest 3.x line that still supports ZooKeeper. Kafka 4.x removed ZK entirely.

From your lab root:

```bash
# Download (~110 MB)
wget https://archive.apache.org/dist/kafka/3.9.0/kafka_2.13-3.9.0.tgz

# Extract into ./kafka
tar -xzf kafka_2.13-3.9.0.tgz
mv kafka_2.13-3.9.0 kafka

# Verify
ls kafka/bin | head
```

Expected: you should see `kafka-topics.sh`, `kafka-server-start.sh`, `zookeeper-server-start.sh`, etc.

Create the config and data directories:

```bash
mkdir -p ./config
mkdir -p ./data/zookeeper ./data/broker-1 ./data/broker-2 ./data/broker-3
```

---

## 5. Step 2 — Generate the ZooKeeper config

Kafka's `.properties` files do **not** expand environment variables or `~`, so `log.dirs` / `dataDir` must be **absolute paths**. We use a heredoc so `$(pwd)` is expanded once — at file creation time — and the result is baked in.

From your lab root:

```bash
cat > ./config/zookeeper.properties <<EOF
dataDir=$(pwd)/data/zookeeper
clientPort=2181
maxClientCnxns=0
admin.enableServer=false
EOF

# Sanity check — dataDir should be a real absolute path on your machine
cat ./config/zookeeper.properties
```

---

## 6. Step 3 — Generate the three broker configs

The three brokers differ only in **broker.id**, **listener port**, and **log.dirs**. Everything else is identical. Generate all three with one loop:

```bash
LAB_ROOT="$(pwd)"

for i in 101 102 103; do
  port=$((9091 + i-100))    # 9092, 9093, 9094
  cat > "./config/broker-${i}.properties" <<EOF
broker.id=${i}
listeners=PLAINTEXT://:${port}
advertised.listeners=PLAINTEXT://localhost:${port}

log.dirs=${LAB_ROOT}/data/broker-${i}
zookeeper.connect=localhost:2181
EOF
done

# Sanity check
grep -E '^(broker.id|listeners|log.dirs)' ./config/broker-*.properties
```

Expected output (paths will match your `pwd`):

```
./config/broker-1.properties:broker.id=1
./config/broker-1.properties:listeners=PLAINTEXT://:9092
./config/broker-1.properties:log.dirs=/.../kafka-lab/data/broker-1
./config/broker-2.properties:broker.id=2
./config/broker-2.properties:listeners=PLAINTEXT://:9093
./config/broker-2.properties:log.dirs=/.../kafka-lab/data/broker-2
./config/broker-3.properties:broker.id=3
./config/broker-3.properties:listeners=PLAINTEXT://:9094
./config/broker-3.properties:log.dirs=/.../kafka-lab/data/broker-3
```

> **Discussion point:** Why does each broker need a unique `broker.id`, port, and `log.dirs`? What would happen if two brokers shared a `log.dirs`? (Answer: file locks — the second broker would refuse to start.)

---

## 7. Step 4 — Start the cluster (6 terminals)

Open **six terminal panes**, each in the same lab root directory (`cd` there in every pane). Label them mentally: **ZK**, **B1**, **B2**, **B3**, **UI**, **Admin**.

(Terminals 1–4 come up now. **UI** and **Admin** come into play in Steps 6 and 7.)

### Terminal 1 — ZooKeeper

```bash
./kafka/bin/zookeeper-server-start.sh ./config/zookeeper.properties
```

Wait for a log line like:

```
INFO binding to port 0.0.0.0/0.0.0.0:2181 (org.apache.zookeeper.server.NIOServerCnxnFactory)
```

**Leave this terminal running.** Do not close it.

### Terminal 2 — Broker 1

```bash
./kafka/bin/kafka-server-start.sh ./config/broker-101.properties
```

Wait for:

```
INFO [KafkaServer id=1] started (kafka.server.KafkaServer)
```

### Terminal 3 — Broker 2

```bash
./kafka/bin/kafka-server-start.sh ./config/broker-102.properties
```

### Terminal 4 — Broker 3

```bash
./kafka/bin/kafka-server-start.sh ./config/broker-103.properties
```

You should see each broker's log announce it has joined the cluster and elected/followed for internal partitions.

---

## 8. Step 5 — Verify the cluster (Terminal 6, the Admin pane)

```bash
# 1. Ask the cluster who's alive
./kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 | head -20

# 2. Create a demo topic with 3 partitions, RF=3
./kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic demo --partitions 3 --replication-factor 3

# 3. Describe it
./kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic demo
```

Expected output (leader/replica IDs may vary):

```
Topic: demo   TopicId: xxxxxxxx   PartitionCount: 3   ReplicationFactor: 3
    Topic: demo   Partition: 0   Leader: 1   Replicas: 1,2,3   Isr: 1,2,3
    Topic: demo   Partition: 1   Leader: 2   Replicas: 2,3,1   Isr: 2,3,1
    Topic: demo   Partition: 2   Leader: 3   Replicas: 3,1,2   Isr: 3,1,2
```

**What to point out to trainees:**

- **PartitionCount = 3**, **ReplicationFactor = 3** — as configured
- **Replicas** — the assigned broker IDs (Kafka spread them across all 3 brokers)
- **ISR** (in-sync replicas) — currently equal to Replicas → cluster is healthy
- **Leader** — one broker per partition handles all reads/writes; leadership is roughly balanced across brokers 1, 2, 3

---

## 9. Step 6 — Kafka UI (Terminal 5, the UI pane)

We'll use **Kafbat UI** — a free web console for Kafka (community-maintained fork of the original Provectus Kafka UI, now the actively-developed one). We run it as a plain Java process on the host, not in Docker — same JVM you already have for Kafka, one fat JAR, and no container networking gotchas.

### 9.1 Download the JAR (one-time)

We pin **Kafbat UI v1.0.0** because it's the last release compiled for **Java 17** — the same JDK that runs Kafka. Every release from v1.1.0 onward requires Java 21+, and v1.5.0 requires Java 25. Pinning keeps trainees on one JDK.

From your lab root:

```bash
# Download the v1.0.0 fat JAR (~93 MB)
curl -L -o kafka-ui.jar \
  https://github.com/kafbat/kafka-ui/releases/download/v1.0.0/kafbat-ui-v1.0.0.jar

ls -lh kafka-ui.jar
```

> The v1.0.0 asset is named `kafbat-ui-v1.0.0.jar` (later releases use `api-vX.Y.Z.jar`). If the download stalls, grab it manually from
> <https://github.com/kafbat/kafka-ui/releases/tag/v1.0.0> and save it as `kafka-ui.jar` in the lab root.
>
> **Want a newer UI?** Install `openjdk-25-jre-headless` and run the latest release under Java 25 via an explicit path (`/usr/lib/jvm/java-25-openjdk-amd64/bin/java -jar kafka-ui.jar`), keeping Java 17 as the default for Kafka.

### 9.2 Run it (Terminal 5)

```bash
KAFKA_CLUSTERS_0_NAME=local \
KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=localhost:9092,localhost:9093,localhost:9094 \
java -Xmx512m -jar kafka-ui.jar
```

Wait for a log line like:

```
Started KafkaUiApplication in X seconds
```

**Leave this terminal running.** `Ctrl+C` to stop when you're done.

### 9.3 Open the console

Open **http://localhost:8080** in your browser.

You should see:

- **Brokers** page → three brokers (IDs 1, 2, 3), each `Online`
- **Topics** page → `demo` topic (plus internal `__consumer_offsets` once you produce/consume)
- Click a topic → see partitions, replicas, ISR, offsets — same info as `--describe`, visually

> **Why the JAR and not Docker?** Kafka UI in a container can't reach `localhost:9092` on your host (inside the container, `localhost` is the container itself). Fixing that needs either `--network host` (Linux only, doesn't work with Docker Desktop on Mac/Windows/WSL) or a dual-listener broker setup with `host.docker.internal`. Running the JAR on the host skips both problems.

---

## 10. Step 7 — First produce & consume

Split the Admin pane, or use two more panes. (Each must be in the lab root.)

**Producer pane:**

```bash
./kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo
> hello
> kafka
> from-terminal-1
```

**Consumer pane** — notice we bootstrap off a *different* broker to prove the cluster works:

```bash
./kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9093 \
  --topic demo --from-beginning
```

Expected: the three messages appear in the consumer pane (order across partitions is not guaranteed, order within a partition is).

Switch to **Kafka UI → Topics → demo → Messages** and confirm the same messages appear there.

---

## 11. Sanity checks & "gotchas"

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `Address already in use` on broker startup | Port 9092/9093/9094 or 2181 in use | `ss -ltnp \| grep -E '2181\|909[234]'` and kill / change port |
| `NodeExists` errors from ZK on broker startup | Old ZK data left over from a prior lab | Stop everything, `rm -rf ./data/*`, recreate subfolders, restart |
| Broker exits with `InconsistentClusterIdException` | Broker data dir from a different cluster | Wipe that broker's `data/broker-N/` and restart |
| Kafka UI won't start — `Port 8080 already in use` | Something else has 8080 (Grafana, another Spring app…) | Run with `SERVER_PORT=9090 java -jar kafka-ui.jar` and browse to `:9090` |
| Kafka UI shows "Offline" for brokers | Brokers aren't listening on `localhost:9092/9093/9094`, or you set `KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS` to the wrong host/port | `ss -ltnp \| grep 909` to confirm brokers are up; verify env var matches |
| `curl` for the JAR returns HTML instead of a binary | Bad URL / GitHub redirect not followed | Ensure you used `curl -L` and the exact filename `kafbat-ui-v1.0.0.jar` |
| Kafka UI crashes with `UnsupportedClassVersionError ... class file version 6X` | You downloaded a newer Kafbat release (v1.1.0+) — needs Java 21/25 | Re-download **v1.0.0** (Java 17), or install `openjdk-25-jre-headless` and run the newer JAR under Java 25 |
| `LEADER_NOT_AVAILABLE` on first produce | Topic was just created; metadata still propagating | Retry after 1–2 seconds |
| Consumer sees no messages | Missing `--from-beginning` (defaults to latest) | Add `--from-beginning` |
| Config file paths look wrong | You ran the heredoc in a different directory than `pwd` now | Regenerate the configs from the current lab root |

---

## 12. Shutting down cleanly

Order matters: **UI first, then brokers, then ZooKeeper**.

1. In the **UI terminal** (5): press `Ctrl+C`.
2. In each **broker terminal** (2, 3, 4): press `Ctrl+C`, wait for `[KafkaServer id=N] shut down completed`.
3. In the **ZooKeeper terminal** (1): press `Ctrl+C`.

To reset the cluster to a blank slate for the next lab (from the lab root):

```bash
rm -rf ./data/*
mkdir -p ./data/zookeeper ./data/broker-1 ./data/broker-2 ./data/broker-3
```

---

## 13. What we didn't cover (yet)

Deliberately out of scope for this first lab:

- KRaft mode (no ZooKeeper) — later lab
- TLS / SASL — later lab
- Reassignment, tuning, JMX metrics — later labs
- Docker Compose orchestration — we kept things manual on purpose so startup / logs / shutdown are visible

---

## 14. Instructor talking points

- Emphasise **startup order**: ZK must be up before brokers; brokers must be up before Kafka UI can connect.
- Draw the **broker ↔ ZK relationship** on the board while ZK's terminal shows client connection logs.
- After `--describe`, ask trainees: *"What happens to partition 0 (Leader=1) if I `Ctrl+C` broker 1?"* — then do it in the next lab.
- Point out that **all admin commands take `--bootstrap-server`, not `--zookeeper`**. The old `--zookeeper` flag was removed years ago; brokers are the source of truth now.
- Kafka UI is convenient but **the CLI is authoritative** — every UI action maps to an admin API call, and in a production incident you'll be on the CLI, not clicking around.
