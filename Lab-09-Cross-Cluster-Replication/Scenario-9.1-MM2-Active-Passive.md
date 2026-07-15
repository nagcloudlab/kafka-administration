# Scenario 9.1 — MM2 active-passive replication (primary → DR)

**Prereqs**
- Labs 1 and 3 done.
- Source cluster up on 9092–9094: `./cluster.sh start-monitoring` in `kafka-lab/`.

**Paste at the top of every new terminal:**

```bash
cd ~/kafka-administration
export KAFKA=./kafka-lab/kafka/bin
export SRC=localhost:9092,localhost:9093,localhost:9094
export DR=localhost:29092,localhost:29093,localhost:29094
```

---

## Problem

- **The DR pattern:** one cluster is "primary" (all reads + writes), a second cluster is "DR" (read-only mirror). If primary dies, consumers fail over to DR. This scenario builds that mirror.

- **What MirrorMaker 2 (MM2) does:**
  - Runs as a **Kafka Connect** app. Under the hood it's four connectors:
    - **MirrorSourceConnector** — reads from source topics, writes to target topics.
    - **MirrorCheckpointConnector** — periodically translates source `__consumer_offsets` into target-compatible offsets (stored in a special `<source>.checkpoints.internal` topic on target).
    - **MirrorHeartbeatConnector** — writes a heartbeat to a `<source>.heartbeats` topic so consumers can measure lag.
    - **MirrorOffsetSyncConnector** — records source ↔ target offset mappings in `<source>.offset-syncs.internal`.

- **Topic naming — the loop-avoidance rule:**
  - **Default DefaultReplicationPolicy** — target topics are prefixed with `<source-alias>.` (e.g. `primary.orders`). This is what stops active-active from looping (an event replicated back would appear as `dr.primary.orders`, mismatching the topic filter).
  - **IdentityReplicationPolicy** — no prefix; requires careful two-way filters. Simpler for active-passive; a foot-gun for active-active.

- **Consumer offsets need TRANSLATION.**
  - Records may be written to different physical offsets on source vs target (batch boundaries differ, in-flight state differs).
  - MM2 records source-offset → target-offset mappings and periodically **checkpoints** each source consumer group to translated offsets on the target cluster.
  - When a consumer fails over, it starts on target at the checkpointed translated offset — not the raw source offset.
  - Sc 9.2 demonstrates this end-to-end.

- **What MM2 does NOT do:**
  - **Not synchronous.** Target lags source by seconds to minutes depending on network and MM2 tasks. A source failure with acked-but-not-yet-replicated messages loses those messages.
  - **Not exactly-once by default.** Records may be duplicated on target after MM2 restarts (unless configured for EOS with idempotent + transactional-id, which is fiddly).
  - **Not automatic failover.** Something else — a script, a monitoring system, a human — decides when consumers switch clusters.
  - **Not schema-registry aware.** Schemas must be replicated separately (Schema Registry replication is a separate concern).

- **Deployment topologies for MM2:**
  - **Dedicated MM2 process** (`connect-mirror-maker.sh`) — one JVM manages all four connectors + its own Connect worker. Easiest to demo (this scenario).
  - **On top of an existing Kafka Connect cluster** — run MM2 connectors as regular Connect connectors. Better for production (Connect cluster is monitorable, scalable).
  - **Kubernetes operators** — Strimzi ships a `KafkaMirrorMaker2` CRD.

- **Common misconfigurations:**
  - Forgetting to raise `checkpoints.topic.replication.factor` / `heartbeats.topic.replication.factor` / `offset-syncs.topic.replication.factor` from 3 (default). If target has < 3 brokers, MM2 fails to create internal topics silently — you notice hours later when checkpoints don't appear.
  - `topics=.*` on the source picks up MM2's own internal topics on target if you also mirror in the other direction. Excludes: `topics.exclude=.*[\-\.]internal, __.*, .*\.replica`.
  - Using `IdentityReplicationPolicy` (no prefix) then enabling replication in both directions — infinite loop. Use only for strict active-passive.

## Symptom

Setup scenario — the "failure modes" are what to check IF replication doesn't start:

- MM2 log: `Connector MirrorSourceConnector_primary->dr already exists` — connector state leaked from a prior run; clean the Connect internal topics.
- MM2 log: `Insufficient replicas ... primary.checkpoints.internal` — target has fewer brokers than the topic RF; lower the RF or add brokers.
- Target has topic `primary.orders` but it's empty even after producing to source `orders` — check MM2 log for `Failed to fetch metadata`, usually a source connection issue.
- Offsets don't translate — either the connector isn't running (`MirrorCheckpointConnector` missing) or the group hasn't committed to source since MM2 started.

## Setup — 5 terminals

**Terminal 1 (control):**

```bash
cd ~/kafka-administration
# We'll build the target cluster here, then MM2 config.
```

**Terminal 2 (source topic view — leave running):**

```bash
watch -n 2 "$KAFKA/kafka-topics.sh --bootstrap-server $SRC --list | grep -v '^__'"
```

**Terminal 3 (target topic view — leave running):**

```bash
# Empty until Step 2 brings the target cluster up
watch -n 2 "$KAFKA/kafka-topics.sh --bootstrap-server $DR --list 2>/dev/null | grep -v '^__' || echo 'target cluster not up yet'"
```

**Terminal 4 (MM2 log tail — leave empty until Step 4):**

```bash
# tail -F Lab-09-Cross-Cluster-Replication/logs/mm2.log
```

**Terminal 5 (producer to source — used in Step 5):**

```bash
# We'll open a producer here later
```

## Trigger — Step 1: build the DR (target) cluster

Paste in Terminal 1:

```bash
mkdir -p kafka-lab-dr/{config,data/{zookeeper,broker-201,broker-202,broker-203},logs,pids}
cd kafka-lab-dr
ln -sf ../kafka-lab/kafka kafka        # share the distribution
LAB_DR="$(pwd)"

# ZK on 22181
cat > config/zookeeper.properties <<EOF
dataDir=$LAB_DR/data/zookeeper
clientPort=22181
maxClientCnxns=0
admin.enableServer=false
EOF

# Three brokers on 29092/29093/29094
for id in 201 202 203; do
  port=$((28891 + id))       # 201→29092, 202→29093, 203→29094
  cat > config/broker-$id.properties <<EOF
broker.id=$id
listeners=PLAINTEXT://:$port
advertised.listeners=PLAINTEXT://localhost:$port
log.dirs=$LAB_DR/data/broker-$id
zookeeper.connect=localhost:22181

# small-cluster defaults
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
EOF
done

# Tiny helper — start/stop/status
cat > cluster-dr.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
LAB="$(cd "$(dirname "$0")" && pwd)"
CONF="$LAB/config"; LOG="$LAB/logs"; PID="$LAB/pids"; KAFKA="$LAB/kafka/bin"
IDS=(201 202 203)
mkdir -p "$LOG" "$PID"
is_alive() { [[ -f "$1" ]] && kill -0 "$(cat "$1")" 2>/dev/null; }
start_zk() {
  local pf="$PID/zookeeper.pid"
  is_alive "$pf" && { echo "zk (dr) already running"; return; }
  echo "starting zk (dr) on 22181..."
  nohup "$KAFKA/zookeeper-server-start.sh" "$CONF/zookeeper.properties" > "$LOG/zk.log" 2>&1 &
  echo $! > "$pf"; sleep 3
  echo "zk (dr) up (pid $(cat "$pf"))"
}
start_broker() {
  local id="$1" pf="$PID/broker-$id.pid"
  is_alive "$pf" && { echo "broker-$id already running"; return; }
  echo "starting broker-$id..."
  nohup "$KAFKA/kafka-server-start.sh" "$CONF/broker-$id.properties" > "$LOG/broker-$id.log" 2>&1 &
  echo $! > "$pf"; sleep 3
  echo "broker-$id up (pid $(cat "$pf"))"
}
stop_pf() {
  local pf="$1" name="$2"
  is_alive "$pf" || { echo "$name not running"; rm -f "$pf"; return; }
  kill "$(cat "$pf")"; for _ in $(seq 1 30); do is_alive "$pf" || break; sleep 1; done
  is_alive "$pf" && kill -9 "$(cat "$pf")" 2>/dev/null || true; rm -f "$pf"
  echo "$name stopped"
}
case "${1:-}" in
  start)   start_zk; for id in "${IDS[@]}"; do start_broker "$id"; done ;;
  stop)    for id in "${IDS[@]}"; do stop_pf "$PID/broker-$id.pid" "broker-$id"; done; stop_pf "$PID/zookeeper.pid" "zk (dr)" ;;
  status)  for id in "${IDS[@]}"; do pf="$PID/broker-$id.pid"; is_alive "$pf" && echo "broker-$id RUNNING (pid $(cat "$pf"))" || echo "broker-$id stopped"; done ;;
  *) echo "usage: $0 {start|stop|status}"; exit 1 ;;
esac
EOF
chmod +x cluster-dr.sh

cd ..
```

## Trigger — Step 2: start the target cluster

```bash
cd kafka-lab-dr && ./cluster-dr.sh start && cd ..

# Sanity — target is up on 29092
$KAFKA/kafka-broker-api-versions.sh --bootstrap-server $DR | head -1
```

**T3 (target topic view)** should now show empty (just `__consumer_offsets` if you filter for it).

## Trigger — Step 3: write MM2 config

**Terminal 1:**

```bash
cd Lab-09-Cross-Cluster-Replication

cat > mm2.properties <<EOF
# ==== Cluster aliases ====
clusters = primary, dr

primary.bootstrap.servers = $SRC
dr.bootstrap.servers      = $DR

# ==== Enable primary -> dr replication (active-passive) ====
primary->dr.enabled = true
primary->dr.topics  = ordersorderevents-.*|checkout-events|inventory-.*
primary->dr.groups  = .*

# Explicitly disable the reverse direction so we don't accidentally loop
dr->primary.enabled = false

# ==== Replication policy ====
# Default: prefixes target topics with "primary." — safer for active-passive too.
replication.policy.class = org.apache.kafka.connect.mirror.DefaultReplicationPolicy

# ==== Internal topic settings (target cluster has 3 brokers) ====
replication.factor = 3
checkpoints.topic.replication.factor = 3
heartbeats.topic.replication.factor  = 3
offset-syncs.topic.replication.factor = 3

# ==== Sync intervals ====
sync.topic.configs.enabled = true
sync.topic.acls.enabled    = false           # ACL replication requires SASL; off for this lab
refresh.topics.interval.seconds  = 10        # check for new source topics every 10s
refresh.groups.interval.seconds  = 10
emit.checkpoints.interval.seconds = 5        # checkpoint offsets every 5s

# ==== Tasks (number of parallel replication threads per connector) ====
tasks.max = 2
EOF
```

**Topic filter explained:** `primary->dr.topics` is a regex. In this scenario we mirror any topic matching `orderevents-*`, `checkout-events`, or `inventory-*`. In production you'd usually just do `.*` and exclude what you don't want. For the demo we filter narrowly so unrelated topics from earlier scenarios don't pollute the DR cluster.

Fix that regex — it's got a typo above:

```bash
sed -i 's|primary->dr.topics.*|primary->dr.topics  = orderevents-.*|checkout-events|inventory-.*|' mm2.properties
# Hmm — pipes need escaping. Just rewrite the line cleanly:
sed -i '/primary->dr.topics/c primary->dr.topics  = orderevents-.*|checkout-events|inventory-.*' mm2.properties
grep 'topics' mm2.properties
```

## Trigger — Step 4: write a tiny MM2 helper and start it

**Terminal 1:**

```bash
cat > mm2.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
KAFKA="$DIR/../kafka-lab/kafka/bin"
LOG="$DIR/logs"; PID="$DIR/mm2.pid"
mkdir -p "$LOG"

case "${1:-}" in
  start)
    [[ -f "$PID" ]] && kill -0 "$(cat "$PID")" 2>/dev/null && { echo "mm2 already running"; exit 0; }
    echo "starting mm2..."
    nohup "$KAFKA/connect-mirror-maker.sh" "$DIR/mm2.properties" > "$LOG/mm2.log" 2>&1 &
    echo $! > "$PID"
    sleep 5
    echo "mm2 up (pid $(cat "$PID"))"
    ;;
  stop)
    [[ -f "$PID" ]] || { echo "mm2 not running"; exit 0; }
    kill "$(cat "$PID")" 2>/dev/null || true
    for _ in $(seq 1 20); do kill -0 "$(cat "$PID")" 2>/dev/null || break; sleep 1; done
    kill -0 "$(cat "$PID")" 2>/dev/null && kill -9 "$(cat "$PID")" 2>/dev/null || true
    rm -f "$PID"; echo "mm2 stopped"
    ;;
  status)
    [[ -f "$PID" ]] && kill -0 "$(cat "$PID")" 2>/dev/null && echo "mm2 RUNNING (pid $(cat "$PID"))" || echo "mm2 stopped"
    ;;
  *) echo "usage: $0 {start|stop|status}"; exit 1 ;;
esac
EOF
chmod +x mm2.sh
./mm2.sh start
```

**Terminal 4:**

```bash
tail -F Lab-09-Cross-Cluster-Replication/logs/mm2.log | grep --line-buffered -E 'ERROR|Connector|primary\.|Assigned'
```

Expected within ~10 s: lines showing all four connectors starting (`MirrorSourceConnector`, `MirrorCheckpointConnector`, `MirrorHeartbeatConnector`, `MirrorOffsetSyncConnector`) and target-side task assignments.

## Trigger — Step 5: produce to source, watch it appear on DR

**Terminal 1:**

```bash
# Create a topic on source that MATCHES the mm2 filter
$KAFKA/kafka-topics.sh --bootstrap-server $SRC \
  --create --topic orderevents-2025 --partitions 3 --replication-factor 3

# Produce
for i in $(seq 1 50); do echo "order-$i"; done | \
  $KAFKA/kafka-console-producer.sh --bootstrap-server $SRC --topic orderevents-2025
```

**T3 (target topic view)** — within ~10 s, a topic called **`primary.orderevents-2025`** appears (note the prefix).

Verify data:

```bash
$KAFKA/kafka-console-consumer.sh --bootstrap-server $DR \
  --topic primary.orderevents-2025 --from-beginning --timeout-ms 5000 | tail -10
```

Expected: `order-41 ... order-50` (last 10). All 50 records made it across.

## Observe
- **Target has three MM2-internal topics** — `primary.checkpoints.internal`, `primary.heartbeats`, `mm2-offset-syncs.primary.internal`. Don't delete them.
- **Topic configs are replicated too:**
  ```bash
  # Change retention on source
  $KAFKA/kafka-configs.sh --bootstrap-server $SRC --entity-type topics \
    --entity-name orderevents-2025 --alter --add-config retention.ms=3600000
  sleep 15  # wait for MM2 to sync
  $KAFKA/kafka-configs.sh --bootstrap-server $DR --entity-type topics \
    --entity-name primary.orderevents-2025 --describe | grep -i retention
  ```
- **Lag metric:** MM2's `MirrorSourceConnector` exposes `replication-latency-ms` via JMX (Connect worker metrics). In production you'd graph this in Grafana.

## Solution

- **Choose the replication policy deliberately:**
  - `DefaultReplicationPolicy` (target topics prefixed with `<source>.`) — the SAFE default. Prevents loops, makes it obvious which topics are mirrored.
  - `IdentityReplicationPolicy` (no prefix) — only for strict active-passive where consumers on DR should see the same topic names. Foot-gun for anything else.
  - Custom policies (implement `ReplicationPolicy`) — for teams with their own naming rules.

- **Topic filter granularity:**
  - `topics=.*` — mirror everything. Simplest; combine with `topics.exclude` to skip internal / test topics.
  - `topics=orderevents-.*` — explicit include list. Safer; new topics need explicit addition.
  - **Never** use `topics=.*` with two-way replication and `IdentityReplicationPolicy` — loops.

- **Sync-what:**
  ```properties
  sync.topic.configs.enabled = true    # replicate topic configs
  sync.topic.acls.enabled    = true    # replicate ACLs (needs SASL both sides)
  emit.checkpoints.enabled   = true    # translate consumer offsets (default true)
  emit.heartbeats.enabled    = true    # useful for measuring replication lag
  ```

- **Scale-out:** `tasks.max` is per connector. For a 100-topic cluster you'd bump to 8-16. Each task pulls a subset of source partitions.

- **Latency budget:** MM2 batches records like any Kafka producer. Tune:
  ```properties
  # Reduce end-to-end lag at the cost of throughput
  primary->dr.producer.override.linger.ms = 5
  primary->dr.producer.override.batch.size = 16384
  ```
  Every source-cluster and producer property can be overridden per-cluster with the `<source>->target.consumer.override.*` and `<source>->target.producer.override.*` prefixes.

- **What to monitor** (JMX on the MM2 process):
  - `kafka.connect.mirror:type=MirrorSourceConnector-task-metrics,name=records-per-sec` — throughput per task.
  - `kafka.connect.mirror:type=MirrorSourceConnector-task-metrics,name=replication-latency-ms` — source-timestamp to target-append lag.
  - `kafka.connect.mirror:type=MirrorCheckpointConnector-metrics,name=checkpoint-latency-ms` — offset-translation freshness.
  - Alert on all three above thresholds you agree with the DR RTO.

## Verify

```bash
# 1. All four connector-manager task threads visible in MM2 log
grep -oE 'Connector [A-Za-z]+' Lab-09-Cross-Cluster-Replication/logs/mm2.log | sort -u

# 2. Target has the mirrored topic
$KAFKA/kafka-topics.sh --bootstrap-server $DR --list | grep primary\\.

# 3. Message counts match (approximately — MM2 duplicates on restart are possible)
echo -n "source count: "
$KAFKA/kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server $SRC --topic orderevents-2025 | \
  awk -F: '{sum+=$3} END{print sum}'
echo -n "target count: "
$KAFKA/kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server $DR --topic primary.orderevents-2025 | \
  awk -F: '{sum+=$3} END{print sum}'
# Expected: both 50 (plus/minus any duplicates from restarts)

# 4. Config sync worked (retention we set in Observe)
$KAFKA/kafka-configs.sh --bootstrap-server $DR --entity-type topics \
  --entity-name primary.orderevents-2025 --describe | grep retention
```

## Takeaway

> **MM2 is async, prefix-by-default, and does not fail over automatically. Set it up, monitor `replication-latency-ms`, and know your RPO.**

## Instructor notes
- Ask before Step 5: *"If I produce 1000 messages to source and immediately kill the source cluster, how many will reach DR?"* Answer depends on MM2 batch/linger — usually the last few hundred are lost. Emphasises "async ≠ synchronous."
- The prefix (`primary.` on target) is the whole loop-avoidance story. Draw the flow: primary → dr topic `primary.X` → if we then mirrored dr→primary, target on primary would be `dr.primary.X`, which doesn't match `topics=X` — loop broken.
- MM2's internal topics (`*.checkpoints.internal`, `*.heartbeats`, `mm2-offset-syncs.*.internal`) look like clutter but are load-bearing. Don't delete.
- Bridge to 9.2: *"Data mirrored. But consumer offsets on the source are different physical numbers from target. Failover needs offset translation — that's the next scenario."*

## Teardown

Leave MM2 + DR cluster up if you're moving on to 9.2. Otherwise:

```bash
# Stop MM2
cd Lab-09-Cross-Cluster-Replication && ./mm2.sh stop && cd ..

# Stop DR cluster
cd kafka-lab-dr && ./cluster-dr.sh stop && cd ..

# Delete the source topic we created
$KAFKA/kafka-topics.sh --bootstrap-server $SRC --delete --topic orderevents-2025
```
