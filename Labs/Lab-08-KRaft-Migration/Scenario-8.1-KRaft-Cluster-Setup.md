# Scenario 8.1 — Stand up a KRaft cluster (side-by-side with the ZK cluster)

**Prereqs**
- Labs 1 and 3 done.
- The ZK cluster may be running or stopped — the KRaft cluster uses different ports.

**Paste at the top of every new terminal:**

```bash
cd ~/kafka-administration
export KAFKA=./kafka-lab/kafka/bin
export BS_KRAFT=localhost:19092,localhost:19093,localhost:19094
```

---

## Problem

- **KRaft = "Kafka Raft"** — Kafka's own consensus protocol replacing ZooKeeper for cluster metadata. GA since Kafka 3.3; ZooKeeper mode fully removed in Kafka 4.0.

- **What ZooKeeper did in the old world:**
  - Elected the controller (ephemeral znode).
  - Stored broker registrations (`/brokers/ids/*`), topics, partitions, ACLs, dynamic configs, quotas.
  - Every broker held a ZK session; loss of that session = broker fenced (the mechanism we abused in Sc 3.2).

- **What KRaft does differently:**
  - Cluster metadata lives in an internal **Kafka topic** — `__cluster_metadata` — replicated via Raft across a small quorum of **controller** nodes (typically 3 or 5).
  - The controller quorum has its own election protocol (Raft's leader election), separate from broker-leader election for user topics.
  - Brokers **tail** the metadata log, applying changes locally. No RPC push, no ephemeral znodes, no ZK session concept.
  - Broker "identity" is a registration record in the metadata log with a monotonic epoch bumped every restart — no ephemeral to accidentally delete (Sc 3.2 becomes impossible).

- **Two deployment topologies:**
  - **Isolated** — controller nodes are separate JVMs from broker nodes. Preferred for large clusters; controller resource use is decoupled from data plane.
  - **Combined** — each node is both a broker and a controller (`process.roles=broker,controller`). Fine for small clusters, dev, and this lab.

- **Config knobs unique to KRaft:**
  - `process.roles=broker,controller` — role list for this node.
  - `node.id=N` — replaces `broker.id`. Must be unique in the cluster.
  - `controller.quorum.voters=1@host1:9095,2@host2:9095,3@host3:9095` — the static voter list; every node has the same value.
  - `controller.listener.names=CONTROLLER` — which listener the controller quorum uses.
  - `listeners=PLAINTEXT://:19092,CONTROLLER://:9095` — same listener syntax as ZK mode, plus a CONTROLLER listener.
  - Storage needs to be **formatted** with a cluster UUID via `kafka-storage.sh format` before the first start.

- **What incidents disappear vs stay the same:**
  - **Gone:** stale broker znodes (no znodes), ZK session timeouts, ZK ensemble ops (backups, snapshots).
  - **Gone-ish:** controller failover — Raft election takes a few hundred ms; ZK's controller re-election was more theatrical.
  - **Same:** replica placement, ISR shrink/expand, unclean election trade-offs, min.ISR semantics, retention/compaction, quotas, ACLs. Everything you learned in Labs 3–7 still applies — just the control-plane substrate is different.

## Symptom

Not a failure scenario — this is setup. Failure modes to watch out for:

- Started brokers before formatting storage → `InconsistentClusterIdException` or `The Kafka cluster must be formatted with kafka-storage.sh format`.
- `node.id` collision with the voter list → `NodeIdMismatchException`.
- Wrong cluster UUID (used the wrong one on one node) → the fresh node refuses to join.

## Setup — 3 terminals

**Terminal 1 (control):**

```bash
cd ~/kafka-administration
mkdir -p kafka-lab-kraft/{config,data,logs,pids}
cd kafka-lab-kraft
ln -sf ../kafka-lab/kafka kafka        # share the distribution
ls -la kafka
```

**Terminal 2 (per-node port view):**

```bash
watch -n 2 "ss -ltnp 2>/dev/null | grep -E ':(909[567]|1909[234])\b'"
```

**Terminal 3 (KRaft controller log tail — we'll open once the first node starts):**

```bash
# Leave empty for now; we'll tail logs/kraft-broker-1.log after Step 3
```

## Trigger — Step 1: generate configs for 3 combined-mode nodes

Paste in Terminal 1 (from `kafka-lab-kraft/`):

```bash
LAB_KRAFT="$(pwd)"

for id in 1 2 3; do
  broker_port=$((19091 + id))          # 19092 / 19093 / 19094
  ctrl_port=$((9094 + id))             # 9095  / 9096  / 9097
  cat > config/kraft-broker-$id.properties <<EOF
# --- Combined-mode KRaft node ---
process.roles=broker,controller
node.id=$id
cluster.id=Y2xhc3NyaWNhZmthbGFi
                        # arbitrary base64; overwritten by kafka-storage.sh in Step 2

# Listeners
listeners=PLAINTEXT://:$broker_port,CONTROLLER://:$ctrl_port
advertised.listeners=PLAINTEXT://localhost:$broker_port
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT

# Raft quorum — every node has the same voter list
controller.quorum.voters=1@localhost:9095,2@localhost:9096,3@localhost:9097

# Storage
log.dirs=$LAB_KRAFT/data/kraft-broker-$id

# Small-cluster defaults so __consumer_offsets is create-able on 3 nodes
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2

# Faster housekeeping so demos are watchable
log.retention.check.interval.ms=10000
EOF
done

ls -la config/
grep -E '^(node.id|listeners|controller.quorum|log.dirs)' config/kraft-broker-1.properties
```

## Trigger — Step 2: generate a cluster UUID and format storage on every node

```bash
# 1. Generate a random cluster UUID (used by all 3 nodes)
CLUSTER_ID=$(./kafka/bin/kafka-storage.sh random-uuid)
echo "cluster UUID: $CLUSTER_ID"

# 2. Format each node's log.dirs with that UUID
for id in 1 2 3; do
  ./kafka/bin/kafka-storage.sh format \
    -t "$CLUSTER_ID" \
    -c config/kraft-broker-$id.properties
done

# 3. Sanity — every data dir now has a meta.properties
head -5 data/kraft-broker-1/meta.properties
```

Expected: `meta.properties` shows the cluster UUID + node.id. The `data/kraft-broker-N` directory is now formatted for the KRaft metadata log.

## Trigger — Step 3: write a mini cluster helper

The Lab-01 `cluster.sh` is ZK-specific. Write a KRaft equivalent:

```bash
cat > cluster-kraft.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
LAB="$(cd "$(dirname "$0")" && pwd)"
CONF="$LAB/config"; LOG="$LAB/logs"; PID="$LAB/pids"
KAFKA="$LAB/kafka/bin"
IDS=(1 2 3)
mkdir -p "$LOG" "$PID"

is_alive() { [[ -f "$1" ]] && kill -0 "$(cat "$1")" 2>/dev/null; }

start_node() {
  local id="$1" pf="$PID/kraft-broker-$id.pid"
  if is_alive "$pf"; then echo "kraft-broker-$id already running"; return; fi
  echo "starting kraft-broker-$id..."
  nohup "$KAFKA/kafka-server-start.sh" "$CONF/kraft-broker-$id.properties" \
      > "$LOG/kraft-broker-$id.log" 2>&1 &
  echo $! > "$pf"
  sleep 2
  echo "kraft-broker-$id up (pid $(cat "$pf"))"
}
stop_node() {
  local id="$1" pf="$PID/kraft-broker-$id.pid"
  if ! is_alive "$pf"; then echo "kraft-broker-$id not running"; rm -f "$pf"; return; fi
  local pid=$(cat "$pf")
  echo "stopping kraft-broker-$id (pid $pid)..."
  kill "$pid"
  for _ in $(seq 1 30); do is_alive "$pf" || break; sleep 1; done
  is_alive "$pf" && kill -9 "$pid" 2>/dev/null || true
  rm -f "$pf"; echo "kraft-broker-$id stopped"
}
status() {
  for id in "${IDS[@]}"; do
    pf="$PID/kraft-broker-$id.pid"
    if is_alive "$pf"; then echo "kraft-broker-$id RUNNING (pid $(cat "$pf"))"
    else                    echo "kraft-broker-$id stopped"; fi
  done
}

case "${1:-}" in
  start)   for id in "${IDS[@]}"; do start_node "$id"; done ;;
  stop)    for id in "${IDS[@]}"; do stop_node  "$id"; done ;;
  restart) for id in "${IDS[@]}"; do stop_node  "$id"; start_node "$id"; done ;;
  start-node)   start_node "${2:?node id required}" ;;
  stop-node)    stop_node  "${2:?node id required}" ;;
  status)  status ;;
  *) echo "usage: $0 {start|stop|restart|start-node <id>|stop-node <id>|status}"; exit 1 ;;
esac
EOF
chmod +x cluster-kraft.sh
```

## Trigger — Step 4: start the cluster

```bash
./cluster-kraft.sh start
```

**Terminal 3 (tail broker 1 for controller election evidence):**

```bash
tail -F logs/kraft-broker-1.log | grep --line-buffered -E 'Raft|Leader|Controller|Ready to serve'
```

Expected: within a few seconds, one node becomes the Raft leader; the others become followers. Look for lines like:

```
INFO [RaftManager id=1] Completed transition to LeaderState(...) (org.apache.kafka.raft.KafkaRaftClient)
INFO [BrokerServer id=1] Transition from STARTING to STARTED (kafka.server.BrokerServer)
```

## Observe

- **Terminal 2 (ports):** 6 sockets bound — one CONTROLLER + one PLAINTEXT per node.

- **Inspect the Raft quorum:**
  ```bash
  ./kafka/bin/kafka-metadata-quorum.sh \
    --bootstrap-controller localhost:9095 describe --status
  ```

  Expected: `LeaderId` = one of 1/2/3, `HighWatermark` incrementing, three voters listed.

- **Inspect a snapshot of the metadata log:**
  ```bash
  # Point at the metadata log directory of any node
  ./kafka/bin/kafka-metadata-shell.sh --snapshot data/kraft-broker-1/__cluster_metadata-0/*.log
  # Interactive shell prompts — try:  ls /brokers; cat /brokers/1
  #                                   ls /topics
  #                                   help; exit
  ```

  This is the equivalent of `zookeeper-shell.sh` in ZK mode — but walking a Kafka log, not znodes.

- **Prove the cluster works:**
  ```bash
  $KAFKA/kafka-topics.sh --bootstrap-server $BS_KRAFT \
    --create --topic kraft-hello --partitions 3 --replication-factor 3

  echo "hello KRaft" | $KAFKA/kafka-console-producer.sh --bootstrap-server $BS_KRAFT --topic kraft-hello
  $KAFKA/kafka-console-consumer.sh --bootstrap-server $BS_KRAFT --topic kraft-hello \
    --from-beginning --timeout-ms 3000
  ```

- **Prove no ZooKeeper is involved:**
  ```bash
  ss -ltnp 2>/dev/null | grep :2181 || echo "no ZooKeeper listener on this host"
  ```

## Trigger — Step 5: kill the Raft leader, watch failover

```bash
# Find who the current leader is (its node id)
./kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-controller localhost:9095 describe --status | grep -E 'LeaderId|CurrentVoters'
```

Note the `LeaderId` (say it's `1`). Stop it hard:

```bash
kill -9 $(cat pids/kraft-broker-1.pid)
sleep 2
```

Re-query — the Raft quorum should show a different `LeaderId` within a couple of seconds. Recover:

```bash
rm -f pids/kraft-broker-1.pid
./cluster-kraft.sh start-node 1
sleep 3
./kafka/bin/kafka-metadata-quorum.sh --bootstrap-controller localhost:9095 describe --status
```

Node 1 is back as a follower, catching up on the metadata log.

## Solution

- **Voter quorum sizing:**
  - 3 voters → tolerates 1 failure. Small teams, dev clusters.
  - 5 voters → tolerates 2 failures. Production standard.
  - Even numbers of voters wastes a node (majority is still `⌈N/2⌉+1`). Always odd.

- **Combined vs isolated:**
  - Small clusters (≤ 6 brokers): combined mode saves nodes.
  - Anything larger: isolated controllers — dedicated JVMs, small heap (~1 GB), less GC pressure than a data-plane broker. Controllers can then be on cheaper hardware.

- **Sizing controller nodes:**
  - Metadata log grows with topics × partitions. Compact + snapshot policy keeps it small (`metadata.log.max.snapshot.interval.ms`).
  - Even a huge cluster's metadata is < 1 GB. Controller nodes don't need much disk.
  - Latency-sensitive: controller writes go through Raft consensus. Keep controller-to-controller RTT under 5 ms if possible.

- **`meta.properties` is sacred.** It ties a data dir to a node.id + cluster.id. Never edit by hand; never share across nodes; if you wipe `data/`, re-run `kafka-storage.sh format`.

- **Upgrading `metadata.version`:** each Kafka release ships new metadata record schemas. Bump the cluster's active version explicitly after upgrading:
  ```bash
  ./kafka/bin/kafka-features.sh --bootstrap-server $BS_KRAFT describe
  ./kafka/bin/kafka-features.sh --bootstrap-server $BS_KRAFT upgrade --release-version 3.9
  ```
  Skip this and you're running with pre-upgrade metadata semantics — sometimes fine, sometimes surprising.

- **Everything from Labs 3–7 still applies:**
  - `min.insync.replicas`, `acks=all` — identical semantics.
  - Rack awareness (`broker.rack`) — identical config.
  - Retention/compaction — identical.
  - ACLs — StandardAuthorizer replaces AclAuthorizer; ACL entries now live in the metadata log.
  - Quotas — identical, still per-client-id/user.

## Verify

```bash
# 1. All 3 nodes visible in the quorum
./kafka/bin/kafka-metadata-quorum.sh --bootstrap-controller localhost:9095 describe --status | \
  grep -E 'LeaderId|CurrentVoters|HighWatermark'

# 2. Metadata log has recent activity (offset > 0 and moving)
./kafka/bin/kafka-metadata-quorum.sh --bootstrap-controller localhost:9095 describe --replication

# 3. Client-side sanity — cluster is functional
$KAFKA/kafka-topics.sh --bootstrap-server $BS_KRAFT --list

# 4. No stale znodes (there IS no ZK)
./cluster-kraft.sh status
```

## Takeaway

> **KRaft moves cluster metadata into a Raft-replicated Kafka topic. Same broker semantics; simpler operations; entire classes of incidents (stale znodes, ZK session timeouts) disappear.**

## Instructor notes
- The single most powerful demo is running Sc 3.2 (stale broker epoch) on this KRaft cluster. There's no znode to delete → the pathological path can't happen. Do it live if time permits.
- The Raft-election demo (Step 5) is fast — < 2 seconds — and unremarkable. That IS the point. Contrast with ZK: controller failover was theatrical because clients depended on ZK session semantics.
- Trainees often ask *"which mode should I use in production?"* Answer: KRaft, always, for any new cluster on Kafka 3.5+. Existing clusters migrate via Sc 8.2.
- Bridge to 8.2: *"You've stood up a KRaft cluster from scratch. What if you have a working ZK cluster with real data — how do you get from there to here without downtime? That's the migration."*

## Teardown

```bash
./cluster-kraft.sh stop
# The kafka-lab-kraft/ folder can be removed entirely — it's independent of the ZK cluster:
#   cd ~/kafka-administration && rm -rf kafka-lab-kraft
```
