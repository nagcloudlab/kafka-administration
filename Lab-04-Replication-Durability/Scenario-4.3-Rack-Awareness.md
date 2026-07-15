# Scenario 4.3 — Rack awareness (placing replicas across failure domains)

**Prereqs**
- Lab 3 done.
- Cluster up: `./cluster.sh start` in `kafka-lab/`.

**Paste at the top of every new terminal:**

```bash
cd ~/kafka-administration/kafka-lab
export KAFKA=./kafka/bin
export BS=localhost:9092,localhost:9093,localhost:9094
export ZK=localhost:2181
```

---

## Problem

- **Default replica placement is rack-blind.**
  - Kafka's default placer round-robins replicas across brokers, ignoring which physical rack / AZ / failure-domain each broker belongs to.
  - On a real cluster with more brokers than replication factor (e.g., 6 brokers, RF=3, 3 AZs), the placer *can* end up putting all 3 replicas of a partition on brokers in the same rack.
  - Single-rack failure → all replicas of that partition go offline together → producer errors (if min.ISR breach) or unclean-election territory (if ISR empties).

- **`broker.rack` declares the broker's failure domain.**
  - Format: any string. Typically the AZ name (`us-east-1a`), rack ID (`rack-07`), or DC (`dc-blr-01`).
  - When set on **all** brokers, the placer becomes rack-aware: it tries to put every partition's replicas on different racks.
  - Rack awareness is applied at **topic-create time only**. Existing topics are *not* retroactively re-spread when `broker.rack` is added later.

- **Placement math:**
  - `RF ≤ number of racks` → guaranteed one replica per rack. Fully rack-safe.
  - `RF > number of racks` → best effort. At least one rack will hold ≥ 2 replicas → losing that rack costs 2 replicas at once.
  - Never run `RF > number of racks` on a durability-sensitive workload.

- **Consumer-side bonus (KIP-392 fetch-from-follower):**
  - Broker sets `replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector` (default is `LeaderSelector`).
  - Consumer sets `client.rack=<its-own-rack>`.
  - Consumer reads are served by a same-rack replica (leader or follower), not always the leader.
  - Cross-AZ bandwidth cost drops sharply — often the ROI that justifies rack awareness in cloud deployments.
  - Applies only to **reads**. Writes still go to the leader wherever it lives. Slight staleness possible (follower may lag leader by ms).

- **Common misconfigurations:**
  - `broker.rack` set on some brokers, not all → placer treats the missing-rack brokers as "unknown rack" → skewed placement.
  - Adding `broker.rack` to a running cluster with existing topics, expecting them to auto-fix. They don't.
  - Consumer sets `client.rack` but broker still runs default `LeaderSelector` → fetches always go to leader; rack setting is decoration.
  - Rack awareness used for **cross-region** durability. Wrong tool — use MirrorMaker 2 / Cluster Linking for cross-region (async). Rack awareness is for within-cluster synchronous replication (single WAN latency budget).

## Symptom
- **No visible symptom on a healthy cluster.** The problem only surfaces during an actual rack failure.
- When a rack fails without rack awareness:
  - Multiple replicas of the same partition go offline simultaneously.
  - `kafka-topics --describe`: `Isr` count drops by 2 (or more) for affected partitions, not by 1.
  - If `|ISR| < min.insync.replicas` → producer errors (2.1).
  - If ISR goes empty on affected partitions → unclean-election territory (2.2).
- Preventive check (no rack awareness in place): count brokers per rack in the replica list of each partition; any partition with all replicas in the same rack is at risk.

## Setup — 3 terminals

**Terminal 1 (control):**

```bash
# Baseline — no rack set on any broker yet
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type brokers --entity-name 101 --describe | grep -i rack || echo "no rack set"

# Baseline topic — created BEFORE rack awareness
$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --create --topic e1-pre-rack --partitions 6 --replication-factor 3

$KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic e1-pre-rack
```

**Terminal 2 (live view of both topics — pre-rack now, post-rack later):**

```bash
watch -n 2 "for t in e1-pre-rack e1-post-rack; do
  echo === \$t ===
  $KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic \$t 2>/dev/null | tail -7
done"
```

**Terminal 3 (broker 101 log tail — for restart confirmation):**

```bash
tail -F logs/broker-101.log
```

## Trigger — Part 1: add `broker.rack` and rolling-restart

### Step 1: append `broker.rack` to each broker config

Paste in Terminal 1:

```bash
# One broker per rack — cleanest visual on a 3-broker lab
echo "broker.rack=r1" >> config/broker-101.properties
echo "broker.rack=r2" >> config/broker-102.properties
echo "broker.rack=r3" >> config/broker-103.properties

# Verify all three
tail -1 config/broker-101.properties config/broker-102.properties config/broker-103.properties
```

### Step 2: rolling restart (graceful, from 1.1)

```bash
for id in 101 102 103; do
  echo "=== restarting broker $id ==="
  ./cluster.sh stop-broker $id
  sleep 3
  ./cluster.sh start-broker $id

  # Wait for Isr on our baseline topic to fully recover before touching the next broker
  until $KAFKA/kafka-topics.sh --bootstrap-server $BS \
       --describe --topic e1-pre-rack 2>/dev/null | \
       grep -q "Isr: 101,102,103\|Isr: 101,103,102\|Isr: 102,101,103\|Isr: 102,103,101\|Isr: 103,101,102\|Isr: 103,102,101"; do
    sleep 2
  done
  echo "broker $id healthy, ISR restored"
done
```

### Step 3: confirm each broker's rack is now known to the cluster

```bash
for id in 101 102 103; do
  echo "=== broker $id ==="
  $KAFKA/kafka-metadata-shell.sh --snapshot data/broker-101/__cluster_metadata-*/*.log 2>/dev/null \
    | head -1 || \
  echo 'ls /brokers/ids/'"$id" | $KAFKA/zookeeper-shell.sh $ZK 2>/dev/null | tail -1
done

# Alternative — describe the broker configs
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type brokers --entity-name 101 --describe | grep -i rack
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type brokers --entity-name 102 --describe | grep -i rack
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type brokers --entity-name 103 --describe | grep -i rack
```

### Step 4: create a NEW topic — rack-aware placement kicks in

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --create --topic e1-post-rack --partitions 6 --replication-factor 3

$KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic e1-post-rack
```

## Observe

- **Terminal 2** — the two topics side by side.
  - **e1-pre-rack**: replica assignment unchanged from when it was created. Whatever the pre-rack placer chose stays.
  - **e1-post-rack**: replicas span all three brokers (= all three racks). Rack-safe by construction.
- **Terminal 1 (Step 3)** — each broker reports its `rack` in `--describe`.
- **The subtle result:** on a 3-broker cluster with RF=3, both topics look nearly identical in `--describe` output — every partition uses all 3 brokers by necessity. **This is the point.** The real payoff appears when:
  - You have more brokers than RF (rack awareness picks *which* brokers).
  - Or you add brokers later (rack awareness protects new topics from bad choices).
  - Or you reassign existing topics (rack awareness guides the plan).

## Trigger — Part 2: retroactive fix for the OLD topic

The old topic was created rack-blind. Setting `broker.rack` didn't move any replicas. To fix, reassign explicitly.

**Terminal 1:**

```bash
# 1. Describe the topic we want to fix
cat > /tmp/topics-to-move.json <<EOF
{"topics":[{"topic":"e1-pre-rack"}],"version":1}
EOF

# 2. Ask Kafka to GENERATE a rack-aware plan across brokers 101,102,103
$KAFKA/kafka-reassign-partitions.sh --bootstrap-server $BS \
  --topics-to-move-json-file /tmp/topics-to-move.json \
  --broker-list "101,102,103" \
  --generate > /tmp/plan.out
cat /tmp/plan.out
```

- Output has two blocks: current placement + proposed placement. Save the "Proposed" JSON.

```bash
# 3. Extract the proposed plan and execute it
sed -n '/Proposed partition reassignment/,$p' /tmp/plan.out | tail -n +2 > /tmp/reassign.json
cat /tmp/reassign.json

$KAFKA/kafka-reassign-partitions.sh --bootstrap-server $BS \
  --reassignment-json-file /tmp/reassign.json \
  --execute

# 4. Verify
$KAFKA/kafka-reassign-partitions.sh --bootstrap-server $BS \
  --reassignment-json-file /tmp/reassign.json \
  --verify
```

**Terminal 2** will show `e1-pre-rack`'s replica list flipping to match the new plan. On a 3-broker/3-rack lab this often looks like a no-op (already used all brokers); on a real cluster it's where the fix actually happens.

## Solution

- **Set `broker.rack` at cluster stand-up** — not later. Every broker gets one line in its properties:
  ```properties
  # broker-*.properties
  broker.rack=us-east-1a       # AZ / rack / failure-domain identifier
  ```
  Restart to pick up.

- **Every broker must have it set.** Missing brokers get treated as "no rack" and skew placement. Grep across broker configs before shipping:
  ```bash
  grep -L '^broker.rack=' config/broker-*.properties && \
    echo "ERROR: some brokers missing broker.rack"
  ```

- **If added later, reassign existing topics.** Rack awareness is create-time only. Old topics won't self-heal. Use the reassignment workflow shown above (also covered in more depth in 3.2).

- **RF ≤ number of racks** for durability-critical topics. `RF=3` needs 3 racks to be fully rack-safe. If you only have 2 AZs, either use RF=4 across 4 brokers spread 2/2 (not fully safe either — one AZ down = 2 replicas gone), or accept the trade-off consciously.

- **Fetch-from-follower for cost/latency:**
  ```properties
  # broker-*.properties (dynamic config also works)
  replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector
  ```
  ```properties
  # consumer config
  client.rack=us-east-1a       # same value as the local broker's broker.rack
  ```
  Verify with consumer JMX: `kafka.consumer:type=consumer-fetch-manager-metrics,client-id=X,topic=Y,partition=Z,name=preferred-read-replica`.

- **Cross-region is a different problem.** Rack awareness is for a single cluster spanning racks/AZs within one WAN latency budget. For cross-region: MirrorMaker 2 or Confluent Cluster Linking (asynchronous replication between separate clusters). Stretching one Kafka cluster across regions is almost always a bad idea (RTT kills ISR replication).

## Verify

```bash
# 1. Every broker knows its rack
for id in 101 102 103; do
  echo -n "broker $id: "
  $KAFKA/kafka-configs.sh --bootstrap-server $BS \
    --entity-type brokers --entity-name $id --describe 2>/dev/null | \
    grep -oE 'broker.rack=[^ ]+' || echo "MISSING"
done

# 2. Post-rack topic uses all brokers (= all racks)
$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --describe --topic e1-post-rack | grep -E 'Replicas:'

# 3. Check: no partition has all replicas in one rack (would be visible on a bigger cluster)
# For this 3-broker lab, all partitions use all brokers by necessity — always rack-safe.
```

## Takeaway

> **Set `broker.rack` cluster-wide at stand-up. Otherwise you're one accidental rack fault from an unclean election.**

## Instructor notes
- Poll before Step 4: *"Without rack awareness, is my 3-broker RF=3 cluster rack-safe?"* Trainees say yes (on 3 brokers with RF=3, all brokers are always used). The demo confirms — but the point is what happens with 6 brokers / 3 AZs (real cluster). Draw that on the board.
- The **not-retroactive** point is worth pausing on. Someone reads an article, adds `broker.rack`, restarts, thinks the cluster is safe. Six months later a rack fails and topics from before the change lose 2/3 of their replicas.
- Fetch-from-follower is the ROI story in cloud deployments. Cross-AZ read bandwidth on AWS is $0.02/GB, intra-AZ is $0.00 — for a 100 MB/s consumer that's ~$500/mo per consumer saved. That's what gets rack awareness prioritized in a budget meeting.
- Bridge to 3.1: *"Now that replicas are placed correctly, what happens to them over time? Retention and compaction control what stays on disk."*

## Teardown

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic e1-pre-rack
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic e1-post-rack

# Keep broker.rack in config? Yes — this is a valid production setting.
# To revert (unlikely): remove the appended line and rolling-restart.
```
