# Scenario 5.2 — Partition reassignment without saturating the network

**Prereqs**
- Labs 3 and 4 done.
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

- **Why reassignment happens:**
  - Adding a broker → new node has 0 partitions; existing brokers are hot. Reassign to move some partitions to the new broker.
  - Removing a broker → its partitions must be moved off before you can decommission it.
  - Rack awareness added to an existing cluster (from 2.3) → old topics need to be re-placed.
  - Chronic skew — one broker owns 60% of leaderships. Rebalance to even it out.

- **Reassignment is a plain data copy.**
  - The target broker becomes a **follower** for each moved partition and fetches from the current leader — same code path as normal replication.
  - Leadership doesn't switch until the target is fully caught up (in ISR).
  - Old replicas are removed after leadership migrates.
  - For big partitions (TBs), this is a lot of bytes over the network — same NIC that your clients use.

- **Naive reassignment = self-inflicted DoS.**
  - Without a throttle, the target follower fetches at full bandwidth — often saturating the leader's outbound NIC.
  - Producers on the same broker see backpressure; consumers see fetch delays.
  - You start a rebalance during a quiet hour, forget it's running, business-hours traffic hits, everything's slow.

- **The throttle is two knobs, coordinated:**
  - `leader.replication.throttled.rate` on the leader broker — cap outbound replication bytes/sec for throttled replicas.
  - `follower.replication.throttled.rate` on the follower broker — cap inbound replication bytes/sec.
  - Plus per-topic markers telling the brokers **which** replicas count as "throttled" (partition:broker list).
  - `kafka-reassign-partitions --execute --throttle N` sets **all four** in one shot.

- **`--verify` is required and clears throttles.**
  - `--execute` starts the copy; brokers may be busy for seconds, minutes, or hours depending on data size.
  - `--verify` reports progress. When all partitions have caught up and been reassigned, `--verify` automatically **removes the throttle configs**.
  - If you skip `--verify`, throttles stay applied forever — killing replication performance for **all** future writes to those topics.

- **The three-step workflow:**
  1. **Generate** a proposed plan via `--generate` (takes a topics-to-move JSON + target broker list).
  2. **Execute** the plan via `--execute --throttle N`.
  3. **Verify** repeatedly until "completed successfully" — this also clears the throttles.

## Symptom
- **Without throttle:** client-side p99 latency spikes during reassignment. Producer `records/sec` drops. Consumer lag grows temporarily.
- **Throttle never cleared** (forgot `--verify`): a normal (non-reassignment) follower runs slow because `follower.replication.throttled.rate` still applies. Symptoms match Scenario 3.3 (chronic URP).
- **Reassignment stuck:** `--verify` reports "in progress" for hours with no forward motion. Root cause is often: target broker down, leader can't keep up (its own replication throttle), or throttle set too low for the volume.

## Setup — 4 terminals

**Terminal 1 (control):**

```bash
# Two topics with the SAME initial placement: replicas on brokers 101 and 102 only.
# We'll move them to include broker 103 — one throttled, one not.
$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --create --topic g1-throttled  --partitions 3 --replica-assignment 101:102,101:102,101:102
$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --create --topic g1-unthrottled --partitions 3 --replica-assignment 101:102,101:102,101:102

$KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic g1-throttled
$KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic g1-unthrottled
```

- Baseline: both topics have `Replicas: 101,102, Isr: 101,102` on every partition. Broker 103 owns nothing.

**Terminal 2 (per-broker byte-rate probe):**

```bash
watch -n 2 "for id in 101 102 103; do
  echo -n \"broker \$id BytesIn/s: \"
  $KAFKA/kafka-run-class.sh kafka.tools.JmxTool \
    --jmx-url service:jmx:rmi:///jndi/rmi://localhost:91\$id/jmxrmi \
    --object-name 'kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec' \
    --one-time true 2>/dev/null | grep -oE 'OneMinuteRate=[0-9.]+' || echo n/a
done"
```

- Shows per-broker inbound byte rate. During an active reassignment, the target broker (103) will show a spike; throttled runs cap it, unthrottled runs peak much higher.

**Terminal 3 (topic view during reassignment):**

```bash
watch -n 1 "for t in g1-throttled g1-unthrottled; do
  echo === \$t ===
  $KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic \$t | tail -3
done"
```

- During reassignment, `Replicas` shows both the current and new brokers (union set). Once caught up, `Isr` grows to include the new broker, then old ones are removed.

**Terminal 4 (broker 101 log):**

```bash
tail -F logs/broker-101.log
```

## Trigger — Step 1: fill both topics with data

Paste in Terminal 1:

```bash
# 50 MB per topic — enough to make a throttled move take ~10 s
for topic in g1-throttled g1-unthrottled; do
  $KAFKA/kafka-producer-perf-test.sh \
    --topic $topic \
    --num-records 50000 \
    --record-size 1000 \
    --throughput -1 \
    --producer-props bootstrap.servers=$BS acks=all
done

# Verify size on disk
du -sh data/broker-101/g1-throttled-* data/broker-101/g1-unthrottled-* 2>/dev/null
```

## Trigger — Step 2: generate a plan (spread to 103)

```bash
# The topics-to-move descriptor
cat > /tmp/topics-to-move.json <<EOF
{
  "topics": [
    {"topic": "g1-throttled"},
    {"topic": "g1-unthrottled"}
  ],
  "version": 1
}
EOF

# Ask Kafka to propose a rebalance across all 3 brokers
$KAFKA/kafka-reassign-partitions.sh --bootstrap-server $BS \
  --topics-to-move-json-file /tmp/topics-to-move.json \
  --broker-list "101,102,103" \
  --generate > /tmp/plan.out

cat /tmp/plan.out
```

The output has two JSON blocks: **Current** and **Proposed**. Save the Proposed block to its own file — we'll execute it twice below (once per topic path).

```bash
sed -n '/Proposed partition reassignment/,$p' /tmp/plan.out | tail -n +2 > /tmp/plan.json
cat /tmp/plan.json
```

## Trigger — Step 3A: execute the UNTHROTTLED path

We'll cheat by using the same plan for both topics but only measuring the unthrottled one first. Split the plan into a per-topic file so each move is independent:

```bash
# Extract only g1-unthrottled from the plan
python3 -c "
import json
plan = json.load(open('/tmp/plan.json'))
plan['partitions'] = [p for p in plan['partitions'] if p['topic'] == 'g1-unthrottled']
json.dump(plan, open('/tmp/plan-unthrottled.json', 'w'))
"

# Time the unthrottled move
echo "=== UNTHROTTLED start: $(date +%T) ==="
$KAFKA/kafka-reassign-partitions.sh --bootstrap-server $BS \
  --reassignment-json-file /tmp/plan-unthrottled.json \
  --execute

# Poll --verify until complete
while true; do
  out=$($KAFKA/kafka-reassign-partitions.sh --bootstrap-server $BS \
    --reassignment-json-file /tmp/plan-unthrottled.json \
    --verify 2>&1)
  echo "$out" | tail -5
  echo "$out" | grep -q "completed successfully" && break
  sleep 1
done
echo "=== UNTHROTTLED done:  $(date +%T) ==="
```

**Watch Terminals 2 and 3.** T2's `BytesIn/s` for broker 103 will spike sharply. T3 shows `Replicas` union then convergence.

## Trigger — Step 3B: execute the THROTTLED path

```bash
# Extract only g1-throttled from the plan
python3 -c "
import json
plan = json.load(open('/tmp/plan.json'))
plan['partitions'] = [p for p in plan['partitions'] if p['topic'] == 'g1-throttled']
json.dump(plan, open('/tmp/plan-throttled.json', 'w'))
"

# Time the throttled move (5 MB/s cap on both sides)
echo "=== THROTTLED start: $(date +%T) ==="
$KAFKA/kafka-reassign-partitions.sh --bootstrap-server $BS \
  --reassignment-json-file /tmp/plan-throttled.json \
  --execute \
  --throttle 5242880   # 5 MB/s

# Peek at the throttle configs Kafka just set
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type brokers --entity-name 101 --describe | grep throttle
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type topics --entity-name g1-throttled --describe | grep throttle

# Poll --verify until complete (this also clears throttles when done)
while true; do
  out=$($KAFKA/kafka-reassign-partitions.sh --bootstrap-server $BS \
    --reassignment-json-file /tmp/plan-throttled.json \
    --verify 2>&1)
  echo "$out" | tail -5
  echo "$out" | grep -q "completed successfully" && break
  sleep 1
done
echo "=== THROTTLED done:  $(date +%T) ==="
```

## Observe
- **Time to complete** (from the two timestamped `date +%T` pairs):
  - Unthrottled: usually **< 2 s** for 50 MB on localhost.
  - Throttled at 5 MB/s: ~**10 s** for 50 MB — the throttle is doing its job.
- **T2 (byte-rate probe):** during unthrottled, `BytesIn/s` on broker 103 goes into the tens of MB/s. During throttled, stays near 5 MB/s.
- **Throttle configs during throttled run:**
  - Broker 101/102: `follower.replication.throttled.rate=5242880`, `leader.replication.throttled.rate=5242880`.
  - Topic `g1-throttled`: `follower.replication.throttled.replicas=<partition>:<broker>,...`, same for leader side.
- **After `--verify` completes** (throttled run only):
  - Re-run the same `kafka-configs.sh --describe` — throttle keys are gone. Kafka auto-removed them.
- **T3 (topic view):** final state — both topics now have replicas spread across 101/102/103.

## Solution

- **Always `--throttle` in production.** Even on an idle cluster right now, a slow reassignment will inevitably overlap with production traffic. Pick a rate that leaves >= 80% of your NIC free for clients.
  ```bash
  # 20 MB/s throttle on a 1 Gbit link (~125 MB/s available)
  --throttle 20971520
  ```

- **Always `--verify` — every reassignment, until "completed successfully".**
  - `--verify` is what clears the throttle configs on completion. Skipping it means throttles stay in place forever.
  - Loop it in your runbook, not "check back in an hour."

- **Break TB-scale moves into stages.**
  - Move 10–20% of partitions at a time. Verify. Move the next batch.
  - Reason: if a target broker has a problem mid-move, you're only rolling back a small batch, not the whole cluster.

- **Never reassign during peak traffic windows.** Even with a modest throttle, reassignment is extra load. Schedule for low-traffic hours or maintenance windows.

- **Monitor while it runs:**
  ```bash
  # Cluster-wide "reassignment in progress" — should hit 0 when done
  $KAFKA/kafka-reassign-partitions.sh --bootstrap-server $BS \
    --reassignment-json-file /path/to/plan.json --verify

  # Per-broker: BytesInPerSec / BytesOutPerSec (JMX)
  # Alert if either exceeds ~80% of NIC line rate
  ```

- **If it gets stuck:**
  1. Check target broker is up (`ss -tlnp` on that host).
  2. Check throttle isn't so low that legitimate replication can't keep up. Bump with `kafka-configs --alter --add-config leader.replication.throttled.rate=<higher>`.
  3. Check disk on target isn't full.
  4. Last resort: cancel via `--cancel` (newer Kafka versions), then plan a smaller batch.

- **Post-mortem cleanup — always check for leftover throttles:**
  ```bash
  # Any broker with a lingering throttle config = someone forgot --verify
  for id in 101 102 103; do
    echo -n "broker $id: "
    $KAFKA/kafka-configs.sh --bootstrap-server $BS \
      --entity-type brokers --entity-name $id --describe 2>/dev/null | \
      grep -E 'throttled\.rate' || echo "clean"
  done

  # Nuke a leftover throttle manually
  $KAFKA/kafka-configs.sh --bootstrap-server $BS \
    --entity-type brokers --entity-name 101 \
    --alter --delete-config leader.replication.throttled.rate \
    --delete-config follower.replication.throttled.rate
  ```

## Verify

```bash
# 1. Both topics now use all 3 brokers
$KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic g1-throttled | grep -E 'Replicas:'
$KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic g1-unthrottled | grep -E 'Replicas:'

# 2. No leftover throttle configs anywhere
for id in 101 102 103; do
  echo -n "broker $id: "
  $KAFKA/kafka-configs.sh --bootstrap-server $BS \
    --entity-type brokers --entity-name $id --describe 2>/dev/null | \
    grep -E 'throttled\.rate' || echo "clean"
done
# Expected: all three "clean"

# 3. No pending reassignments
$KAFKA/kafka-reassign-partitions.sh --bootstrap-server $BS --list 2>/dev/null || \
  echo "no pending reassignments"
```

## Takeaway

> **Reassignment without `--throttle` is a self-inflicted DoS. Without `--verify`, the throttle never leaves.**

## Instructor notes
- Poll before Step 3A: *"On a healthy cluster with 100 MB/s traffic, what happens if you reassign a 200 GB partition unthrottled?"* Point out: it's not the reassignment that fails — it's the producers on that broker that time out.
- The visible payoff is Terminal 2 — watch broker 103's `BytesIn/s` in real time. The peak during unthrottled vs the flat plateau during throttled is the whole demo.
- Emphasise: `--verify` is not optional. Wire it into your reassignment runbook as a loop that exits on "completed successfully."
- Real-world story: a lot of "mystery slow followers" turn out to be leftover throttles from a reassignment done last month by someone who skipped `--verify`. This is the fingerprint to look for in Scenario 3.3 triage.
- Bridge to Lab 6: *"Now we've covered how partitions live, die, and move. Lab 6 is about the clients — how to keep one greedy consumer or producer from ruining the party for everyone else."*

## Teardown

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic g1-throttled
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic g1-unthrottled

# Defensive throttle cleanup (should be no-ops if --verify was run)
for id in 101 102 103; do
  $KAFKA/kafka-configs.sh --bootstrap-server $BS \
    --entity-type brokers --entity-name $id \
    --alter --delete-config leader.replication.throttled.rate \
    --delete-config follower.replication.throttled.rate 2>/dev/null
done

rm -f /tmp/topics-to-move.json /tmp/plan.out /tmp/plan.json \
      /tmp/plan-throttled.json /tmp/plan-unthrottled.json
```
