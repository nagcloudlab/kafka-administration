# Scenario 12.3 — Partition sizing &amp; growth (increase allowed, decrease impossible)

**Prereqs**
- Labs 1 and 3 done.
- Cluster on 9092–9094 running: `./cluster.sh start-monitoring` in `kafka-lab/`.

**Paste at the top of every new terminal:**

```bash
cd ~/kafka-administration/kafka-lab
export KAFKA=./kafka/bin
export BS=localhost:9092,localhost:9093,localhost:9094
export ZK=localhost:2181
```

---

## Problem

- **Adding partitions changes future key-to-partition mapping.** The standard producer partitioner is `partition = hash(key) % partitionCount`. When `partitionCount` changes, records for the same key can start landing on a **different** partition. Historical records for that key stay where they were.
- **Consequences of that reordering**:
  - **Per-key ordering is broken across the change.** A consumer reading records for `orderId=ORD-9001` may see the last historical event on partition 2 and the next new event on partition 5. Two different consumer instances (one per partition) process them in indeterminate relative order.
  - **Stateful consumers** (like Kafka Streams) that keep per-key state keyed by partition rebuild state from wrong offsets after a partition change.
  - **Exactly-once semantics** across the change are lost.

- **You cannot decrease the partition count.** Kafka has no shrink API. To reduce partitions you must:
  1. Create a new topic with fewer partitions.
  2. Copy data (MirrorMaker 2 or equivalent).
  3. Cut consumers over.
  4. Delete the old topic.
  A weeks-long migration. This is why choosing partition count carefully at create time matters.

- **Producers that don't use keys** (`key=null`) are safe from the reordering trap — the default partitioner distributes them for throughput, not by key. Only keyed producers hit the ordering problem.

- **Adding partitions doesn't rebalance existing data.** New partitions start empty. If the topic was skewed before, adding partitions doesn't fix historic imbalance — it only distributes *new* traffic differently.

- **When adding partitions IS the right answer**:
  - Consumer concurrency truly limited by partition count (group with 20 consumers on a 10-partition topic → 10 idle).
  - Per-partition throughput near tested safe limit.
  - Producer traffic is keyless or key-based reordering across the change is acceptable.
  - Cluster has capacity for more replicas + leaders.

- **When adding partitions is NOT the fix**:
  - **Hot partition** from bad key strategy → fix the key strategy first; adding partitions won't rebalance a `hash(country) % partitions` skew.
  - **Slow consumer** → fix the consumer (see Lab 6.1); more partitions don't process records faster if the code is the bottleneck.
  - **Broker CPU saturation** → more partitions add more overhead; consider more brokers instead.

- **Common misconfigurations**:
  - Adding partitions to fix a slow consumer without checking whether the consumer even uses multi-partition parallelism.
  - Adding partitions on a keyed topic without warning the application team → they process records out of key-order.
  - Assuming a rebalance will "spread the load" — it only spreads new traffic.

## Symptom

- **Consumer group has more consumers than partitions**: `kafka-consumer-groups --describe` shows some consumers assigned to zero partitions.
- **Per-partition traffic imbalance persists after adding partitions**: `kafka_topic_bytesinpersec{topic="X"}` still concentrated on old partitions; new ones empty.
- **Application logs suddenly report "unexpected event for order X"** shortly after partition count change: keyed consumers hit the reordering.
- **Attempts to reduce partitions**: `kafka-topics --alter --partitions <smaller-number>` fails with `InvalidPartitionsException: Topic currently has N partitions, which is higher than the requested M partitions. Number of partitions cannot be reduced.`

## Setup — 4 terminals

**Terminal 1 (control):**

```bash
# Topic starts with 3 partitions
$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --create --topic partition-growth --partitions 3 --replication-factor 3
```

**Terminal 2 (live topic view — leave running):**

```bash
watch -n 2 "$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --describe --topic partition-growth"
```

**Terminal 3 (per-partition consumer — leave running):**

```bash
$KAFKA/kafka-console-consumer.sh --bootstrap-server $BS \
  --topic partition-growth --from-beginning \
  --property print.key=true --property print.partition=true \
  --property key.separator=' | ' --formatter kafka.tools.DefaultMessageFormatter
```

The `--property print.partition=true` flag prints `Partition:N` on each record.

**Terminal 4 (used for producing keyed messages):**

Empty for now.

## Trigger — Step 1: produce keyed records, observe stable key→partition mapping

**Terminal 4:**

```bash
# Produce 30 records: 3 keys × 10 events each, in the format "key:value"
for i in $(seq 1 30); do
  key="ORD-$((1000 + i % 3))"        # ORD-1000, ORD-1001, ORD-1002 rotating
  echo "$key:event-$i"
done | $KAFKA/kafka-console-producer.sh --bootstrap-server $BS \
  --topic partition-growth \
  --property parse.key=true --property key.separator=:
```

**Terminal 3** now shows records with their partition assignment. Look for the pattern:

```
Partition:0    ORD-1000 | event-3
Partition:0    ORD-1000 | event-6
Partition:1    ORD-1001 | event-1
Partition:1    ORD-1001 | event-4
Partition:2    ORD-1002 | event-2
Partition:2    ORD-1002 | event-5
...
```

**Each key stays on the same partition.** `ORD-1000` always goes to (say) partition 0; `ORD-1001` to partition 1. That's the `hash(key) % 3` mapping.

Note down which key goes to which partition — you'll need this comparison after the growth.

## Trigger — Step 2: increase partition count from 3 → 6

**Terminal 1:**

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --alter --topic partition-growth --partitions 6

# Verify — describe now shows 6 partitions; new ones (3, 4, 5) start empty
$KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic partition-growth
```

**Terminal 2** — partitions 3, 4, 5 appear immediately with their own Leader/Replicas/Isr assignments. LogEndOffset shows 0 for these new partitions.

## Observe — Step 3: produce the same keys again, watch the reordering

**Terminal 4:**

```bash
# Same keys as Step 1 — but 3, 4, 5 didn't exist then
for i in $(seq 31 60); do
  key="ORD-$((1000 + i % 3))"
  echo "$key:event-$i"
done | $KAFKA/kafka-console-producer.sh --bootstrap-server $BS \
  --topic partition-growth \
  --property parse.key=true --property key.separator=:
```

**Terminal 3** — new records may now appear on **different** partitions than before. Look for keys that moved:

```
Partition:5    ORD-1000 | event-33       ← Was on partition 0 before!
Partition:5    ORD-1000 | event-36
Partition:1    ORD-1001 | event-31       ← Might stay on 1, or move
Partition:4    ORD-1002 | event-32       ← Was on 2 before, now on 4
```

The exact new mapping depends on `hash(key) % 6` vs the old `hash(key) % 3`. **Some keys stay, some move.** For any key that moved, all events after the partition change are on a different partition than events before.

## Observe — Step 4: prove per-key ordering is broken

To make the break dramatic, consume in strict partition order and look at one key:

**Terminal 1:**

```bash
# Consume everything from partition 0 (all of ORD-1000's OLD events)
$KAFKA/kafka-console-consumer.sh --bootstrap-server $BS \
  --topic partition-growth --partition 0 --from-beginning \
  --property print.key=true --property key.separator=' | ' \
  --timeout-ms 2000 | grep 'ORD-1000' | tail -5

echo "---"

# Now consume everything from the partition ORD-1000 moved to
# (find it from Step 3's output; assume it's partition 5)
$KAFKA/kafka-console-consumer.sh --bootstrap-server $BS \
  --topic partition-growth --partition 5 --from-beginning \
  --property print.key=true --property key.separator=' | ' \
  --timeout-ms 2000 | grep 'ORD-1000' | tail -5
```

Expected: `ORD-1000` events split across two partitions — earlier events on the old partition, later events on the new one. **Two independent consumers** (one per partition, as in a group) will process them in undefined relative order.

## Trigger — Step 5: prove you cannot decrease partitions

```bash
# Try to shrink from 6 back to 3
$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --alter --topic partition-growth --partitions 3 2>&1
```

Expected error:

```
Error while executing topic command : Topic currently has 6 partitions, which is higher
than the requested 3 partitions. Number of partitions cannot be reduced.
```

Kafka has no shrink operation. To end up with fewer partitions, the only path is:

```
create new topic with fewer partitions
  → mirror data across (MirrorMaker 2 from Lab 9)
    → cut consumers over
      → delete old topic
```

## Trigger — Step 6: prove keyless producers are safe

Different producer, no key:

```bash
# Produce 30 records with NO KEY
for i in $(seq 1 30); do echo "keyless-$i"; done | \
  $KAFKA/kafka-console-producer.sh --bootstrap-server $BS \
  --topic partition-growth
```

**Terminal 3** — with `enable.idempotence=true` (default in newer clients), the null-key partitioner uses a "sticky" strategy — batches go to one partition until a size/time threshold, then rotate. Not by hash. So keyless records don't have the "same key must land on same partition" contract; the partition change didn't break anything for them.

## Solution

- **Size partitions correctly at creation time.** Use the formula from the slide deck:
  ```
  Partitions = max(
    Pwrite  = peak MB/s ÷ safe MB/s per partition,
    Pread   = required group MB/s ÷ safe per-consumer MB/s,
    concurrency = max consumers you'll ever run
  ) × (1 + growth headroom)
  ```
  Err on the side of MORE partitions initially. You can't shrink, so it's a one-way ratchet.

- **If you MUST add partitions to a keyed topic, coordinate with the application team:**
  1. Announce the change in advance.
  2. Producers should PAUSE at a safe boundary if possible (drain in-flight writes for critical keys).
  3. Apply `--alter --partitions N` during a maintenance window.
  4. Consumers restart, pick up new assignments.
  5. Application must be resilient to the possibility that a key's ordering is broken across the change.

- **For high-cardinality keys** (millions of unique keys, e.g. `customerId`), the reordering trap is theoretical but real. The specific application usually only cares about ordering per key WITHIN some session, not across arbitrarily-long history — so the practical impact is small. Discuss with the team.

- **For low-cardinality keys** (e.g. `country`, 5–10 values), adding partitions barely helps distribution — you get more partitions but a few still get almost all the traffic. **Fix the key strategy first**, e.g., `<country>-<userId>` for compound distribution.

- **Alternative: create a new topic with more partitions and migrate.** This is the cleanest option when the current design is deeply wrong (bad keys, wrong partition count, wrong retention). More work upfront, no downstream surprises.

- **When you observe a hot partition**, check:
  ```bash
  # Bytes-in per partition (needs the JMX exporter from Lab 2)
  # In Grafana: kafka_topic_bytesinpersec{topic="X"} — but per-partition needs custom scraping
  # From CLI, use kafka-run-class kafka.tools.GetOffsetShell repeatedly and diff
  ```
  ```bash
  # Compare per-partition offsets 60 seconds apart to see per-partition rate
  $KAFKA/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --bootstrap-server $BS --topic partition-growth > /tmp/t1.txt
  sleep 60
  $KAFKA/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --bootstrap-server $BS --topic partition-growth > /tmp/t2.txt
  paste /tmp/t1.txt /tmp/t2.txt | awk -F: '{print $1":"$2" rate="($6-$3)" msg/min"}'
  ```
  If one partition rate is >2× the average, adding partitions won't fix it — fix the key strategy.

- **Never rely on the `--alter` command as a rollback path.** There isn't one. Every partition-add is permanent.

## Verify

```bash
# 1. Topic now has 6 partitions
$KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic partition-growth | head -1
# Expected: PartitionCount: 6

# 2. New partitions (3, 4, 5) received some records
for p in 3 4 5; do
  count=$($KAFKA/kafka-console-consumer.sh --bootstrap-server $BS \
    --topic partition-growth --partition $p --from-beginning --timeout-ms 2000 2>/dev/null | wc -l)
  echo "partition $p: $count records"
done

# 3. Confirm you cannot shrink
$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --alter --topic partition-growth --partitions 4 2>&1 | grep -o 'cannot be reduced'
# Expected: cannot be reduced

# 4. Any key that appears on ≥ 2 partitions after growth → the reordering trap fired for it
$KAFKA/kafka-console-consumer.sh --bootstrap-server $BS \
  --topic partition-growth --from-beginning --timeout-ms 3000 \
  --property print.key=true --property print.partition=true \
  --property key.separator='|' 2>/dev/null | \
  awk -F'|' '{print $1" "$2}' | sort | uniq | \
  awk '{print $2}' | sort | uniq -c | sort -rn | head
# Any key with count > 1 means it landed on multiple partitions across the growth
```

## Takeaway

> **Adding partitions is a one-way, application-visible change. Existing data doesn't rebalance; future keys may relocate; you can never shrink. Size correctly at creation; add later only with the app team's agreement.**

## Instructor notes
- Before Step 2, ask the room: *"If I add 3 partitions to a topic where all my `hash(key) % 3` traffic is concentrated on partition 0, does adding partitions fix the imbalance?"* Answer: no — only new traffic that happens to hash to the new partitions helps; old traffic on partition 0 stays there forever. The demo makes this concrete.
- Terminal 3 output during Step 3 is the visceral moment — trainees see `ORD-1000` flip to a new partition, and can point at the exact records that broke ordering. Read out one specific example.
- The `Number of partitions cannot be reduced` error in Step 5 is the single most-quoted-by-consultants Kafka error. Show it explicitly.
- Real-world story: a team increased partitions on their `payments.events` topic during peak traffic to "spread the load". Their fraud-detection service processed records from the same customer out of order for 20 minutes. Two customers were incorrectly flagged as fraud. Fixing the fraud model to handle out-of-order events took 6 weeks. Point at the "coordinate with app team" line.
- Bridge to 12.4: *"Adding partitions is one 'you can't undo' operation on a topic. Message size is another — the wrong limit at any layer and the whole write path stalls."*

## Teardown

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic partition-growth
```
