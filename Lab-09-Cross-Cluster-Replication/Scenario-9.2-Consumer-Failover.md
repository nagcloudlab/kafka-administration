# Scenario 9.2 — Consumer failover with offset translation

**Prereqs**
- Sc 9.1 done — DR cluster on 29092–29094 up, MM2 running, `orderevents-2025` mirrored as `primary.orderevents-2025`.

**Paste at the top of every new terminal:**

```bash
cd ~/kafka-administration
export KAFKA=./kafka-lab/kafka/bin
export SRC=localhost:9092,localhost:9093,localhost:9094
export DR=localhost:29092,localhost:29093,localhost:29094
```

---

## Problem

- **Naive failover fails.** If a consumer group `orders-processor` was at offset 42000 on source topic `orderevents-2025`, and the operator points that same group at target topic `primary.orderevents-2025`, the target broker has no idea what offset 42000 means for that group — the group has never committed on target. Depending on `auto.offset.reset`, the consumer restarts from the beginning (`earliest`, thousands of duplicates) or the end (`latest`, thousands of skipped records). Both are catastrophic.

- **Why source offsets don't match target offsets:**
  - Source batches records differently from MM2's producer to target.
  - MM2 may write MULTIPLE source records into a single target batch (or vice versa).
  - Restarts on either side create small gaps or duplicates.
  - Result: source offset 42000 might correspond to target offset 41894 — close, but not equal.

- **What MM2 records for translation:**
  - **`<source>.checkpoints.internal`** — per-group, per-partition checkpoints: "source-offset X corresponds to target-offset Y at time T".
  - Written by **`MirrorCheckpointConnector`** every `emit.checkpoints.interval.seconds` (default 60).
  - Only records offsets for groups that have committed something on source since the last checkpoint. Idle groups get stale checkpoints.

- **Two ways to use the checkpoints:**
  1. **`sync.group.offsets.enabled = true`** (auto-sync mode) — MM2 periodically writes translated offsets directly to target's `__consumer_offsets`. Consumer that fails over uses the same `group.id` and just resumes. **This is what this scenario demonstrates.**
  2. **`RemoteClusterUtils.translateOffsets()`** (Java API) — application code reads the checkpoints topic on-demand, translates a specific source offset to a target offset. Useful when you want explicit control.

- **Critical safety rule for auto-sync:** MM2 refuses to overwrite a group's offsets on target if the group is **currently active** on target. Otherwise you'd race MM2 vs the active consumer. Failover procedure:
  1. Stop consumer on source.
  2. Wait for MM2 to write the final checkpoint (up to `emit.checkpoints.interval.seconds`).
  3. Start consumer on target with the same `group.id`.

- **Common misconfigurations:**
  - `sync.group.offsets.enabled = false` (the default) → target `__consumer_offsets` never gets translated. Failover resumes from beginning or end.
  - Consumer on target started BEFORE MM2 has emitted a checkpoint for that group → group registers on target with offset 0 (or -1); MM2 then refuses to overwrite because the group is now active. Deadlock.
  - Consumer on source using `enable.auto.commit=false` and never calling `commitSync()` → nothing to checkpoint. MM2 has no signal.

## Symptom

- **No sync configured, plain fail-over:** consumer log on target shows `Seeking to LATEST offset of partition primary.orderevents-2025-0` (or earliest); either thousands of duplicates or thousands of skipped records; end-user tickets.
- **Sync configured but consumer started too early:** target `__consumer_offsets` for the group shows position 0 for every partition, but MM2 never overwrites; group processes from start.
- **Consumer on both source AND target simultaneously:** duplicate processing; consumer group is split (one in each cluster).

## Setup — 4 terminals

**Terminal 1 (control):**

```bash
cd ~/kafka-administration
```

**Terminal 2 (source consumer — will be the one that "fails over"):**

Open but don't run anything yet.

**Terminal 3 (target consumer — the failover destination):**

Open but don't run anything yet.

**Terminal 4 (offsets probe — leave running):**

```bash
watch -n 3 '
echo "=== source: orders-processor on orderevents-2025 ==="
'"$KAFKA"'/kafka-consumer-groups.sh --bootstrap-server '"$SRC"' \
  --describe --group orders-processor 2>/dev/null | head -6

echo
echo "=== target: orders-processor on primary.orderevents-2025 ==="
'"$KAFKA"'/kafka-consumer-groups.sh --bootstrap-server '"$DR"' \
  --describe --group orders-processor 2>/dev/null | head -6
'
```

## Trigger — Step 1: enable auto-sync in MM2

The mm2.properties from 9.1 has `emit.checkpoints.interval.seconds = 5` — great, we get checkpoints quickly. Add auto-sync:

```bash
cd Lab-09-Cross-Cluster-Replication

# Idempotent: replace or add the line
grep -q '^sync.group.offsets.enabled' mm2.properties && \
  sed -i 's|^sync.group.offsets.enabled.*|sync.group.offsets.enabled = true|' mm2.properties || \
  echo 'sync.group.offsets.enabled = true' >> mm2.properties

grep -q '^sync.group.offsets.interval.seconds' mm2.properties && \
  sed -i 's|^sync.group.offsets.interval.seconds.*|sync.group.offsets.interval.seconds = 5|' mm2.properties || \
  echo 'sync.group.offsets.interval.seconds = 5' >> mm2.properties

# Restart MM2 to pick up the change
./mm2.sh stop && ./mm2.sh start
cd ..
```

## Trigger — Step 2: produce some more data + start a consumer on source

**Terminal 1:**

```bash
# Top up the source topic
for i in $(seq 51 200); do echo "order-$i"; done | \
  $KAFKA/kafka-console-producer.sh --bootstrap-server $SRC --topic orderevents-2025
```

**Terminal 2 (consumer on SOURCE, group `orders-processor`):**

```bash
$KAFKA/kafka-console-consumer.sh --bootstrap-server $SRC --topic orderevents-2025 \
  --group orders-processor --from-beginning
```

Consumes ~200 messages, then keeps polling with no more data.

**Terminal 4 (probe)** — within ~10 s of the consumer starting:
- **Source** side shows `orders-processor` with CURRENT-OFFSET = 200 (or so) per partition.
- **Target** side shows `orders-processor` with CURRENT-OFFSET populated too — MM2 auto-synced the translated offsets. The number may be slightly different from source (e.g. 199 or 201) due to batching.

## Trigger — Step 3: fail over — stop the source consumer, start on target

**Terminal 2:** press **Ctrl+C** to stop the source consumer. Wait 5–10 s for MM2 to write a final checkpoint (interval was set to 5 s).

**Terminal 3 (consumer on TARGET, same group):**

```bash
$KAFKA/kafka-console-consumer.sh --bootstrap-server $DR --topic primary.orderevents-2025 \
  --group orders-processor
```

**Observe:**
- The consumer starts and **does not** re-print the 200 messages. It's positioned at the translated offset.
- **Produce more messages to the SOURCE** (they'll flow through MM2):
  ```bash
  # Terminal 1
  for i in $(seq 201 210); do echo "order-$i"; done | \
    $KAFKA/kafka-console-producer.sh --bootstrap-server $SRC --topic orderevents-2025
  ```
- Terminal 3 (target consumer) prints `order-201 ... order-210` within ~5–10 s.

The failover worked. No duplicates, no gaps.

## Observe — inspect the checkpoints topic

**Terminal 1:**

```bash
# Directly read the internal checkpoint topic
$KAFKA/kafka-console-consumer.sh --bootstrap-server $DR \
  --topic primary.checkpoints.internal \
  --from-beginning --timeout-ms 3000 \
  --formatter org.apache.kafka.connect.mirror.formatters.CheckpointFormatter 2>/dev/null | head -10
```

Each record is a checkpoint entry: `Checkpoint{consumerGroupId=orders-processor, topicPartition=orderevents-2025-N, upstreamOffset=X, downstreamOffset=Y, metadata=...}`. This is what MM2's `sync.group.offsets` reads and applies to target `__consumer_offsets`.

## Trigger — Step 4: the safety rule (target group must not be active)

**Terminal 2 (restart source consumer):**

```bash
$KAFKA/kafka-console-consumer.sh --bootstrap-server $SRC --topic orderevents-2025 \
  --group orders-processor
```

Meanwhile the Terminal 3 consumer is still running on target. Now both are active for the SAME group.

**Terminal 4 (probe)** — you'll see MM2 stops updating the target's offsets (its safety check: "group is active on target, don't touch"). If the source consumer commits new offsets, target-side sync **freezes at the last update**. Look in MM2 log:

```bash
grep -E 'active|refusing|skip' Lab-09-Cross-Cluster-Replication/logs/mm2.log | tail
```

You may see lines like `Skipping offset sync for group orders-processor: consumer active on target`.

**Fix:** stop one of the consumers. Never run a consumer group on both source and target simultaneously.

Stop Terminal 3 (Ctrl+C).

## Solution

- **Failover runbook:**
  1. **Detect** — automated health check on source cluster (broker liveness, replication lag).
  2. **Fence source** — either take source offline entirely, or (better) mark it read-only via `kafka-acls.sh` for the affected consumer groups.
  3. **Wait one `sync.group.offsets.interval.seconds`** for MM2 to write a final checkpoint.
  4. **Redirect consumers** — update their bootstrap-servers config to point at target; keep `group.id` the same; add the `<source>.` prefix to topic names (unless using `IdentityReplicationPolicy`).
  5. **Verify no duplicates / gaps** — spot-check a few partitions manually.

- **Failback runbook (source cluster comes back later):**
  1. Set up **reverse** MM2 (dr → primary). Enable `sync.group.offsets.enabled` there too.
  2. Wait until reverse replication has caught up.
  3. Same fence-wait-redirect pattern back to primary.

- **Alerts to page on:**
  - `MirrorCheckpointConnector` task down for > 5 min — consumers can't fail over cleanly.
  - Target-side `__consumer_offsets` lag vs source > 30 s — stale checkpoints; failover would resume from too-old a point.
  - Consumer active on BOTH clusters for the same group id — split-brain.

- **Java-side alternative** for surgical control:
  ```java
  RemoteClusterUtils.translateOffsets(
    props,                                    // properties pointing at target cluster
    "primary",                                // source alias
    "orders-processor",                       // group id
    Duration.ofSeconds(10));                  // timeout
  // Returns Map<TopicPartition, OffsetAndMetadata>
  // Feed to consumer.seek(...) before subscribing.
  ```
  Use when you want the app to decide the exact failover moment (e.g. based on a business signal, not just source health).

- **Avoid** using `auto.offset.reset` as a failover strategy. On any target consumer, set it to `none` — that way a missing group triggers an explicit exception you can handle, rather than silently starting from beginning/end.

## Verify

```bash
# 1. Target consumer group is populated
$KAFKA/kafka-consumer-groups.sh --bootstrap-server $DR --describe --group orders-processor | head -6

# 2. No lag between source and target for that group (or bounded)
echo "source lag:"
$KAFKA/kafka-consumer-groups.sh --bootstrap-server $SRC --describe --group orders-processor | \
  awk 'NR>2 && $6!="-" {print "  partition "$3": lag "$6}'
echo "target lag:"
$KAFKA/kafka-consumer-groups.sh --bootstrap-server $DR --describe --group orders-processor | \
  awk 'NR>2 && $6!="-" {print "  partition "$3": lag "$6}'

# 3. Checkpoint records exist for our group
$KAFKA/kafka-console-consumer.sh --bootstrap-server $DR \
  --topic primary.checkpoints.internal --from-beginning --timeout-ms 3000 \
  --formatter org.apache.kafka.connect.mirror.formatters.CheckpointFormatter 2>/dev/null | \
  grep -c 'orders-processor'
# Expected: >= 1
```

## Takeaway

> **Offsets don't survive replication automatically. `sync.group.offsets.enabled=true` makes them survive; never run the same group on both clusters simultaneously.**

## Instructor notes
- Ask before Step 3: *"If we just point the target consumer at `primary.orderevents-2025` without any offset translation, what happens?"* Trainees split — some say "starts from earliest", some "starts from latest". The answer is "depends on `auto.offset.reset`" — either way, wrong.
- The most memorable moment is Step 4 — showing MM2 refuse to overwrite when the group is active. This is the safety belt that prevents split-brain. Point at the log line.
- Real-world story: an on-call engineer failed over consumers manually, forgot to stop the source-side ones, ended up with duplicate processing that took 48 hours to reconcile. This scenario's Step 4 is that story rehearsed.
- Bridge to Lab 10: *"You now have data everywhere. But some of it is very old — do you really need 30 days of it on hot disk? Tiered storage says no."*

## Teardown

```bash
cd Lab-09-Cross-Cluster-Replication && ./mm2.sh stop && cd ..
cd kafka-lab-dr && ./cluster-dr.sh stop && cd ..

$KAFKA/kafka-topics.sh --bootstrap-server $SRC --delete --topic orderevents-2025
$KAFKA/kafka-consumer-groups.sh --bootstrap-server $SRC --delete --group orders-processor 2>/dev/null
```
