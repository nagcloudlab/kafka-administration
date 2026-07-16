# Scenario 5.1 — Retention vs compaction (what stays on disk, and when)

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

**One-time cluster tuning (so retention/compaction happens fast enough to demo):**

```bash
# Speed up retention checks and cleaner loop cluster-wide.
# Defaults are 5 min / 15 s; too slow for a live demo.
for id in 101 102 103; do
  grep -q '^log.retention.check.interval.ms' config/broker-$id.properties \
    || echo 'log.retention.check.interval.ms=10000' >> config/broker-$id.properties
  grep -q '^log.cleaner.backoff.ms' config/broker-$id.properties \
    || echo 'log.cleaner.backoff.ms=5000' >> config/broker-$id.properties
done

# Rolling restart (graceful, from 1.1) so brokers pick up the change
for id in 101 102 103; do
  ./cluster.sh stop-broker $id
  sleep 2
  ./cluster.sh start-broker $id
  sleep 5
done
```

---

## Problem

- **Two cleanup policies, different jobs:**
  - **`cleanup.policy=delete`** (default) — time- or size-based retention. Old **segments** are deleted wholesale when their newest message is older than `retention.ms` (or when total size exceeds `retention.bytes`).
  - **`cleanup.policy=compact`** — key-based dedup. For each key, keep only the **latest** message. Older values for the same key are eligible for removal by the log cleaner. Requires **keyed** messages.
  - **`cleanup.policy=delete,compact`** — both. Compact keeps latest-per-key; delete then removes very old segments regardless. Useful for state topics that need a long history but not forever.

- **Common misconceptions:**
  - *"`retention.ms=60000` deletes messages older than 60 s."* — No. It deletes old **segments** whose newest message is > 60 s old. The **currently-active segment is never eligible**, no matter how old its oldest message is. That's why you need `segment.ms` too — force segments to roll frequently, so retention has something to delete.
  - *"Compact removes duplicates."* — No. It keeps only the **latest** value per key. If you write `k=v1, k=v2, k=v3`, compaction eventually removes `v1` and `v2`; `v3` stays forever (until you write another `k=X` or a null-value tombstone).
  - *"Null-keyed messages get compacted."* — No. Compaction skips them; they live forever. This is the most common footgun.
  - *"Compaction happens immediately."* — No. Cleaner scans periodically. Only scans **rolled** (non-active) segments. Only runs when `dirty-portion / total-log >= log.cleaner.min.cleanable.ratio` (default 0.5).

- **Log cleaner internals (per broker):**
  - Configurable via `log.cleaner.*` broker properties.
  - `log.cleaner.enable=true` (default) — turn off and compaction stops entirely; often what happens on tiny lab brokers.
  - `log.cleaner.threads=1` (default) — one thread cleans all compacted partitions. Bumps to 2–4 on busy clusters with many compacted topics.
  - `log.cleaner.min.cleanable.ratio=0.5` (default) — cleaner ignores partitions whose dirty portion is under this ratio. Set to 0.01 on demo topics to force aggressive compaction.
  - `log.cleaner.min.compaction.lag.ms=0` (default) — messages younger than this can't be compacted (protects readers who want a short "history" window).

- **Tombstones (compaction only):**
  - Publish a message with a non-null key and **null value** → cleaner treats it as a deletion marker for that key.
  - Tombstones themselves stick around for `delete.retention.ms` (default 24 h) so consumers have time to see them before they vanish.

## Symptom
- **Delete misconfigured:** disk fills up despite `retention.ms=<small>`. Root cause: `segment.ms` / `segment.bytes` too big → active segment never rolls → old data can't be evicted.
- **Compact misconfigured:** same key produced 1M times, but consumer sees all 1M values. Root cause: messages don't have keys (null key = skipped by cleaner), or cleaner is disabled, or `min.cleanable.dirty.ratio` too high.
- **Cleaner stopped:** log line `Halting because log cleaner is dead` on a broker. Root cause: OOM in cleaner buffer, corrupt segment, or `log.cleaner.enable=false`. Compaction stops; disk grows unbounded.

## Setup — 4 terminals

**Terminal 1 (control) — create two topics with aggressive settings:**

```bash
# DELETE topic — 60 s retention, 10 s segment roll, single partition for clarity
$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --create --topic f1-delete --partitions 1 --replication-factor 3 \
  --config cleanup.policy=delete \
  --config retention.ms=60000 \
  --config segment.ms=10000

# COMPACT topic — force cleaner to run on tiny dirty ratio, 10 s segment roll
$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --create --topic f1-compact --partitions 1 --replication-factor 3 \
  --config cleanup.policy=compact \
  --config segment.ms=10000 \
  --config min.cleanable.dirty.ratio=0.01 \
  --config delete.retention.ms=1000

# Confirm configs
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type topics --entity-name f1-delete --describe | grep -v SENSITIVE
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type topics --entity-name f1-compact --describe | grep -v SENSITIVE
```

**Terminal 2 (segment inspector — watch files appear/disappear):**

```bash
# Find the leader broker's data dir for each topic-partition
watch -n 2 "echo === f1-delete ===; \
  ls -la data/broker-*/f1-delete-0/ 2>/dev/null | grep -E '\.log$|\.index$' | head -12; \
  echo === f1-compact ===; \
  ls -la data/broker-*/f1-compact-0/ 2>/dev/null | grep -E '\.log$|\.index$' | head -12"
```

- Each segment shows up as three files: `NNNNNNNNNNNNNNNNNNNN.log` (data), `.index` (offset index), `.timeindex` (time index). Watch the sequence numbers grow (segment rolls) and older ones disappear (retention / compaction).

**Terminal 3 (producer to f1-delete — keyed):**

```bash
# Key = "a"/"b"/"c" rotating; value = incrementing counter
for i in $(seq 1 30); do
  key=$(echo -e "a\nb\nc" | sed -n "$(( (i % 3) + 1 ))p")
  echo "$key:v$i"
  sleep 3
done | $KAFKA/kafka-console-producer.sh --bootstrap-server $BS \
  --topic f1-delete \
  --property parse.key=true --property key.separator=:
```

**Terminal 4 (producer to f1-compact — same pattern):**

```bash
for i in $(seq 1 30); do
  key=$(echo -e "a\nb\nc" | sed -n "$(( (i % 3) + 1 ))p")
  echo "$key:v$i"
  sleep 3
done | $KAFKA/kafka-console-producer.sh --bootstrap-server $BS \
  --topic f1-compact \
  --property parse.key=true --property key.separator=:
```

- Each producer runs for ~90 s and sends 30 messages.
- **Total elapsed for demo:** ~2 min of production + 1–2 min wait for cleanup.

## Observe

### During production (~T + 30 s)

- **T2:** both topics have 2–3 segment files (`.log`) as `segment.ms=10000` rolls them every 10 s.
- Consume both while producers run:
  ```bash
  $KAFKA/kafka-console-consumer.sh --bootstrap-server $BS \
    --topic f1-delete --from-beginning --timeout-ms 5000 \
    --property print.key=true --property key.separator=: 2>/dev/null | sort | uniq -c
  ```
  ```bash
  $KAFKA/kafka-console-consumer.sh --bootstrap-server $BS \
    --topic f1-compact --from-beginning --timeout-ms 5000 \
    --property print.key=true --property key.separator=: 2>/dev/null | sort | uniq -c
  ```
- At this point both topics look similar — everything produced is still present. Cleanup hasn't run.

### After production finishes and 60–90 s pass

Paste again in Terminal 1:

```bash
echo "=== f1-delete (delete policy) ==="
$KAFKA/kafka-console-consumer.sh --bootstrap-server $BS \
  --topic f1-delete --from-beginning --timeout-ms 5000 \
  --property print.key=true --property key.separator=: 2>/dev/null

echo
echo "=== f1-compact (compact policy) ==="
$KAFKA/kafka-console-consumer.sh --bootstrap-server $BS \
  --topic f1-compact --from-beginning --timeout-ms 5000 \
  --property print.key=true --property key.separator=:
```

- **f1-delete:** only the last ~20 s of messages (whichever whole segments are within `retention.ms`). Earlier segments are gone from disk — confirm via T2 (fewer `.log` files, all recent).
- **f1-compact:** exactly **three lines** — one per key (`a`, `b`, `c`), showing the latest value only. All older values compacted away.

### Tombstone demo (compact-only)

```bash
# Delete key "a" by writing a null value
echo "a:" | $KAFKA/kafka-console-producer.sh --bootstrap-server $BS \
  --topic f1-compact \
  --property parse.key=true --property key.separator=: \
  --property null.marker="~"     # any non-empty value; we're testing the empty-value path

# Wait ~15 s for cleaner
sleep 15

# Consume again
$KAFKA/kafka-console-consumer.sh --bootstrap-server $BS \
  --topic f1-compact --from-beginning --timeout-ms 5000 \
  --property print.key=true --property key.separator=: 2>/dev/null
```

- After ~15 s the compacted topic will show only keys `b` and `c`; key `a` is gone.
- The tombstone message itself will vanish `delete.retention.ms` later (1 s in our config, 24 h by default).

## Solution

- **Pick the policy for the workload:**
  - **Event stream** (payments, page views, IoT readings) → `cleanup.policy=delete`, `retention.ms` in days/weeks.
  - **Materialized state** (user profile, latest price, session cache) → `cleanup.policy=compact`. **Must** have keys.
  - **State + audit trail** → `cleanup.policy=delete,compact`. Compact keeps latest per key indefinitely; delete removes very old segments after `retention.ms`.

- **Segment settings drive both:**
  ```properties
  segment.ms=604800000              # 7 days — roll segment on time
  segment.bytes=1073741824          # 1 GB  — roll segment on size (whichever hits first)
  ```
  - For delete to actually remove data, segments must roll. Too-large `segment.bytes` on a slow topic = data lives forever.
  - For compact to keep working, segments must roll — cleaner never touches the active segment.

- **Retention:**
  ```properties
  cleanup.policy=delete
  retention.ms=604800000            # 7 days
  retention.bytes=-1                # unlimited (use for time-based only)
  # OR
  retention.bytes=53687091200       # 50 GB per partition (use for size-based)
  ```
  Set one or the other; using both makes reasoning harder.

- **Compaction:**
  ```properties
  cleanup.policy=compact
  min.cleanable.dirty.ratio=0.5      # default; lower = more aggressive, more IO
  delete.retention.ms=86400000       # 24 h — how long tombstones stick around
  min.compaction.lag.ms=0            # protect a "recent history" window if consumers need it
  ```

- **Broker-level cleaner tuning:**
  ```properties
  log.cleaner.enable=true            # default — never turn off on a broker with compacted topics
  log.cleaner.threads=2              # bump for clusters with many compacted topics
  log.cleaner.dedupe.buffer.size=134217728   # 128 MB — grow if you see "buffer overflow" in cleaner log
  log.retention.check.interval.ms=300000     # 5 min default (we set it to 10 s for the demo)
  ```

- **Cleaner health = a real metric.**
  ```bash
  # If the cleaner dies, this metric stops updating
  # kafka.log:type=LogCleanerManager,name=uncleanable-partitions-count
  # kafka.log:type=LogCleanerManager,name=max-dirty-percent
  ```
  Log line to alert on: `Halting because log cleaner is dead`. When it happens, no more compaction runs anywhere on that broker.

## Verify

```bash
# 1. f1-compact ends up with exactly 3 keys (a, b, c) after cleaner runs
$KAFKA/kafka-console-consumer.sh --bootstrap-server $BS \
  --topic f1-compact --from-beginning --timeout-ms 5000 \
  --property print.key=true --property key.separator=: 2>/dev/null | \
  cut -d: -f1 | sort -u
# Expected: a, b, c (one line each)

# 2. f1-delete has fewer segments than it did at peak production
ls data/broker-101/f1-delete-0/*.log 2>/dev/null | wc -l
# Expected: 1-2 segments (older ones evicted)

# 3. Cleaner is alive and has processed our topic
grep -E 'Cleaner.*f1-compact|Compaction complete' logs/broker-101.log | tail -5
```

## Takeaway

> **Delete deletes segments (not messages). Compact keeps latest per key (requires keys). Different tools — pick per workload.**

## Instructor notes
- Poll before Step 3: *"If I set `retention.ms=1000` on a busy topic, are messages older than 1 s gone?"* Most say yes. Demo shows the segment-boundary reality: an active segment protects everything inside it.
- Draw a log on the whiteboard: active segment on the right (writing), rolled segments to the left. Retention deletes from the LEFT; compact rewrites the LEFT. Neither touches the active (rightmost) segment.
- The **null-key trap** in compact is the #1 real-world outage: someone puts a JSON payload in the value with a null key, expects compaction, disk fills up over months.
- Cleaner-death is a silent failure. Alert on `Halting because log cleaner is dead` from broker logs, plus the `max-dirty-percent` JMX going above ~90% and staying there.
- Bridge to 3.2: *"Retention/compaction manages data in place. What if we need to MOVE partitions between brokers — same data, different node?"*

## Teardown

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic f1-delete
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic f1-compact

# Leave the broker-level cleaner/retention timing changes in place —
# Scenario 5.2 doesn't need them, but they're harmless and useful for future data-lifecycle demos.
```
