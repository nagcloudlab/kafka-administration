# Scenario 12.4 — Message size limits &amp; auto-create traps

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

### Part A · Message size — the three-way handshake

- **Kafka enforces record size at multiple layers.** A record must survive each check to be written and read.
- **Producer side** — `max.request.size` (default **1 MB**). Client-side cap. A record larger than this is rejected on `send()` with `RecordTooLargeException` before hitting the network.
- **Broker side** — `message.max.bytes` (default **1 MB, in older versions**; **1 MiB = 1048588 bytes** in current versions to accommodate protocol overhead). Broker rejects the produce request with `MESSAGE_TOO_LARGE` error code if the batch exceeds this.
- **Topic side** — `max.message.bytes` (default = broker's `message.max.bytes`). Per-topic override. **Strictest wins on the wire.**
- **Replication** — `replica.fetch.max.bytes` on the broker (default **1 MiB**). If followers can't fetch a batch this big, they fall behind → ISR shrinks → possible under-min-ISR. **Must be ≥ topic's `max.message.bytes`.**
- **Consumer side** — `fetch.max.bytes` (default 50 MB) and `max.partition.fetch.bytes` (default 1 MB). Consumer must be able to fetch what the broker stored. If broker allows 10 MB records but consumer's `max.partition.fetch.bytes=1MB`, consumer gets stuck at the first oversized record.

- **The correct sizing rule** — if you want to allow 10 MB messages:
  ```
  producer.max.request.size       ≥ 10 MB
  broker.message.max.bytes        ≥ 10 MB    (cluster-wide default)
  topic.max.message.bytes         ≥ 10 MB    (per-topic override, if applicable)
  broker.replica.fetch.max.bytes  ≥ 10 MB    (else replication breaks)
  consumer.max.partition.fetch.bytes ≥ 10 MB (else consumer stalls)
  ```
  **All five, or the smallest wins.**

- **Common misconfiguration cascade**:
  1. App team asks "can we send 5 MB messages?" — admin sets `topic.max.message.bytes=5MB` on the topic.
  2. Producer errors: `RecordTooLargeException` (producer still has default 1 MB `max.request.size`). Admin sets producer config.
  3. Producer works; broker accepts. But followers can't replicate — `replica.fetch.max.bytes` still 1 MB. ISR shrinks; under-min-ISR alerts fire.
  4. Admin bumps `replica.fetch.max.bytes` cluster-wide, rolling restart.
  5. Consumers stall at first big message — `max.partition.fetch.bytes` still 1 MB.
  6. Consumer team updates configs.
  7. Same admin's OTHER topic (using default settings) starts producing 3 MB messages — cluster-wide replication throughput drops because followers now fetch huge batches. Latency SLO breach.

  **Every one of these is a separate incident that could have been avoided by treating "message size" as a cross-cutting design decision, not a per-topic override.**

### Part B · Auto-create trap

- **Kafka can create topics automatically when a producer publishes to a non-existent topic, or when a consumer subscribes to one.** Controlled by broker `auto.create.topics.enable` (default: **true** in most 3.x versions).
- **What auto-created topics use for defaults**:
  - Partitions: `num.partitions` (default **1**)
  - Replication factor: `default.replication.factor` (default **1** — *not 3*)
  - Retention: cluster-wide default (`log.retention.ms`)
  - Everything else: broker defaults
- **Why this is a trap**:
  - **RF=1** = no replication, no fault tolerance. Silent single-broker dependency.
  - **1 partition** = 1 consumer maximum in the group; no parallelism.
  - **Wrong topic name typo** = a new topic with 1 partition, RF=1 that shadows the real one. Data goes there silently.
  - **Compliance nightmare** — you have topics you didn't approve, with configs you didn't review.
- **Even with `auto.create.topics.enable=false`, some clients still can trigger creation** via the `CreateTopicsRequest` API. ACLs on `CreateTopics` are the real defense.

## Symptom

Message-size:
- Producer log: `RecordTooLargeException: The message is 1234567 bytes when serialized which is larger than the maximum request size you have configured with the max.request.size configuration.`
- Broker log: `The request included message batch larger than the configured segment size on the server` — `MESSAGE_TOO_LARGE` returned to producer.
- Follower log: `Fetcher ... expected offset X but got batch size Y exceeding fetcher limit` — replication stuck.
- Consumer stalled: `Received oversized message from partition X of topic Y — increase max.partition.fetch.bytes`.

Auto-create:
- Cluster suddenly has 100 topics no one recognizes → someone deployed a new service with default configs.
- `kafka-topics --describe --topic <newtopic>` shows `PartitionCount: 1 ReplicationFactor: 1` — auto-created.
- Data-loss incident post-mortem reveals critical events went to `order.event` (typo) instead of `orders.events` — auto-created shadow topic.

## Setup — 3 terminals

**Terminal 1 (control):**

```bash
# Confirm current broker defaults for message size (should be ~1 MB)
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type brokers --entity-default --describe 2>&1 | grep -E 'message.max|replica.fetch'

# Check current auto-create setting cluster-wide
grep -h auto.create.topics.enable config/broker-*.properties 2>/dev/null || \
  echo "not explicitly set in server.properties — using default (true)"
```

**Terminal 2 (broker 101 log tail — for size errors):**

```bash
tail -F logs/broker-101.log | grep --line-buffered -E 'MESSAGE_TOO_LARGE|Fetcher|Auto-created'
```

**Terminal 3 (topic count monitor):**

```bash
watch -n 3 "echo -n 'total topics: '; $KAFKA/kafka-topics.sh --bootstrap-server $BS --list | wc -l; \
  echo 'recent topics:'; $KAFKA/kafka-topics.sh --bootstrap-server $BS --list | grep -v '^__' | tail -10"
```

## Trigger — Part A: message-size cascade

### Step A1: create a topic with default limits, try to send 2 MB

**Terminal 1:**

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --create --topic bigmsg-demo --partitions 3 --replication-factor 3

# Build a 2 MB payload and try to send it (default limits are ~1 MB — this should fail)
head -c $((2 * 1024 * 1024)) /dev/urandom | base64 > /tmp/2mb.txt
ls -l /tmp/2mb.txt   # 2.7 MB after base64

$KAFKA/kafka-console-producer.sh --bootstrap-server $BS --topic bigmsg-demo < /tmp/2mb.txt 2>&1 | tail -8
```

Expected: `RecordTooLargeException: The message is 2796204 bytes when serialized which is larger than 1048576, which is the value of the max.request.size configuration.`

The **producer-side** limit rejected the message before it reached the broker.

### Step A2: bump the producer's `max.request.size` — now the BROKER rejects

```bash
$KAFKA/kafka-console-producer.sh --bootstrap-server $BS --topic bigmsg-demo \
  --producer-property max.request.size=10485760 < /tmp/2mb.txt 2>&1 | tail -8
```

Expected: `MessageSizeTooLargeException: The message is 2796204 bytes when serialized which is larger than the maximum server allows.`

Producer got past its own limit, broker rejected. **Terminal 2** shows the broker-side error line.

### Step A3: bump topic's `max.message.bytes` — now REPLICATION breaks

```bash
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type topics --entity-name bigmsg-demo \
  --alter --add-config max.message.bytes=10485760

# Try again with the producer bump AND the topic override
$KAFKA/kafka-console-producer.sh --bootstrap-server $BS --topic bigmsg-demo \
  --producer-property max.request.size=10485760 < /tmp/2mb.txt

echo "produce succeeded — but check ISR"
sleep 5
$KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic bigmsg-demo | tail -3
```

Expected: the produce **succeeds** (silently — console producer doesn't print on success). But `--describe` may show ISR shrinking on the partition that received the record — followers can't fetch a 2.7 MB batch because their `replica.fetch.max.bytes` is still 1 MB.

**Terminal 2** shows follower fetch errors.

### Step A4: fix `replica.fetch.max.bytes` cluster-wide

```bash
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type brokers --entity-default \
  --alter --add-config replica.fetch.max.bytes=10485760

# Wait for followers to catch up — ISR should recover
sleep 10
$KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic bigmsg-demo | tail -3

# Producer AND replication OK now
head -c $((3 * 1024 * 1024)) /dev/urandom | base64 > /tmp/3mb.txt
$KAFKA/kafka-console-producer.sh --bootstrap-server $BS --topic bigmsg-demo \
  --producer-property max.request.size=10485760 < /tmp/3mb.txt

sleep 3
$KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic bigmsg-demo | tail -3
# ISR should stay complete this time
```

### Step A5: consume the large records — CONSUMER breaks next

```bash
# Default consumer max.partition.fetch.bytes = 1 MB, our records are 2-3 MB — expect stall
$KAFKA/kafka-console-consumer.sh --bootstrap-server $BS \
  --topic bigmsg-demo --from-beginning --timeout-ms 5000 2>&1 | tail -6
```

Expected either a "no messages" (nothing returned in timeout) or `RecordTooLargeException` on the consumer side.

Fix consumer side:

```bash
$KAFKA/kafka-console-consumer.sh --bootstrap-server $BS \
  --topic bigmsg-demo --from-beginning --timeout-ms 5000 \
  --consumer-property max.partition.fetch.bytes=10485760 \
  --consumer-property fetch.max.bytes=52428800 2>&1 | head -3
# Now records return successfully
```

## Trigger — Part B: auto-create trap

### Step B1: confirm auto-create is currently enabled

```bash
# If the setting isn't in broker-*.properties, the DEFAULT is true.
# For 3.9.0, check via kafka-configs:
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type brokers --entity-name 101 --describe --all 2>&1 | \
  grep auto.create.topics.enable
```

If `auto.create.topics.enable=true`, we can demonstrate the trap. If the training cluster has it disabled (Lab-01 defaults left it on), proceed.

### Step B2: produce to a NEW topic that doesn't exist

**Terminal 1:**

```bash
# BEFORE — this topic doesn't exist yet
$KAFKA/kafka-topics.sh --bootstrap-server $BS --list | grep autocreated || \
  echo "no autocreated-* topics yet"

# Produce to a "typo" topic — imagine the app was supposed to send to "orders.events"
# but the config has "order.events" (missing 's')
echo "oops-typo-1" | $KAFKA/kafka-console-producer.sh --bootstrap-server $BS \
  --topic autocreated-typo-topic 2>&1 | tail -3

sleep 2
```

**Terminal 3** — a new topic appeared: `autocreated-typo-topic`.

### Step B3: inspect what the auto-created topic looks like

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic autocreated-typo-topic
```

Expected:
```
Topic: autocreated-typo-topic  PartitionCount: 1  ReplicationFactor: 1
  Topic: autocreated-typo-topic  Partition: 0  Leader: 102  Replicas: 102  Isr: 102
```

Note:
- **1 partition** — no consumer parallelism.
- **RF=1** — single broker. If broker 102 dies, this topic is gone until 102 comes back. No fault tolerance.
- Everything else = broker default retention (probably 7 days), no compaction, etc.

**Compare** with a topic you'd have created properly:

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --create --topic properly-created --partitions 6 --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=86400000

$KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic properly-created
```

RF=3, min.ISR=2, explicit retention. Zero of that on the auto-created one.

### Step B4: disable auto-create (the fix)

```bash
# The safe production setting. Requires broker restart because it's read-only.
# For this demo we'll set it dynamically per-broker as much as we can.
# NOTE: auto.create.topics.enable is a READ-ONLY config in Kafka 3.x — needs restart.

# Add to each broker's server.properties:
for id in 101 102 103; do
  grep -q '^auto.create.topics.enable' config/broker-$id.properties && \
    sed -i 's/^auto.create.topics.enable=.*/auto.create.topics.enable=false/' config/broker-$id.properties || \
    echo 'auto.create.topics.enable=false' >> config/broker-$id.properties
done

# Rolling restart to pick up
for id in 101 102 103; do
  ./cluster.sh restart-broker-monitoring $id
  sleep 5
done
```

### Step B5: prove producing to a non-existent topic now fails

```bash
# Try to produce to another non-existent topic — should NOT auto-create
echo "should-fail" | $KAFKA/kafka-console-producer.sh --bootstrap-server $BS \
  --topic no-such-topic 2>&1 | head -6
```

Expected: `UNKNOWN_TOPIC_OR_PARTITION` errors, produce fails, and:

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS --list | grep no-such-topic || \
  echo "no such topic exists — auto-create is DISABLED, as intended"
```

## Solution

### Message size — the discipline

- **Treat message size as a cluster-wide design decision**, not a per-topic override.
- **Decide the cluster's max supported size** (say, 4 MB) and set consistently:
  ```properties
  # In every broker-*.properties:
  message.max.bytes=4194304
  replica.fetch.max.bytes=4194304
  ```
  Then document to app teams that individual records must fit in 4 MB. For anything bigger, use object storage (S3/GCS) with a Kafka message that just holds the reference.

- **Producer / consumer defaults** should match:
  ```properties
  # producer
  max.request.size=4194304
  # consumer
  max.partition.fetch.bytes=4194304
  fetch.max.bytes=52428800     # can be higher — fetches multiple batches
  ```
  Ship these as your team's standard producer / consumer config templates.

- **Never let a single topic's `max.message.bytes` exceed the cluster's `replica.fetch.max.bytes`.** That's the ISR-shrink trap.

- **When the request comes ("we need to send 100 MB messages")**:
  1. Ask if it's really needed. Usually no — the payload has structured data that could be sent as multiple smaller records.
  2. If genuinely needed (e.g., ML model weights, image blobs): use S3 + reference pattern.
  3. If ABSOLUTELY MUST be in Kafka (rare): bump everything (broker, replication, topic, producer, consumer) — 5 config changes, all rolled out simultaneously. Communicate the throughput impact (big messages spike network, disk, and cache).

### Auto-create — the discipline

- **Disable in production. Full stop.**
  ```properties
  # broker-*.properties
  auto.create.topics.enable=false
  ```
  Requires broker restart (`auto.create.topics.enable` is a `read-only` config).

- **Also**:
  ```properties
  # broker-*.properties
  delete.topic.enable=true              # default true — leave it, so you CAN delete when needed
  ```
  Some teams turn off deletion too as a safety measure, but that ties one hand behind your back for legitimate lifecycle ops.

- **ACL enforcement** is the second layer of defense:
  ```bash
  # Only admins can create topics
  $KAFKA/kafka-acls.sh --bootstrap-server $BS --add \
    --allow-principal User:admin \
    --operation Create --cluster

  # Everyone else — apps get read/write on existing topics only
  # (see Lab-07.2 for full ACL patterns)
  ```

- **Topic creation runbook** (referenced in slide deck's Section 5):
  1. App team files ticket with topic name, partitions, RF, retention, owner.
  2. Admin reviews against naming standard.
  3. Admin creates with explicit configs (never bare `--create --topic X`).
  4. Add topic-owner label to your CMDB or monitoring config.
  5. Add topic to Grafana dashboard(s).
  6. Grant app's ACL(s).
  7. App team confirms produce/consume works.

- **Detect drift** — the Step 7 audit script from Sc 12.2 catches manually-created or auto-created topics. Run it weekly.

## Verify

```bash
# 1. Auto-create is disabled
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type brokers --entity-name 101 --describe --all 2>&1 | \
  grep auto.create.topics.enable
# Expected: shows "false"

# 2. Producing to a non-existent topic fails
echo "test" | $KAFKA/kafka-console-producer.sh --bootstrap-server $BS \
  --topic definitely-does-not-exist 2>&1 | grep -o 'UNKNOWN_TOPIC_OR_PARTITION'
# Expected: UNKNOWN_TOPIC_OR_PARTITION

# 3. All three size-layer values match
grep -h 'message.max.bytes\|replica.fetch.max.bytes' config/broker-*.properties 2>/dev/null | sort -u
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type topics --entity-name bigmsg-demo --describe 2>&1 | grep max.message.bytes

# 4. ISR healthy on the bigmsg-demo topic
$KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic bigmsg-demo | tail -3
```

## Takeaway

> **Message size is a five-way handshake — producer, broker, topic, replica-fetch, consumer must ALL be sized to the same ceiling. Auto-create is convenient in dev and radioactive in prod — turn it off, enforce with ACLs, provision through automation.**

## Instructor notes
- Show `/tmp/2mb.txt` (`ls -l`) before running Step A1 — 2.7 MB is bigger than default 1 MB by design; students see the numeric fit before the exception fires.
- After each step in Part A, the errror message CHANGES to point at a different layer (`RecordTooLargeException` (client) → `MESSAGE_TOO_LARGE` (broker) → ISR shrink (replication) → consumer stall). Read each error out loud so trainees learn to distinguish them.
- Part B's `autocreated-typo-topic` name is the entire scenario in one line. Real incidents are usually caused by a single misconfigured environment variable in a canary deploy.
- Real-world story: a customer had `auto.create.topics.enable=true` on their production cluster. A staging service was accidentally deployed with the prod bootstrap servers. Within an hour, 40 new topics existed with RF=1. When one of those RF=1 topics' hosting broker was rebooted (routine patching), 3 hours of data was permanently lost. Point at the `RF=1` line in Step B3.
- Bridge back to whole chapter: *"You've now seen topic anatomy, config precedence, partition growth, size limits, and auto-create traps. Every one of these came from a real incident. The slide deck's checklist is your take-home artifact."*

## Teardown

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic bigmsg-demo
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic properly-created
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic autocreated-typo-topic 2>/dev/null

# Revert broker-level replica.fetch.max.bytes if you don't want to keep it high
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type brokers --entity-default \
  --alter --delete-config replica.fetch.max.bytes 2>/dev/null

# Auto-create is best LEFT DISABLED — this is the production posture.
# If you want to revert for other demos:
# for id in 101 102 103; do
#   sed -i 's/^auto.create.topics.enable=false/auto.create.topics.enable=true/' config/broker-$id.properties
# done
# then rolling restart

rm -f /tmp/2mb.txt /tmp/3mb.txt
```
