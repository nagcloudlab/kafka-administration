# Kafka Consumer Client — administrator teaching sequence

This lesson is for Kafka administrators who do not write application code. Use one normal consumer file as a visual reference; explain what Kafka is doing rather than teaching Java syntax.

- Code on screen: [`KafkaConsumerClient.java`](KafkaConsumerClient.java)
- Topic consumed: `transactions` (created and populated by the producer lesson)
- One run command:

```bash
cd kafka-clients
mvn -q compile exec:java -Dexec.mainClass=com.example.consumer.KafkaConsumerClient
```

The consumer runs continuously. Stop it with `Ctrl+C`. There are no menus or pauses.

## Before class

The three brokers must be running on `9092`, `9093`, and `9094`. The topic should have three partitions and three replicas:

```bash
./kafka-lab/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic transactions \
  --partitions 3 \
  --replication-factor 3
```

Produce some records using `KafkaProducerClient` or the console producer before starting the consumer.

Keep these two administrator commands ready in another terminal:

```bash
./kafka-lab/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic transactions

./kafka-lab/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group payments-reporting-group
```

## How to present the file

Open `KafkaConsumerClient.java` and move from section 1 through section 6. For every setting, use this pattern:

1. What does the consumer ask Kafka to do?
2. What happens when the consumer or broker fails?
3. What should an administrator monitor?
4. Which client, broker, or topic setting is coupled to it?

Temporary edits below are classroom experiments. Restore the baseline after each experiment.

---

## 1. Connection, client identity, and group identity

Point at:

```java
bootstrap.servers
group.id
client.id
allow.auto.create.topics
```

Explain in plain language:

- `bootstrap.servers` is an entry door, not a permanent routing list. After connecting, the client learns the cluster and partition leaders.
- `client.id` identifies one process in metrics, logs, and client quotas.
- `group.id` identifies the logical consuming application. Its committed offsets are the application's recovery checkpoints.
- Consumers with the same group ID cooperate and divide partitions.
- Consumers with different group IDs each receive their own logical copy of the topic.
- `allow.auto.create.topics=false` prevents a spelling mistake from creating a badly configured production topic.

Teaching demonstration:

1. Run one consumer. It receives all three partitions.
2. Run the same command in a second terminal. Both use `payments-reporting-group`, so the partitions move between them.
3. Temporarily change `group.id` in the second copy to `payments-audit-group`. It now reads independently instead of sharing.

Administrator lesson:

- Three partitions allow at most three active consumers in one group. A fourth member is idle.
- More replicas do not increase consumer parallelism. Replicas provide broker fault tolerance; partitions provide read parallelism.
- Changing a production `group.id` creates a new logical application and may replay or skip retained data according to `auto.offset.reset`.

Watch with:

```bash
./kafka-lab/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group payments-reporting-group \
  --members --verbose
```

---

## 2. Deserialization and poison records

Point at:

```java
key.deserializer
value.deserializer
```

Explain:

- Kafka brokers store byte arrays. They do not know that a value is JSON, Avro, Protobuf, or a Java string.
- The consumer deserializer must understand the producer's wire format.
- A bad record can make `poll()` fail repeatedly at the same partition and offset. This is often called a poison record.
- Restarting the consumer does not repair incompatible bytes.

Teaching change:

Temporarily replace the value deserializer with an incompatible deserializer. Show that the failure occurs on the consumer, while the broker and topic remain healthy. Restore `StringDeserializer` afterward.

Administrator lesson:

- Ask application teams for the format, schema-compatibility policy, and poison-record procedure.
- Common operational choices are fail and alert, send the raw record to a dead-letter topic, or skip under an approved procedure.
- Skipping an offset is data loss from the application's viewpoint and must be auditable.

---

## 3. Subscribe, assignment, poll, and record identity

Point at:

```java
consumer.subscribe(List.of(TOPIC), ...)
consumer.poll(Duration.ofSeconds(1))
record.topic()
record.partition()
record.offset()
```

Explain:

- `subscribe()` says which topic the group wants. It does not immediately assign partitions.
- `poll()` discovers the coordinator, joins the group, receives assignments, fetches records, and maintains client progress.
- A poll returns a batch, including records from multiple partitions.
- An offset belongs to one partition. Offset 10 in partition 0 is unrelated to offset 10 in partition 1.
- Ordering exists inside one partition, not across the entire topic.

Administrator lesson:

- If processing stops calling `poll()` for too long, Kafka removes the member even if the process is still running.
- The record key usually controls partition placement. Stable keys preserve ordering for the same business entity.
- `assign()` would bypass group-managed assignment; it is useful for special tools and controlled replay, but the application then owns failover and partition distribution.

---

## 4. Consumer groups and rebalances

Point at:

```java
partition.assignment.strategy
LoggingRebalanceListener
onPartitionsRevoked
onPartitionsAssigned
onPartitionsLost
```

Explain the lifecycle:

- `ASSIGNED`: this member may start processing those partitions.
- `REVOKED`: an orderly rebalance is transferring ownership; finish or checkpoint work.
- `LOST`: ownership is already gone, commonly after a timeout; do not commit as though the member still owns the partitions.

Teaching demonstration:

1. Start consumer A and observe `ASSIGNED` for all partitions.
2. Start consumer B with the same group. Observe revocation and new assignments.
3. Stop B cleanly. A eventually receives the partitions again.
4. Kill B abruptly and compare detection time with a clean shutdown.

Assignor change:

Baseline:

```java
"org.apache.kafka.clients.consumer.RangeAssignor"
```

Temporary comparison:

```java
"org.apache.kafka.clients.consumer.CooperativeStickyAssignor"
```

Explain that cooperative assignment transfers only partitions that must move, reducing stop-the-world disruption. An assignor migration must be compatible across old and new application versions during rolling deployment.

Administrator lesson:

- Rebalance storms reduce throughput and create unstable lag.
- Common causes are crashes, poll deadline violations, network instability, autoscaling churn, and deployments.
- More consumers are not always better; membership churn can cost more than the added capacity.

---

## 5. Position, committed offset, and lag

Use these exact definitions:

| Term | Meaning |
|---|---|
| Current position | Next offset this live consumer will fetch. |
| Committed offset | Next offset the group should use after recovery. |
| Log-end offset | Next offset that would be appended to the partition. |
| Lag | Log-end offset minus consumer position or committed offset. |

Point at `printPositionLag()` in the code. It calculates live position lag. The admin CLI normally reports group lag from committed offsets.

Important example:

```text
partition 0 committed offset = 25
```

This means “resume by reading offset 25.” It does not mean offset 25 has already been processed.

Administrator diagnosis sequence:

1. Is input rate greater than processing rate?
2. Is one partition hot while the others are caught up?
3. Is a database, API, or storage dependency slow?
4. Is the group repeatedly rebalancing?
5. Are records failing and retrying?
6. Are commits infrequent while live processing is actually healthy?
7. Is `read_committed` waiting behind an open transaction?

Do not alert only on one absolute lag number. Use sustained lag growth, time lag, processing rate, errors, and rebalance frequency together.

---

## 6. Manual commit and delivery guarantees

Point at:

```java
enable.auto.commit=false
process(record)
new OffsetAndMetadata(record.offset() + 1)
consumer.commitSync(processedOffsets)
```

The order in the code is:

```text
poll → process → commit next offset
```

That produces at-least-once delivery. If the process crashes after business processing but before the commit succeeds, Kafka sends the record again.

Compare the guarantees:

| Order | Crash result | Guarantee |
|---|---|---|
| Commit, then process | Record can be skipped | At-most-once |
| Process, then commit | Record can repeat | At-least-once |
| Kafka transaction commits output records and consumed offsets together | Atomic Kafka read-process-write | Exactly-once within Kafka |

Teaching change:

Change `enable.auto.commit` to `true` and remove/comment the explicit `commitSync()` block for the experiment. Explain:

- Auto commit runs as part of later polls.
- It knows which offsets were returned, not whether database or business processing succeeded.
- `auto.commit.interval.ms` controls commit eligibility; it is not an independent background confirmation of completed work.

Restore manual commit after the explanation.

Administrator lesson:

- Duplicates are expected in at-least-once systems. Applications must make side effects idempotent or deduplicate.
- Kafka cannot atomically commit a normal external database transaction and a Kafka group offset.
- `commitSync()` blocks and reports errors. `commitAsync()` avoids blocking but callbacks can complete out of order.

---

## 7. New groups, missing offsets, and replay

Point at:

```java
auto.offset.reset=earliest
```

This property is used only when no valid committed offset exists, such as:

- A brand-new group
- An inactive group's offsets expired
- The committed offset is older than the topic's retained log

Compare values:

| Value | Behavior without a valid commit |
|---|---|
| `earliest` | Start at the earliest record still retained. |
| `latest` | Start after records already present. |
| `none` | Fail and require an explicit decision. |

Teaching change:

Use a new `group.id` for each run and rotate `earliest`, `latest`, and `none`. If you reuse a group with valid commits, the setting is ignored—this is the key lesson.

Admin offset-reset procedure:

1. Stop every member of the group.
2. Capture current offsets.
3. Run the reset with `--dry-run`.
4. Validate target offsets and retained timestamps.
5. Execute the reset.
6. Restart and monitor duplicates, lag, and downstream load.

Example:

```bash
./kafka-lab/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group payments-reporting-group \
  --topic transactions \
  --reset-offsets --to-earliest --dry-run
```

Topic retention must exceed the longest realistic consumer outage and recovery period. Otherwise the consumer may recover its committed number but discover the corresponding records no longer exist.

---

## 8. Heartbeats, session timeout, and poll timeout

Point at:

```java
session.timeout.ms=10000
heartbeat.interval.ms=3000
max.poll.interval.ms=300000
max.poll.records=100
```

There are two different failure detectors:

- Session timeout: the group coordinator stopped receiving valid heartbeats.
- Max poll interval: application code stopped calling `poll()` often enough.

Teaching change:

1. Change `max.poll.interval.ms` to `5000`.
2. Add `Thread.sleep(8000)` inside `process()`.
3. Run two consumers in the same group.
4. Observe the slow member lose its assignment even though its process is alive.

Restore the code afterward.

Administrator tuning order:

1. Measure worst-case processing time per record.
2. Bound work per poll with `max.poll.records`.
3. Fix blocking or unbounded dependencies.
4. Increase `max.poll.interval.ms` only when valid worst-case processing requires it.

The broker constrains valid session timeouts using group minimum and maximum settings. A client cannot bypass those broker guardrails.

---

## 9. Static membership

Point at the commented `group.instance.id` line.

Explain:

- A normal consumer receives a temporary member identity each time it joins.
- Static membership gives a process a stable identity so a brief restart can avoid unnecessary partition movement.
- Every live instance must have a unique stable ID.
- Starting a second process with the same ID fences the older member.

Teaching change:

1. Uncomment `group.instance.id`.
2. Give each terminal a different value, such as `payments-consumer-instance-1` and `-2`.
3. Restart one member within the session timeout and observe reduced churn.

Do not copy a single static ID into a deployment template used by every replica.

---

## 10. Fetch batching, throughput, latency, and memory

Point at:

```java
max.poll.records
fetch.min.bytes
fetch.max.wait.ms
max.partition.fetch.bytes
fetch.max.bytes
```

Explain the relationships:

- `max.poll.records` limits record count returned to application code; it does not limit fetched bytes already buffered by the client.
- `fetch.min.bytes` lets the broker wait for a larger batch, improving throughput at the cost of latency.
- `fetch.max.wait.ms` caps that broker wait.
- `max.partition.fetch.bytes` must allow the largest legal record from one partition.
- `fetch.max.bytes` is the soft overall response limit across partitions.

Teaching change for throughput versus latency:

| Run | `fetch.min.bytes` | `fetch.max.wait.ms` | Expected behavior |
|---|---:|---:|---|
| Latency | 1 | 100 | Small, fast responses |
| Throughput | 65536 | 2000 | Larger batches, more waiting at low traffic |

Administrator lesson:

- Consumer memory grows with consumer count, assigned partitions, fetch sizes, and in-flight responses.
- Producer `max.request.size`, broker/topic maximum message size, replica fetch size, and consumer partition fetch size must support the same largest record.
- Raising only one limit moves the failure elsewhere.

---

## 11. Transaction visibility

Point at:

```java
isolation.level=read_committed
```

Explain:

- `read_uncommitted` reads committed and aborted transactional data.
- `read_committed` hides aborted records and does not read past the last stable offset while a transaction remains open.
- Aborted records still exist physically in the log with transaction control markers.
- A long-running open transaction can therefore look like consumer lag.
- Isolation level does not provide exactly-once behavior for external databases or APIs.

Teaching change:

Use the producer transaction scenario to write committed and aborted records. Run once with `read_committed`, then once with `read_uncommitted`, using a fresh group ID for each comparison.

Administrator lesson:

- Confirm upstream actually uses transactions before requiring `read_committed`.
- Monitor transaction coordinator health and long-running transactions.

---

## 12. Graceful shutdown

Point at:

```java
consumer.wakeup()
catch (WakeupException ...)
consumer.close(...)
```

Explain:

- `KafkaConsumer` is not thread-safe. Another thread should not call normal consumer methods.
- `wakeup()` is the supported exception: it interrupts a blocking poll so the owning thread can close.
- Clean close leaves the group immediately. A crashed process is removed only after failure detection.
- Faster clean deployment means shorter rebalances and less lag disturbance.

Press `Ctrl+C` and show `Consumer closed cleanly.` Then compare with a forced process kill.

---

## Administrator change-review checklist

| Application change | Evidence to request |
|---|---|
| New `group.id` | Intended reset behavior and retained-data impact |
| Auto commit enabled | Proof offsets cannot move ahead of completed work |
| Offset reset policy changed | New-group and expired-offset recovery requirement |
| Larger `max.poll.records` | Worst-case batch time remains below poll interval |
| Larger poll interval | Measured legitimate processing time, not a hidden hang |
| Session/heartbeat change | Broker bounds and failure-detection objective |
| Assignor change | Rolling compatibility and migration plan |
| Static membership | Unique stable IDs and replacement procedure |
| Fetch limit increase | Largest-record need and heap calculation |
| `read_committed` | Transactional upstream and open-transaction monitoring |

## Final mental model for students

- A topic contains partitions; each partition is an ordered log.
- A group divides partitions among its members.
- An offset is a partition-local position.
- A committed offset is a recovery checkpoint owned by the group.
- Poll keeps the member alive and delivers batches.
- Processing and commit order determine duplicates or loss.
- Rebalances move partition ownership.
- Lag is a symptom that must be correlated with rates, errors, skew, and rebalances.
- Topic retention determines how far back recovery can go.
- Consumer configuration is always coupled to topic design, broker limits, and application processing time.
