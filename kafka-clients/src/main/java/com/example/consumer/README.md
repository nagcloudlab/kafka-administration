# Consumer — teaching sequence (one file, one guide)

For non-coder Kafka admins. The whole consumer track lives in **one Java file** and **one guide** (this document).

- **File on screen:** [`KafkaConsumerClient.java`](KafkaConsumerClient.java)
- **Same run command every scenario:**

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.consumer.KafkaConsumerClient
```

- Between scenarios: **the trainer edits the file** (each scenario below lists the exact lines), saves, and re-runs.
- No CLI flags to remember.

---

## For the trainer

- **Audience:** admins, not developers. Read every scenario's *What* aloud in English before any code changes.
- **Session length:** pick 4–5 scenarios per session. All 18 scenarios span ~6–8 hours.
- **Reset between scenarios:** `git checkout kafka-clients/src/main/java/com/example/consumer/KafkaConsumerClient.java` undoes all edits in one shot.
- **The admin cheat sheet at the end** is the takeaway your students will remember longest.

## For the student

- **First pass:** read this doc with `KafkaConsumerClient.java` open on screen while your trainer walks you through.
- **Later, as reference:** jump to a specific scenario by number when the topic surfaces in production (*"lag alert fired — that's scenario 12"*).
- **The Admin take-home** at the end of every scenario is the memorable summary — three to five bullets you should be able to recite for each concept.
- **Quick lookups** at the end of this doc:
  - `Reference — failure signatures` — *"my consumer is doing X — what fired?"*.
  - `Tuning cheat sheet` — throughput / latency / durability / memory levers.
  - `Quick concept reference` — poll semantics, offsets, rebalancing, health checks.
  - `Admin cheat sheet` — KNOW / WATCH / TUNE tables plus the one-liner every admin should walk away with.

---

## One-time setup

> **Before you run:**
> - The Lab-01 cluster must be up (3 brokers on `9092` / `9093` / `9094`).
> - `mvn -q -f kafka-clients/pom.xml compile` succeeds.
> - **One topic** — `transactions` — is used across every scenario. Created once (also used by the producer track).
> - **Seed data:** run the producer at least once so the topic has records to consume.

Create the topic (if not already there from the producer track):

```bash
./kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic transactions --partitions 3 --replication-factor 3
```

Seed records by running the producer client (leave it running for ~30 s, then `Ctrl+C`):

```bash
mvn -q -f kafka-clients/pom.xml compile exec:java -Dexec.mainClass=com.example.producer.KafkaProducerClient
```

Two admin CLI commands to keep ready in another terminal:

```bash
./kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic transactions
./kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group payments-reporting-group
```

> **Admin lens:** All 18 scenarios reuse the same `transactions` topic. Scenarios that need a fresh consumer group (7, 8, 9, 11) change `group.id` in code — trivially reversible. One topic keeps the mental model simple; production would use many.

---

## Consumer config quick reference

Every knob set explicitly in `KafkaConsumerClient.java`. The scenario column points to where the value is actively changed.

| Section | Knob | Default in file | Scenario that changes it |
|---|---|---|---|
| Connection | `bootstrap.servers` | 3 seed brokers | – |
| Connection | `client.id` | `payments-consumer-1` | 15 (quota) |
| Connection | `allow.auto.create.topics` | `false` | – |
| Deserialization | `key.deserializer` / `value.deserializer` | `StringDeserializer` | – |
| Group | `group.id` | `payments-reporting-group` | 2 (multi-instance), 7 / 8 / 11 (fresh group) |
| Group | `group.instance.id` | not set | 14 (static membership) |
| Group | `session.timeout.ms` | 10 s | 6 |
| Group | `heartbeat.interval.ms` | 3 s | 6 |
| Group | `max.poll.interval.ms` | 5 min | 5 |
| Fetch | `max.poll.records` | 100 | 5 |
| Fetch | `fetch.min.bytes` | 1 | – (concept reference) |
| Fetch | `fetch.max.wait.ms` | 500 ms | – (concept reference) |
| Fetch | `max.partition.fetch.bytes` | 1 MB | – |
| Fetch | `fetch.max.bytes` | 50 MB | – |
| Offsets | `enable.auto.commit` | `false` | 7 |
| Offsets | `auto.commit.interval.ms` | 5 s | – |
| Offsets | `auto.offset.reset` | `earliest` | 8 |
| Offsets | `isolation.level` | `read_committed` | 13 |
| Assignment | `partition.assignment.strategy` | `RangeAssignor` | 10 |
| Assignment | `client.rack` | not set | Concept reference |
| Interceptors | `interceptor.classes` | commented out | 3 (uncomment) |

---

## Scenario index

| # | Scenario | Kind of change |
|---|---|---|
| 1 | Baseline — full-config walkthrough | none |
| 2 | Consumer group basics | none (run 2+ instances) |
| 3 | Interceptor + rebalance-listener hooks | uncomment one line |
| 4 | Broker down / coordinator failover | none (kill a broker) |
| 5 | Poll loop / `max.poll.interval.ms` | change 3 values + add sleep |
| 6 | Session timeout / heartbeat | change 2 values + add sleep |
| 7 | Offset commit modes | flip `enable.auto.commit`, alter commit call |
| 8 | `auto.offset.reset` behavior | change 2 values, run 3× with fresh group |
| 9 | Delivery semantics | reorder process() vs commit() |
| 10 | Rebalance protocols | change 1 value, run 2× |
| 11 | Seek / replay | add ~4 lines of `seek()` API |
| 12 | Consumer lag | none (JMX + CLI) |
| 13 | Isolation level | change 1 value |
| 14 | Static membership | uncomment 1 line |
| 15 | Consumer quota / throttle | change `client.id` + `kafka-configs.sh` |
| 16 | JMX metrics | none (attach JConsole) |
| 17 | Benchmarking with `kafka-consumer-perf-test.sh` | none (CLI tool) |
| 18 | Graceful shutdown — `wakeup()` + `close()` | change 1 value, run 2× |

---

# The scenarios

---

## 1. Baseline — full-config walkthrough

**What**

- Run the file as-is and tour every consumer knob with the class open on screen.
- Every `props.put(...)` line has a two-bullet comment: *what* the setting does and *what* it costs.
- Seven numbered banner sections: Connection → Deserialization → Group membership → Fetch → Offsets → Assignment → Interceptors.
- This scenario is *"read the file top-to-bottom"* — the only scenario with no code edits.

**Change** — none.

**Run**

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.consumer.KafkaConsumerClient
```

- Consumes from `transactions` continuously.
- `Ctrl+C` when done. The shutdown hook triggers a clean group leave.

**See**

```
[rebalance] ASSIGNED [transactions-0, transactions-1, transactions-2]
Received topic=transactions partition=0 offset=17 key=upi value={"txnId":"..."}
Received topic=transactions partition=1 offset=4  key=neft value={"txnId":"..."}
lag transactions-0 position=18 logEnd=52 recordsBehind=34
Consumer closed cleanly.
```

- All 3 partitions assigned to this single member.
- Position lag prints every 10 polls — visible catchup.

**Admin take-home**

- Every consumer knob has a topic-level or broker-level counterpart.
- This class is the reference against which real production consumer configs should be compared.
- Reading this file top-to-bottom = touring the consumer-broker contract.
- Every deviation in a team's consumer from *this* baseline is a trade-off you should ask them to justify.

---

## 2. Consumer group basics

**What**

- Multiple processes with the same `group.id` **share** the topic's partitions.
- Each partition is consumed by **exactly one** consumer in the group at any moment.
- A group with more members than partitions has **idle** members.
- Different `group.id` values consume **independently** — every group sees its own copy of the topic.

**Change** — none. Run multiple instances in separate terminals.

**Run** (in three separate terminals)

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.consumer.KafkaConsumerClient
```

**See**

- Terminal 1 alone: 3 partitions assigned to it (`[transactions-0, transactions-1, transactions-2]`).
- Start terminal 2: rebalance — each terminal ends up with a subset (e.g. `[0, 1]` and `[2]`).
- Start terminal 3: rebalance — one partition per terminal (`[0]`, `[1]`, `[2]`).
- Start terminal 4: rebalance — one member gets NO partitions (idle).

Verify with the admin CLI:

```bash
./kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group payments-reporting-group --members --verbose
```

**Admin take-home**

- **A topic with N partitions supports at most N active consumers per group.** More members = idle members.
- **More brokers or more replicas do not increase consumer parallelism.** Only more partitions do.
- Adding partitions is one-way — you cannot shrink a topic. Size for the largest fleet you'll ever run.
- Different `group.id`s each have their own set of committed offsets — changing group.id can silently replay or skip data.

---

## 3. Interceptor + rebalance-listener hooks

**What**

- `LoggingRebalanceListener` (nested class at the bottom) fires on:
  - `onPartitionsAssigned` — this member may start processing these partitions.
  - `onPartitionsRevoked` — orderly rebalance in progress; finish or checkpoint.
  - `onPartitionsLost` — ownership already gone (usually after timeout); do NOT commit.
- `LoggingConsumerInterceptor` (also nested) fires on:
  - `onConsume` — each poll batch, before the app sees the records.
  - `onCommit` — each offset commit.
- Both are wired: the rebalance listener via `consumer.subscribe(..., listener)`; the interceptor via a commented-out `interceptor.classes` line.

**Change** — uncomment the `interceptor.classes` line in section 7. Find:

```java
// props.put("interceptor.classes",
//         "com.example.consumer.KafkaConsumerClient$LoggingConsumerInterceptor");
```

Change to:

```java
props.put("interceptor.classes",
        "com.example.consumer.KafkaConsumerClient$LoggingConsumerInterceptor");
```

**Run**

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.consumer.KafkaConsumerClient
```

**See**

- Rebalance events show up in one form:

```
[rebalance] ASSIGNED [transactions-0, transactions-1, transactions-2]
```

- Interceptor events show up in another:

```
[interceptor] onConsume count=42
[interceptor] onCommit  {transactions-0=OffsetAndMetadata{offset=200, ...}}
```

- Start a second terminal to force a rebalance; watch:

```
[rebalance] REVOKED  [transactions-1, transactions-2] pendingCommits=0
[rebalance] ASSIGNED [transactions-0]
```

**Admin take-home**

- **Rebalance listeners are the SPI point for correctness under partition movement.** Real apps commit or checkpoint in `onPartitionsRevoked`; recover state in `onPartitionsAssigned`; discard partial work in `onPartitionsLost`.
- Consumer interceptors are less common than producer interceptors — the standard use is tracing header extraction (`traceparent`).
- Blocking work in `onConsume` slows every poll. Blocking work in a rebalance callback slows every rebalance.
- Re-comment the interceptor line after the demo so subsequent scenarios stay clean.

---

## 4. Broker down / coordinator failover

**What**

- Every consumer group has a **group coordinator** — a broker that manages membership, tracks offsets, and drives rebalancing.
- The coordinator is one of the brokers (determined by the group.id hash mod internal partition count of `__consumer_offsets`).
- Kill that broker → the coordinator moves; the group rebalances; consumption resumes.
- Kill a non-coordinator broker holding a partition leader → the leader moves; the fetch retries against the new leader.

**Change** — none. This is an infrastructure action.

> **Before you run:** Uncomment `NetworkClient` and `Metadata` in `src/main/resources/simplelogger.properties` so the coordinator + leader-move chatter is visible. Re-comment after.

**Run**

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.consumer.KafkaConsumerClient
```

At any moment, `Ctrl+C` any broker.

**See**

- If you killed a partition leader: brief `WARN` about disconnect, then metadata refresh, then consumption resumes against the new leader.
- If you killed the coordinator: a longer pause (up to `session.timeout.ms`), then the group discovers a new coordinator, rebalances, and resumes.
- The `LoggingRebalanceListener` prints `REVOKED` / `ASSIGNED` cycles during the rebalance.

**Admin take-home**

- **Coordinator election adds latency to a broker failure**, on top of the normal leader-move latency. Budget for both.
- Watch broker JMX `ActiveControllerCount` (should be exactly 1 across cluster) and `UnderReplicatedPartitions` on every broker.
- `__consumer_offsets` is a critical topic. Its `replication.factor` (default 3) and `min.insync.replicas` (recommend 2) are broker-level settings — a bad default here means group-state loss.
- Watch consumer JMX `last-heartbeat-seconds-ago` and `coordinator-request-latency-avg` — spikes correlate with coordinator moves.

---

## 5. Poll loop mechanics — `max.poll.interval.ms`

**What**

- The consumer must call `poll()` at least every `max.poll.interval.ms` (default 5 min).
- If it doesn't — regardless of heartbeats — the broker considers it dead and rebalances.
- Common cause: a batch of `max.poll.records` records that take too long to process.
- The math you must know: **worst-case-time-per-record × max.poll.records ≤ max.poll.interval.ms**.

**Change** — three values in section 3 and section 4, plus a sleep in `process()`.

Section 3 (Group membership):

```java
props.put("max.poll.interval.ms", 300000);
```

Change to:

```java
props.put("max.poll.interval.ms", 5000);
```

Section 4 (Fetch behavior):

```java
props.put("max.poll.records", 100);
```

Change to:

```java
props.put("max.poll.records", 5);
```

Add a sleep inside the `process()` method to simulate slow work:

```java
private static void process(ConsumerRecord<String, String> record) {
```

Add as the first line of the method:

```java
try { Thread.sleep(2_000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
```

Then run TWO consumers in the same group to make the eviction visible.

**Run** (two terminals)

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.consumer.KafkaConsumerClient
```

**See**

- Each poll returns 5 records × 2 s each = 10 s. But `max.poll.interval.ms=5 s`.
- The slow member gets evicted. Rebalance moves partitions to the other terminal.
- Log:

```
[rebalance] LOST [transactions-1, transactions-2]
```

- The evicted member then tries to rejoin and takes back some partitions. Cycle repeats — a **rebalance storm**.

**Admin take-home**

- **The eviction is silent to the app** — the process is still running, but the group treats it as dead.
- **Rebalance storm** shows up as high `consumer-lag` + `record-consumed-rate=0` (processing while evicted). Classic operator alert.
- Fix in this order:
  1. Measure worst-case processing time per record.
  2. Bound work per poll with `max.poll.records`.
  3. Fix blocking / unbounded dependencies inside processing.
  4. Raise `max.poll.interval.ms` only when honest processing needs it.
- **Don't raise `max.poll.interval.ms` to hide the problem** — you're just delaying detection of stuck consumers.

Undo the three changes + delete the sleep after the demo.

---

## 6. Session timeout / heartbeat

**What**

- Even if `poll()` is called on time, the coordinator can lose the consumer via missed **heartbeats**.
- The consumer sends heartbeats on its background heartbeat thread every `heartbeat.interval.ms`.
- Missing them for `session.timeout.ms` → the coordinator declares the consumer dead → rebalance.
- **Common cause:** blocking work INSIDE the consumer thread that also blocks the heartbeat thread (long GC, JNI, deadlock).

**Change** — two values in section 3, plus a longer sleep in `process()`.

Section 3:

```java
props.put("session.timeout.ms", 10000);
props.put("heartbeat.interval.ms", 3000);
```

Change to:

```java
props.put("session.timeout.ms", 6000);
props.put("heartbeat.interval.ms", 2000);
```

In `process()`, add a blocking wait longer than the session timeout:

```java
try { Thread.sleep(15_000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
```

**Run**

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.consumer.KafkaConsumerClient
```

**See**

- First record processes → sleeps 15 s → misses ~5 heartbeats.
- Coordinator evicts. Consumer detects on next heartbeat attempt and rebalances.
- Log:

```
[rebalance] LOST [transactions-0, transactions-1, transactions-2]
[rebalance] ASSIGNED [transactions-0, transactions-1, transactions-2]
```

**Admin take-home**

- **Session timeout is for failure detection**; poll interval is for stuck-processing detection. They're two different clocks.
- Broker enforces bounds: `group.min.session.timeout.ms` and `group.max.session.timeout.ms`. You can't bypass these from the client.
- **Heartbeats travel on the consumer's I/O thread but are driven by the poll loop's cadence** in classic protocol. If poll is stuck, heartbeats stop (this is why max.poll.interval.ms scenario 5 and session.timeout scenario 6 often overlap in practice).
- Set `heartbeat.interval.ms ≤ session.timeout.ms / 3`. Lower = faster detection but more chatter.

Undo the changes + delete the sleep.

---

## 7. Offset commit modes

**What**

- Three ways to commit offsets:
  - **Auto-commit** (`enable.auto.commit=true`) — the client commits automatically every `auto.commit.interval.ms`.
  - **`commitSync()`** — blocks until broker acks; failures surface to the calling thread.
  - **`commitAsync(callback)`** — fires and forgets; callback reports result; multiple in-flight commits can complete out of order.
- Auto-commit runs **inside `poll()`**. It commits the LAST offset returned by the previous poll — not necessarily the last offset processed.

**Change** — flip auto-commit on for one run, then compare with commitSync vs commitAsync.

Section 5 currently:

```java
props.put("enable.auto.commit", false);
```

**Run (a) — auto-commit:** change to `true` and remove the manual commit block from `main()`:

```java
if (!processedOffsets.isEmpty()) {
    consumer.commitSync(processedOffsets);
    processedOffsets.clear();
}
```

Delete or comment out these four lines. Then run:

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.consumer.KafkaConsumerClient
```

**Run (b) — commitSync (baseline):** revert to the original code and run.

**Run (c) — commitAsync:** change:

```java
consumer.commitSync(processedOffsets);
```

to:

```java
consumer.commitAsync(processedOffsets, (offsets, exception) -> {
    if (exception != null) System.err.println("commit failed: " + exception.getMessage());
});
```

Then run.

**See**

- Auto-commit: no `[interceptor] onCommit` line explicitly at end of batch — commits happen on the NEXT poll.
- commitSync: predictable, blocking. Failure prints an exception on the main thread.
- commitAsync: fast, non-blocking. A failure is reported via callback — may fire long after the offending commit.

**Admin take-home**

- **Auto-commit is dangerous with slow processing.** It commits records returned by the last poll, not records finished. A crash mid-batch = **lost records**.
- **`commitSync` in a tight loop is slow** — one round trip per commit. Use it for the FINAL commit before shutdown.
- **`commitAsync` in the loop + `commitSync` in shutdown** is the production pattern.
- Broker-side JMX to watch: `commit-latency-avg`; `commit-rate`.

Undo the changes.

---

## 8. `auto.offset.reset` behavior

**What**

- Applies ONLY when the group has no valid committed offset for a partition:
  - Fresh group (`group.id` never seen before).
  - Committed offset points at data that has been retention-deleted.
- Three values:
  - `earliest` — start at the beginning of the log.
  - `latest` — start after the current end (only new records).
  - `none` — throw `NoOffsetForPartitionException` (safest for critical apps).
- Once the group has committed offsets, this setting is **ignored**.

**Change** — two values in section 5 for each run:

```java
props.put("auto.offset.reset", "earliest");
props.put("group.id", GROUP_ID);
```

Change the second to a NEW group id per run so `auto.offset.reset` actually applies:

```java
props.put("group.id", "offset-reset-demo-" + java.util.UUID.randomUUID());
```

Rotate `auto.offset.reset` through `"earliest"`, `"latest"`, `"none"`.

**Run** (three times)

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.consumer.KafkaConsumerClient
```

**See**

| Run | `auto.offset.reset` | Behavior |
|-----|---------------------|----------|
| a | `earliest` | Consumes from record offset 0 — all historical records. |
| b | `latest` | Empty — no records printed unless the producer writes new ones. |
| c | `none` | `Exception in thread "main" org.apache.kafka.clients.consumer.NoOffsetForPartitionException`. |

**Admin take-home**

- **`earliest` on a fresh group is a replay of the entire retained log** — can flood downstream systems.
- **`latest` on a fresh group silently skips existing data** — worst choice for critical apps.
- **`none` forces an explicit human decision** — safest for irreplaceable data pipelines.
- **When you change `group.id` in production**, this setting decides what happens. Always check retention + downstream capacity first.
- Explicit offset reset (`kafka-consumer-groups.sh --reset-offsets`) is the safe alternative to `auto.offset.reset` for known-state migrations.

Undo the changes.

---

## 9. Delivery semantics — at-most-once / at-least-once / exactly-once

**What**

- Three delivery semantics, chosen by the **order of process() vs commit()**:
  - **At-most-once** — `commit → process`. If commit succeeds and process fails/crashes, record is lost.
  - **At-least-once** — `process → commit` (this file's default). If process succeeds and commit fails/crashes, record repeats.
  - **Exactly-once (within Kafka)** — producer + consumer inside a Kafka transaction, with `sendOffsetsToTransaction()`.
- **Exactly-once across external systems** requires idempotent writes (natural key + upsert) or a transactional outbox pattern.

**Change** — swap the commit ordering. Baseline (at-least-once):

```java
for (ConsumerRecord<String, String> record : records) {
    process(record);
    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
    processedOffsets.put(tp, new OffsetAndMetadata(record.offset() + 1));
}
if (!processedOffsets.isEmpty()) {
    consumer.commitSync(processedOffsets);
    processedOffsets.clear();
}
```

Change to at-most-once (commit BEFORE process — dangerous):

```java
Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
for (ConsumerRecord<String, String> record : records) {
    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
    toCommit.put(tp, new OffsetAndMetadata(record.offset() + 1));
}
if (!toCommit.isEmpty()) consumer.commitSync(toCommit);
for (ConsumerRecord<String, String> record : records) {
    process(record);  // if this throws or the JVM crashes, the record is silently lost
}
```

**Run**

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.consumer.KafkaConsumerClient
```

Terminate mid-batch with `kill -9` to simulate a crash. Restart and count records missing.

**See**

- At-most-once: records missing after a `kill -9` mid-batch.
- At-least-once: same records re-processed after a `kill -9` mid-batch.

**Admin take-home**

- **At-least-once is the safest default.** Applications must handle duplicates via idempotent writes (natural keys, upserts, dedup tables).
- **At-most-once is rare — reserve for metrics / lossy telemetry only.**
- **True exactly-once requires Kafka transactions on BOTH sides.** Producer scenario 12 shows the write side.
- **Kafka cannot atomically commit a Kafka offset AND an external database write.** That's the outbox / two-phase-commit pattern territory.

Undo the code changes.

---

## 10. Rebalance protocols — Range vs CooperativeSticky

**What**

- Two rebalance protocols:
  - **Eager** (Range, RoundRobin, Sticky assignors) — every rebalance revokes ALL partitions from ALL members, then reassigns. Stop-the-world.
  - **Cooperative** (CooperativeStickyAssignor) — only partitions that MUST move are revoked. Others keep going.
- Cooperative rebalance is **incremental**: two rounds of rebalance, but total downtime for each partition is much shorter.
- Migrating from Eager → Cooperative requires a coordinated rolling deploy.

**Change** — one value in section 6:

```java
props.put("partition.assignment.strategy",
        "org.apache.kafka.clients.consumer.RangeAssignor");
```

Change to:

```java
props.put("partition.assignment.strategy",
        "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
```

**Run** — start 2–3 consumers with **the same protocol setting**, then stop / start one to trigger rebalances. Repeat with each protocol.

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.consumer.KafkaConsumerClient
```

**See**

- Eager (RangeAssignor):

```
[rebalance] REVOKED  [transactions-0, transactions-1, transactions-2] pendingCommits=0
[rebalance] ASSIGNED [transactions-0, transactions-1]
```

- Cooperative (CooperativeStickyAssignor):

```
[rebalance] REVOKED  [transactions-2] pendingCommits=0     ← only the moved one
[rebalance] ASSIGNED [transactions-0, transactions-1]      ← keeps the others
```

**Admin take-home**

- **CooperativeSticky is a major win in large clusters** — proportional to `partitions_per_member × member_count`.
- **Migrating from Range/RoundRobin to CooperativeSticky is a coordinated rolling deploy.** You cannot mix protocols in one group.
- The **client-side default is still Range** for backward compatibility. Push teams to CooperativeSticky if they have big consumer groups.
- Kafka Streams and Kafka Connect use CooperativeSticky by default. Vanilla apps often don't.

Undo the change.

---

## 11. Seek / replay

**What**

- The `Consumer` API has `seek(partition, offset)` for point-in-time replay.
- Also `seekToBeginning()`, `seekToEnd()`, and `offsetsForTimes()` for timestamp-based replay.
- Common uses:
  - **Bug in downstream:** replay the last hour to re-process with the fix.
  - **Historical debugging:** read a specific offset that a stack trace pointed at.
  - **A/B testing:** two groups reading the same data from different starting points.

**Change** — add a `seek()` call after `subscribe()`. Wrap it in an `onPartitionsAssigned` callback so it fires after the assignment is known.

Modify the LoggingRebalanceListener's `onPartitionsAssigned` to seek to offset 0 on first assignment:

```java
@Override
public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    System.out.println("[rebalance] ASSIGNED " + format(partitions));
}
```

Change to:

```java
@Override
public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    System.out.println("[rebalance] ASSIGNED " + format(partitions));
    // Seek back to offset 0 for every partition — replay the whole retained log.
    for (TopicPartition tp : partitions) {
        consumer.seek(tp, 0L);
    }
}
```

(Requires holding a reference to `consumer` — pass it into the listener constructor or use a field.)

For classroom simplicity, an alternative: after `consumer.subscribe(...)`, add a poll(0) to get assignments, then seek explicitly:

```java
consumer.poll(Duration.ZERO);  // triggers rebalance so assignments are populated
for (TopicPartition tp : consumer.assignment()) {
    consumer.seek(tp, 0L);
}
```

**Run**

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.consumer.KafkaConsumerClient
```

**See**

- Consumer re-reads records from offset 0 of every partition.
- Log-end-offset stays the same; consumer position climbs from 0.

**Admin take-home**

- **`seek()` overrides the group's committed offset for this consumer instance.** It does NOT rewind the group's committed state.
- **For a group-wide reset**, use `kafka-consumer-groups.sh --reset-offsets` — it updates the group's stored offsets.
- **Replay to a specific timestamp** (much more useful than offset): use `offsetsForTimes(Map<TopicPartition,Long>)`.
- **Rehydrating downstream systems** is the #1 use of replay — deploying a new database schema, backfilling a new column, etc.

Undo the changes.

---

## 12. Consumer lag

**What**

- **Position** — the next offset THIS consumer will fetch.
- **Committed offset** — the next offset the group will resume from after recovery.
- **Log-end offset** — the next offset the broker will append.
- **Lag** — log-end minus consumer position (or committed offset for group-level lag).

**Change** — none. This is a JMX + CLI walkthrough.

**Run**

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.consumer.KafkaConsumerClient
```

Watch the file's `printPositionLag()` output every 10 polls:

```
lag transactions-0 position=200 logEnd=252 recordsBehind=52
```

Compare with the admin CLI (uses the group's *committed* offset, not this live consumer's *position*):

```bash
./kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group payments-reporting-group
```

Output columns: `CURRENT-OFFSET` (committed), `LOG-END-OFFSET`, `LAG`.

**Consumer JMX metrics to watch:**

```
kafka.consumer:type=consumer-fetch-manager-metrics,client-id=payments-consumer-1,topic=transactions,partition=0
  → records-lag
  → records-lag-max
  → records-consumed-rate
```

**Admin take-home**

- **Absolute lag is not a good alert.** Alert on:
  - Sustained lag growth (integral of `records-lag` over time).
  - Time-lag (record timestamp vs wall clock) — bounded by `record.timestamp` header.
  - `records-consumed-rate = 0` when it shouldn't be.
- **Skewed partitions** are common — one hot partition can dominate lag while others are caught up. Always look per-partition.
- **`read_committed` waits behind open transactions** — apparent lag that isn't really lag. Confirm via `kafka-transactions.sh` or `kafka-consumer-groups.sh`.
- **Diagnosis order:** input rate vs process rate → hot partition → downstream slowness → rebalance frequency → commit frequency → open transactions.

---

## 13. Isolation level — `read_committed` vs `read_uncommitted`

**What**

- Kafka producer transactions (producer scenario 12) commit or abort groups of records.
- `read_uncommitted` (Kafka client default): consumer sees all records including aborted ones.
- `read_committed`: broker skips aborted records via control markers; consumer never sees them.
- `read_committed` also waits behind **open transactions** — cannot advance past the **Last Stable Offset (LSO)**.

**Change** — one value in section 5:

```java
props.put("isolation.level", "read_committed");
```

Change to:

```java
props.put("isolation.level", "read_uncommitted");
```

**Run**

- First run the producer's transaction scenario (see the producer README, scenario 12) to write committed + aborted records to `transactions`.
- Then run this consumer scenario twice — once with each isolation level.

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.consumer.KafkaConsumerClient
```

**See**

- `read_uncommitted`: sees all records (10 records if the producer did 10 transactions with 1 record each).
- `read_committed`: sees only committed transactions (5 records).

Verify via CLI:

```bash
./kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions --from-beginning --isolation-level read_committed
./kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions --from-beginning --isolation-level read_uncommitted
```

**Admin take-home**

- **Set `isolation.level` to match the producer's contract.** If producers don't use transactions, `read_uncommitted` is fine (and avoids the LSO block).
- **Long-running open transactions block `read_committed` consumers** — they look like lag but aren't.
- **Aborted records physically exist on disk** with control markers. Retention deletes them normally.
- Monitor transaction coordinator health (`kafka.server:type=TransactionCoordinatorMetrics`) alongside consumer lag.

Undo the change.

---

## 14. Static membership — `group.instance.id`

**What**

- **Normal membership** — every join gets a new temporary member id; every restart triggers a rebalance.
- **Static membership** — set `group.instance.id` to a stable string; a restart of the same instance skips the rebalance (during `session.timeout.ms`).
- Requires **every live instance to have a unique static id**. Duplicates fence each other (`FencedInstanceIdException`).
- Kafka Streams and Kafka Connect handle static ids automatically; vanilla apps must set them explicitly.

**Change** — uncomment section 3's line:

```java
// props.put("group.instance.id", "payments-consumer-instance-1");
```

Change to:

```java
props.put("group.instance.id", "payments-consumer-instance-1");
```

For the second terminal, use `payments-consumer-instance-2`.

**Run** (two terminals, each with a UNIQUE `group.instance.id`)

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.consumer.KafkaConsumerClient
```

Stop and restart terminal 1 within `session.timeout.ms` (10 s baseline).

**See**

- No `REVOKED` / `ASSIGNED` on terminal 2 during terminal 1's restart. The partitions stay put.
- Terminal 1 rejoins with the same identity and its old partitions.
- Contrast with the non-static behaviour: without `group.instance.id`, terminal 1's restart triggers a full group rebalance.

**Admin take-home**

- **Static membership drastically reduces rebalance storms during rolling deploys.**
- **Never copy the same `group.instance.id` to two live processes** — the older one gets fenced.
- **A Kubernetes StatefulSet naturally gives unique ordinal-based ids** — set `group.instance.id=$(POD_NAME)`.
- Combines well with `session.timeout.ms` tuned to be > rolling-restart-time-per-pod (usually 30–60 s).

Undo the change.

---

## 15. Consumer quota / throttle

**What**

- Attach a per-`client.id` fetch quota via `kafka-configs.sh`.
- The broker enforces by **delaying the fetch response** — no rejection.
- Consumer sees latency climb; JMX `fetch-throttle-time-avg` becomes non-zero.

**Change** — one value in section 1:

```java
props.put("client.id", "payments-consumer-1");
```

Change to:

```java
props.put("client.id", "quota-demo-consumer");
```

Attach a low quota (8 KB/s):

```bash
./kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type clients --entity-name quota-demo-consumer --add-config 'consumer_byte_rate=8192'
```

Verify:

```bash
./kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type clients --entity-name quota-demo-consumer
```

**Run**

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.consumer.KafkaConsumerClient
```

**See**

- `Received ...` lines trickle out instead of streaming.
- The broker is delaying each fetch response.

**Cleanup** (undo the quota):

```bash
./kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type clients --entity-name quota-demo-consumer --delete-config 'consumer_byte_rate'
```

**Admin take-home**

- **Quotas are broker-side enforcement.** Consumer sees latency, then lag — never errors.
- **Consumer quotas cap FETCH bytes**, not messages. A very slow consumer with a low quota will fall behind.
- Definitive quota signal: consumer JMX `fetch-throttle-time-avg > 0`.
- Common quota targets: multi-tenant clusters (cap noisy consumers by `client.id`), SASL clusters (cap by `user`).

Undo the `client.id` change after the demo.

---

## 16. JMX metrics

**What**

- Every `KafkaConsumer` exposes an in-JVM metrics registry.
- Same numbers published as JMX MBeans under:

```
kafka.consumer:type=consumer-metrics,client-id=<client.id>
kafka.consumer:type=consumer-fetch-manager-metrics,client-id=<client.id>
kafka.consumer:type=consumer-coordinator-metrics,client-id=<client.id>
```

- Prometheus JMX exporter, Grafana dashboards, DataDog integrations all walk this tree.

**Change** — none.

**Run**

Terminal 1:

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.consumer.KafkaConsumerClient
```

Terminal 2:

```bash
jconsole
```

Attach to the running JVM; expand each `kafka.consumer:type=*` tree.

**See** — the "must scrape" set:

| Metric | Alert when… |
|--------|-------------|
| `records-consumed-rate` | Suddenly = 0 while the topic is receiving records — consumer stuck. |
| `bytes-consumed-rate` | Track alongside consumed rate; disparity → skewed partitions. |
| `records-lag-max` | > SLO threshold — lagging behind ingestion. |
| `records-lag` per partition | Skew — one hot partition dominating lag. |
| `fetch-latency-avg` | > SLO — broker or network slow. |
| `commit-latency-avg` | Elevated — coordinator or `__consumer_offsets` slow. |
| `join-rate`, `sync-rate` | Elevated — rebalance storm. |
| `last-heartbeat-seconds-ago` | Climbing → about to be evicted. |
| `fetch-throttle-time-avg` | > 0 → quota active on this `client.id`. |

**Admin take-home**

- These are the numbers a production dashboard row should show. Nothing more, nothing less.
- Metric names are identical between `consumer.metrics()`, JMX, and every JMX exporter — one vocabulary.
- Every prior scenario resurfaces here as a metric:
  - Scenarios 5, 6 → `join-rate`, `sync-rate`, `last-heartbeat-seconds-ago`.
  - Scenario 7 → `commit-latency-avg`, `commit-rate`.
  - Scenario 12 → `records-lag`, `records-lag-max`.
  - Scenario 15 → `fetch-throttle-time-avg`.

---

## 17. Benchmarking with `kafka-consumer-perf-test.sh`

**What**

- The CLI benchmark tool for consumers. Ships with the Kafka distribution.
- One command reports:
  - Total records/sec throughput.
  - MB/sec throughput.
  - Wall-clock time.
- Simpler output than `kafka-producer-perf-test.sh` — no latency percentiles (consumer latency = end-to-end, harder to define).

**Change** — none. This is a separate CLI tool, not the Java class.

**Run** (baseline)

```bash
./kafka/bin/kafka-consumer-perf-test.sh --bootstrap-server localhost:9092 --topic transactions --messages 100000 --group perf-test-group
```

**See**

```
start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec
2026-07-14 05:23:44:123, 2026-07-14 05:23:46:891, 15.234, 5.505, 100000, 36135.298, 245, 2523, 6.038, 39635.437
```

**Key flags**

| Flag | What it does |
|------|--------------|
| `--messages N` | Number of records to consume. |
| `--fetch-size N` | Bytes per fetch (like `fetch.max.bytes`). Bigger = higher throughput. |
| `--threads N` | Number of consumer threads inside the tool. |
| `--group <name>` | Consumer group. Fresh each run for reproducible baselines. |
| `--consumer.config <file>` | Load props from a file. Preferred for repeatable runs. |

**Comparison recipe** (compare `fetch-size` values):

```bash
for SIZE in 1024 65536 1048576; do echo "=== fetch-size=$SIZE ==="; ./kafka/bin/kafka-consumer-perf-test.sh --bootstrap-server localhost:9092 --topic transactions --messages 100000 --group perf-test-$SIZE --fetch-size $SIZE; done
```

**Admin take-home**

- **Baseline before deploying** — save the summary line as your comparison point.
- **Rerun with the exact production consumer config** via `--consumer.config` — small changes move numbers a lot.
- **Consumer throughput is heavily topic-partition-count-limited.** A 3-partition topic can't outrun 3 parallel consumer threads.
- **Always run 3+ times, take the median.** Same JIT / page-cache concerns as producer benchmarking.

---

## 18. Graceful shutdown — `wakeup()` + `close(Duration)`

**What**

- `KafkaConsumer` is NOT thread-safe. The only method safe to call from another thread is `wakeup()`.
- `wakeup()` interrupts a blocking poll with `WakeupException`.
- Signal handler → set flag → `wakeup()` → main loop catches `WakeupException` → `close(Duration)`.
- `consumer.close(Duration)` leaves the group cleanly within N ms; anything longer is dropped.
- Clean close = fast rebalance for the surviving group members. Dirty close = wait `session.timeout.ms`.

**Change** — change close timeout to see the difference.

Baseline (in `finally`):

```java
consumer.close(Duration.ofSeconds(10));
```

Change to (aggressive):

```java
consumer.close(Duration.ofMillis(100));
```

**Run** — with two consumers in the same group. Kill one with `Ctrl+C` (clean signal) and note how quickly the other's rebalance completes.

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.consumer.KafkaConsumerClient
```

**See**

- With `close(Duration.ofSeconds(10))`: killed member sends `LeaveGroup`; other member rebalances in ~1–2 s.
- With `close(Duration.ofMillis(100))`: close returns before `LeaveGroup` can go out; other member waits `session.timeout.ms` (10 s baseline) before rebalancing.
- Compare with `kill -9` (dirty crash): always waits `session.timeout.ms`.

**Admin take-home**

- **Every rolling deploy triggers scenario 18.** A too-short close timeout multiplies rebalance downtime across the fleet.
- **Recommend `consumer.close(Duration.ofSeconds(10))` as the app default.** Enough for a clean LeaveGroup + last commit.
- **Kubernetes `terminationGracePeriodSeconds`** must be ≥ close timeout + commit time + margin. Default 30 s is usually fine; check for slow processing tails.
- **Static membership (scenario 14) + graceful close** — the two together shrink rolling-deploy pain to almost zero.

Undo the change.

---

# Reference — failure signatures

Quick lookup for *"my consumer is doing X — what fired?"*.

| Symptom | Which knob / broker signal fired | Fix |
|---------|----------------------------------|-----|
| `[rebalance] LOST` after slow processing | `max.poll.interval.ms` exceeded | Shrink `max.poll.records` or fix slow processing |
| `[rebalance] LOST` while process is alive | `session.timeout.ms` exceeded (missed heartbeats) | Investigate GC pauses, deadlocks, JNI blocks |
| `NoOffsetForPartitionException` | `auto.offset.reset=none` on a fresh group | Choose explicit reset via `kafka-consumer-groups.sh` |
| Poll returns empty forever after group change | Silent skip via `auto.offset.reset=latest` on fresh group | Change to `earliest` OR use explicit reset |
| Poll hangs at same offset | Poison record (undeserialisable bytes) | Wrap deserializer with skip-on-error / dead-letter |
| `records-lag` climbs but process rate is fine | Skewed partitions (hot key) | Rebalance key strategy on producer |
| `records-lag` climbs and process rate is 0 | Rebalance storm (see scenario 5) | Fix processing time or bump poll interval |
| `records-lag` climbs on `read_committed` | Open transaction blocking LSO advance | Check `kafka.server:type=TransactionCoordinatorMetrics` |
| `fetch-throttle-time-avg > 0` | Consumer quota active | Loosen quota OR let consumer catch up |
| `FencedInstanceIdException` | Two live consumers with same `group.instance.id` | Fix deployment to give each replica unique static id |
| Consumer never receives records after subscribe | Broker ACL missing `READ` on topic | `kafka-acls.sh --add --allow-principal User:X --operation Read --topic transactions` |

---

# Tuning cheat sheet

Levers and the direction they move things.

### For throughput (records/sec)

- **Raise `max.poll.records`** — more records per poll. Match to your processing loop's batch capacity.
- **Raise `fetch.min.bytes`** and `fetch.max.wait.ms` — bigger batches from broker → fewer round-trips.
- **Raise `max.partition.fetch.bytes`** — accommodate larger records without splits.
- **Parallelize by adding partitions and consumers** — the only way to scale beyond one partition's throughput.
- **Measure with `kafka-consumer-perf-test.sh`** — scenario 17.

### For low latency (per-record)

- **`fetch.min.bytes=1`** and low `fetch.max.wait.ms` — return early on small batches.
- **Small `max.poll.records`** — smaller batches, faster commit cadence.
- **`enable.auto.commit=true`** with a low `auto.commit.interval.ms` — but be aware of at-most-once risk.

### For durability (no data loss)

- **`enable.auto.commit=false`** — commit AFTER successful processing.
- **`commitSync` after each batch**, `commitAsync` in the hot path + `commitSync` on shutdown.
- **`auto.offset.reset=none`** on critical pipelines — no silent skip or replay.

### For memory safety

- Bound `max.partition.fetch.bytes × N_partitions × N_in_flight_fetches` to fit in the heap.
- Bound `max.poll.records` so an app-side batch fits in memory.
- Bound `close(Duration)` so blocked shutdown can't tie up a slot.

---

# Quick concept reference

Four cross-cutting concepts that don't have their own scenario but come up across many.

## Poll semantics — what happens per `poll()` call

- The consumer's `poll()` does far more than "return records":
  - Sends heartbeats to the group coordinator.
  - Fires rebalance-listener callbacks.
  - Runs auto-commit (if enabled).
  - Fetches records from partition leaders (asynchronously in the background between polls).
- `poll(Duration timeout)` returns when EITHER records are available OR the timeout expires.
- The consumer is **single-threaded from the app's perspective** — no background thread doing app-visible work.
- **Rule of thumb:** never block the poll loop; if you need parallel processing, hand records to a work queue.

## Consumer thread model

- One `KafkaConsumer` = **one polling thread + one background I/O thread** (invisible to the app).
- The **only** method safe to call from another thread is `wakeup()`.
- Calling ANY other method concurrently → `ConcurrentModificationException`.
- Multi-threaded consumption patterns:
  - **One thread per partition** (one consumer instance per thread) — simplest.
  - **One thread polls, N threads process** (bounded work queue) — highest throughput; must handle offset commits carefully.
  - Kafka Streams and Kafka Connect handle this for you.

## `__consumer_offsets` — where offsets live

- Kafka stores committed group offsets in an **internal compacted topic** called `__consumer_offsets`.
- Default 50 partitions; each group's offsets go to `hash(group.id) % 50`.
- Broker configs to know:
  - `offsets.topic.replication.factor` (default 3) — never lower.
  - `offsets.topic.num.partitions` (default 50) — set at cluster creation, hard to change.
  - `offsets.retention.minutes` (default 7 days) — how long inactive-group offsets are kept.
- Watch `UnderReplicatedPartitions` on `__consumer_offsets` — group state loss risk.

## Poison records — bad bytes on the wire

- A record whose value can't be deserialized → `poll()` throws → next `poll()` hits the same offset → infinite loop.
- Causes:
  - Producer changed serializer without redeploying consumer.
  - Corrupted bytes on disk (rare).
  - Schema Registry misconfiguration.
- Mitigations:
  - **Custom deserializer that returns a sentinel + logs** — never throws.
  - **Dead-letter topic pattern** — bad records go to a separate topic for human triage.
  - **Skip on error** — advance the offset by one and continue (data loss; needs audit trail).
- Discuss with the producing team's on-call before implementing skip.

## Sticky vs Eager rebalance protocols

- **Eager protocols** (Range, RoundRobin, Sticky) revoke ALL partitions from ALL members on every rebalance. Stop-the-world.
- **Cooperative protocol** (CooperativeSticky) revokes only partitions that MUST move. Two rounds of rebalance, but total downtime is much shorter.
- **Sticky (eager)** ≠ **CooperativeSticky** — the first is still stop-the-world; only the ASSIGNMENT is sticky (partitions try to stay).
- Migration from Eager → Cooperative is a **coordinated rolling deploy** — mixed groups fail.
- Kafka Streams and Kafka Connect use Cooperative by default; vanilla apps often don't.

## Rack-awareness — `client.rack`

- Set `client.rack=<zone-id>` on the consumer (matching `broker.rack`) → broker prefers same-rack replicas for fetch.
- Result: lower `fetch-latency-avg`, lower cross-AZ data-transfer cost.
- Requires broker-side `replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector`.
- Works with **fetch-from-follower** (KIP-392) — reading from an in-sync replica in the same rack instead of always from the leader.

## DNS resolution — `client.dns.lookup`

- Default in Kafka 3.0+ is `use_all_dns_ips` — client resolves the bootstrap hostname to ALL DNS A records and tries each in turn.
- Combined with a load balancer or DNS round-robin, HA becomes DNS-configured, not code-configured.
- Watch `connection-count` and `connection-close-rate` in JMX to see the client rotating DNS entries.

## Health check from JMX — is my consumer alive?

Three signals distinguish a healthy consumer from a stuck one:

- `records-consumed-rate > 0` while the topic is receiving records.
- `last-heartbeat-seconds-ago` < `session.timeout.ms / 2`.
- `records-lag-max` bounded (or growing but not runaway).

A dead consumer:

- `records-consumed-rate = 0` AND `last-heartbeat-seconds-ago` climbing.
- Root causes: poll loop blocked, deserializer failing, coordinator unreachable.

Recommended Kubernetes liveness probe:

- Fail liveness when `records-consumed-rate = 0` for > 60 s while the target rate should be > 0.
- Restart the pod on failure — `KafkaConsumer` is not designed to be resurrected in place.

---

# Admin cheat sheet

## KNOW — concepts every admin must internalise

| Question | Where |
|---|---|
| What consumer knobs exist? | Scenario 1 + config quick reference at top of this doc |
| How does a group divide partitions? | Scenario 2 |
| What happens during a broker outage? | Scenario 4 |
| What are the two failure detectors? | Scenarios 5, 6 (`max.poll.interval.ms` and `session.timeout.ms`) |
| What are the three delivery semantics? | Scenario 9 |
| What does `auto.offset.reset` actually do? | Scenario 8 |
| How is lag measured? | Scenario 12 |
| What client-side failure modes show up in prod? | Scenarios 5, 6, 8 + failure signatures table |
| How do I close the consumer safely during a rolling deploy? | Scenario 18 |

## WATCH — signals to scrape and alert on

| Signal | Scenario |
|---|---|
| Full "must-scrape" JMX set | 16 |
| Lag growth (per partition) | 12 |
| Rebalance rate | 5, 6, 10 |
| Commit latency | 7 |
| Quota pressure | 15 |
| Poison-record loops | Concept reference |
| Dead consumer (heartbeat + rate) | Concept reference |

## TUNE — levers admins actually pull

| Lever | Scenario | Quick guidance |
|---|---|---|
| Delivery semantics | 7, 9 | `process → commit` for at-least-once; idempotent writes |
| Poll capacity | 5 | `max.poll.records × time-per-record < max.poll.interval.ms` |
| Failure detection | 6 | `heartbeat.interval.ms ≤ session.timeout.ms / 3` |
| Rebalance protocol | 10 | CooperativeSticky for large groups |
| Static membership | 14 | Enables restarts without rebalance |
| Isolation level | 13 | Match producer's contract |
| Consumer quota | 15 | `consumer_byte_rate` per `client.id` |
| Measure the impact | 17 | `kafka-consumer-perf-test.sh` — median of 3 |
| Graceful shutdown budget | 18 | `consumer.close(Duration.ofSeconds(10))` in app code |

## Administrator change-review checklist

| Application change | Evidence to request |
|---|---|
| New `group.id` | Intended reset behaviour + retained-data impact |
| Auto-commit enabled | Proof offsets cannot move ahead of completed work |
| `auto.offset.reset` change | New-group + expired-offset recovery requirement |
| Larger `max.poll.records` | Worst-case batch time remains below `max.poll.interval.ms` |
| Larger `max.poll.interval.ms` | Measured legitimate processing time, not a hidden hang |
| `session.timeout.ms` change | Broker bounds + failure-detection objective |
| Assignor change | Rolling compatibility + migration plan |
| Static membership | Unique stable ids + replacement procedure |
| Fetch limit increase | Largest-record need + heap calculation |
| `isolation.level=read_committed` | Transactional upstream confirmed + open-transaction monitoring |

## Final mental model for students

- A topic contains partitions; each partition is an ordered log.
- A group divides partitions among its members.
- An offset is a **partition-local** position.
- A committed offset is a **recovery checkpoint** owned by the GROUP.
- `poll()` keeps the member alive AND delivers batches.
- Processing/commit order determines duplicates or loss.
- Rebalances move partition ownership; cooperative rebalances move only what must move.
- Lag is a **symptom** — correlate with rates, errors, skew, rebalances.
- Topic retention determines how far back recovery can go.
- Consumer configuration is always coupled to topic design, broker limits, and application processing time.

## The one-liner every admin should walk away with

> `enable.auto.commit=false` + `process → commitSync` + at-least-once + idempotent writes downstream = the safest default. Anything weaker in production needs justification.
