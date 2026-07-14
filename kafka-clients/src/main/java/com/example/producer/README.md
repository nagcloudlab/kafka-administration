# Producer — teaching sequence (one file, one guide)

For non-coder Kafka admins. The whole producer track lives in **one Java file** and **one guide** (this document).

- **File on screen:** [`KafkaProducerClient.java`](KafkaProducerClient.java)
- **Same run command every scenario:**

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.producer.KafkaProducerClient
```

- Between scenarios: **the trainer edits the file** (each scenario below lists the exact lines), saves, and re-runs.
- No CLI flags to remember.

---

## For the trainer

- **Audience:** admins, not developers. Read every scenario's *What* aloud in English before any code changes.
- **Session length:** pick 4–5 scenarios per session. All 16 scenarios span ~6–8 hours.
- **Reset between scenarios:** `git checkout kafka-clients/src/main/java/com/example/producer/KafkaProducerClient.java` undoes all edits in one shot.
- **The admin cheat sheet at the end** is the takeaway your students will remember longest.

## For the student

- **First pass:** read this doc with `KafkaProducerClient.java` open on screen while your trainer walks you through.
- **Later, as reference:** jump to a specific scenario by number when the topic surfaces in production (*"we saw NotEnoughReplicasException — that's scenario 10"*).
- **The Admin take-home** at the end of every scenario is the memorable summary — three to five bullets you should be able to recite for each concept.
- **Quick lookups** at the end of this doc:
  - `Reference — failure signatures` — *"my producer is doing X — what fired?"*.
  - `Tuning cheat sheet` — throughput / latency / durability / memory levers.
  - `Quick concept reference` — send semantics, headers, ordering, retries.
  - `Admin cheat sheet` — KNOW / WATCH / TUNE tables plus the one-liner every admin should walk away with.

---

## One-time setup

> **Before you run:**
> - The Lab-01 cluster must be up (3 brokers on `9092` / `9093` / `9094`).
> - `mvn -q -f kafka-clients/pom.xml compile` succeeds.
> - **One topic** — `transactions` — is used across every scenario.

```bash
./kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic transactions --partitions 3 --replication-factor 3
```

> **Admin lens:** All 16 scenarios reuse the same `transactions` topic. Scenarios that need "impossible ISR" (10, 13) tweak topic-level config on it via `kafka-configs.sh --alter` and undo the tweak at the end. The transactions scenario (12) sends multiple records to the same topic inside one atomic transaction — the isolation-level distinction still holds with one topic. One topic keeps the demo mental model simple; production would use multiple.

---

## Producer config quick reference

Every knob set explicitly in `KafkaProducerClient.java`. The scenario column points to where the value is actively changed.

| Section | Knob | Default in file | Scenario that changes it |
|---|---|---|---|
| Connection | `bootstrap.servers` | 3 seed brokers | – |
| Connection | `client.id` | `payments-producer-1` | 14 (quota) |
| Serialization | `key.serializer` / `value.serializer` | `StringSerializer` | – |
| Partitioning | `partitioner.class` | `PaymentTypePartitioner` | 2 (observe) |
| Batching | `batch.size` | 16 KB | 5 (buffer full), 7 (batching) |
| Batching | `linger.ms` | 5 ms | 5, 7 |
| Batching | `buffer.memory` | 32 MB | 5 |
| Batching | `compression.type` | `snappy` | 8 |
| Batching | `max.request.size` | 1 MB | 6 (record too big) |
| Reliability | `acks` | `all` | 9 |
| Reliability | `enable.idempotence` | `true` | 9 (turn off), 11 |
| Reliability | `retries` | `MAX_VALUE` | – |
| Reliability | `retry.backoff.ms` | 1000 | – |
| Reliability | `max.in.flight.requests.per.connection` | 5 | – |
| Timeouts | `max.block.ms` | 60 s | 5, 13 |
| Timeouts | `request.timeout.ms` | 30 s | 13 |
| Timeouts | `delivery.timeout.ms` | 120 s | 13 |
| Interceptors | `interceptor.classes` | commented out | 3 (uncomment) |
| Transactions | `transactional.id` | not set | 12 (add) |
| Serialization | `schema.registry.url` | not set | 17 |
| Placement | `client.rack` | not set | Concept reference |
| Placement | `client.dns.lookup` | `use_all_dns_ips` (default) | Concept reference |

---

## Scenario index

| # | Scenario | Kind of change |
|---|---|---|
| 1 | Baseline — full-config walkthrough | none |
| 2 | Partitioning by payment type | none (observe) |
| 3 | Interceptor hooks | uncomment one line |
| 4 | Broker down / metadata refresh | none (kill a broker) |
| 5 | Buffer full → `max.block.ms` | change 4 values |
| 6 | Record too big | change 1 value |
| 7 | Batching sweep — `linger.ms` × `batch.size` | change 2 values, run 3× |
| 8 | Compression codec | change 1 value, run 4× |
| 9 | `acks=0` vs `1` vs `all` | change 2 values, run 3× |
| 10 | `min.insync.replicas` floor | alter topic config, stop a broker |
| 11 | Idempotence toggle | change 1 value |
| 12 | Transactions | add ~10 lines |
| 13 | Timeout hierarchy | change 3 values + alter topic config |
| 14 | Client quota / throttle | change 1 value + `kafka-configs.sh` |
| 15 | JMX metrics | none (attach JConsole) |
| 16 | Benchmarking with `kafka-producer-perf-test.sh` | none (CLI tool) |
| 17 | Schema Registry / Avro | code walkthrough (requires Schema Registry) |
| 18 | Graceful shutdown — `producer.close()` | change 3 lines, run 2× |

---

# The scenarios

---

## 1. Baseline — full-config walkthrough

**What**

- Run the file as-is and tour every producer knob with the class open on screen.
- Every `props.put(...)` line has a two-bullet comment: *what* the setting does, *what* it costs.
- Seven numbered banner sections: Connection → Serialization → Partitioning → Batching → Reliability → Timeouts → Interceptors.
- This scenario is *"read the file top-to-bottom"* — the only scenario with no code edits.

**Change** — none.

**Run**

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.producer.KafkaProducerClient
```

- Sends 100 events at 1-second intervals.
- Leave running while you tour the seven sections in the file.
- `Ctrl+C` when done.

**See**

```
Sent key=upi         topic=transactions partition=0 offset=17
Sent key=neft        topic=transactions partition=0 offset=18
Sent key=credit_card topic=transactions partition=2 offset=9
```

- Cross-check on Kafka UI → Topics → `transactions` → Messages.
- Click one message → headers are visible (`event-type`, `correlation-id`, `content-type`, `schema-version`).

**Admin take-home**

- Every producer knob has a broker- or topic-level counterpart.
- This class is the reference against which real production configs should be compared.
- Every deviation from *this* file in a team's producer is a trade-off you should ask them to justify.

---

## 2. Partitioning by payment type

**What**

- The nested class `PaymentTypePartitioner` (at the bottom of the file) routes payment types to fixed partitions:
  - `neft`, `upi` → partition 0
  - `rtgs`, `net_banking` → partition 1
  - `imps`, `credit_card`, `debit_card`, `wallet` → partition 2
- The `partitioner.class` line at section 3 wires it in via the `$` FQCN.
- Routing is **100 % client-side** — brokers just validate partition numbers and store.

**Change** — none. Show the switch statement and the wiring line.

**Run**

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.producer.KafkaProducerClient
```

In another terminal:

```bash
./kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic transactions
```

**See**

```
Sent key=upi         partition=0
Sent key=rtgs        partition=1
Sent key=credit_card partition=2
```

**Admin take-home**

- Same key → same partition → per-key ordering. That's the guarantee most business processes rely on.
- Custom partitioners **age poorly**: adding partitions later (`kafka-topics.sh --alter --partitions 4`) does NOT retro-route; the new partition is dead space until the code redeploys.
- Default sticky partitioner (used when `partitioner.class` is not set) batches per partition better — pick custom only for business-meaning routing.

---

## 3. Interceptor hooks

**What**

- `LoggingProducerInterceptor` (nested at the bottom) prints on:
  - `onSend` — caller thread, before the record hits the accumulator.
  - `onAcknowledgement` — I/O thread, after the broker responds (or fails).
- Interceptors are a standard Kafka SPI for cross-cutting concerns:
  - Audit trails
  - Tracing header injection (`traceparent`)
  - Metrics counters per-topic
- Wired via the `interceptor.classes` line (currently commented out).

**Change** — uncomment the `interceptor.classes` line in section 7.

Find:

```java
// props.put("interceptor.classes",
//         "com.example.producer.KafkaProducerClient$LoggingProducerInterceptor");
```

Change to:

```java
props.put("interceptor.classes",
        "com.example.producer.KafkaProducerClient$LoggingProducerInterceptor");
```

**Run**

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.producer.KafkaProducerClient
```

**See**

- Two extra lines per record, on different threads:

```
[interceptor] onSend  topic=transactions key=upi valueBytes=145
Sent key=upi topic=transactions partition=0 offset=17
[interceptor] onAck   topic=transactions partition=0 offset=17
```

**Admin take-home**

- Client-side only. Brokers can't tell an interceptor is present.
- Common production use: injecting `traceparent` headers so a producer library that doesn't know about tracing still emits them.
- **Blocking work in `onSend` slows every `send()`** — trainees should ask what an interceptor actually costs before recommending one.

Re-comment the line after the demo so the file returns to baseline.

---

## 4. Broker down / metadata refresh

**What**

- Run the baseline, then kill one broker mid-run.
- The producer:
  - Sees `NetworkException` on the in-flight requests to the dead broker.
  - Refreshes cluster metadata (learns the new leader).
  - Retries against the new leader — records eventually land.
- With `acks=all` + `enable.idempotence=true` (defaults in the file), **no records are lost**.
- `delivery.timeout.ms` (2 min) bounds the total retry budget.

**Change** — none. This is an infrastructure action.

> **Before you run:**
> - Uncomment `Sender` and `Metadata` loggers in `src/main/resources/simplelogger.properties` so the retry chatter is visible.
> - Re-comment after the scenario.

**Run**

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.producer.KafkaProducerClient
```

Around record 15, `Ctrl+C` in any broker terminal.

**See**

```
Sent key=upi partition=0 offset=6
Error sending message: The server disconnected before a response was received.
Error sending message: For requests intended only for the leader, this error indicates the broker is not the current leader...
Sent key=upi partition=1 offset=5    ← metadata refreshed, new leader elected
```

**Admin take-home**

- Watch broker JMX `UnderReplicatedPartitions` — it climbs the instant one broker dies.
- Retry storm is bounded by `delivery.timeout.ms` on the client and `replica.lag.time.max.ms` on the broker.
- A producer that fails records here is either:
  - Using `acks=0` (see scenario 9), OR
  - Has `delivery.timeout.ms` too tight for the broker's recovery time.

---

## 5. Buffer full → `max.block.ms`

**What**

- The producer buffers records in memory (up to `buffer.memory`) before sending.
- If the buffer fills, `send()` blocks the caller thread.
- After `max.block.ms`, `send()` throws `TimeoutException` — **synchronously**, before the record ever reaches a broker.
- This is the ONE producer timeout that fires from `send()` itself. All others surface in the callback.

**Change** — four values in sections 4 and 6.

Sections 4 (Batching):

```java
props.put("batch.size", 16384);
props.put("linger.ms", 5);
props.put("buffer.memory", 33554432);
```

Change to:

```java
props.put("batch.size", 32 * 1024);
props.put("linger.ms", 60_000);
props.put("buffer.memory", 32 * 1024);
```

Section 6 (Timeouts):

```java
props.put("max.block.ms", 60000);
```

Change to:

```java
props.put("max.block.ms", 3_000);
```

**Run**

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.producer.KafkaProducerClient
```

**See**

- The first ~4 sends queue instantly.
- The 5th blocks 3 seconds and throws:

```
Sent key=upi partition=0
Sent key=neft partition=0
Sent key=rtgs partition=1
Sent key=imps partition=2
Error sending message: Expiring N record(s) for transactions-0: 3000 ms has passed since batch creation
```

**Admin take-home**

- **`max.block.ms` hurts the calling application, not just Kafka** — the request thread is blocked.
- Raise `buffer.memory` (or investigate why sends aren't draining) BEFORE raising `max.block.ms`.
- Broker JMX to watch: `bytes-in-rate` per topic; a sudden dip for one `client.id` means their buffer is full and `send()` is queueing.

Undo the changes after the demo.

---

## 6. Record too big

**What**

- The producer validates record size against `max.request.size` **client-side**, before the network.
- If too big → rejected locally with `RecordTooLargeException`; broker never sees it.
- Three ways the error surfaces:
  - **Fire-and-forget** `send()` — silently lost (this is #1 cause of "why did we lose records?" in prod).
  - **`Future.get()`** on the returned Future — throws `ExecutionException(cause=RecordTooLargeException)`.
  - **Callback** — receives `RecordTooLargeException` as the exception arg.

**Change** — one value in section 4.

```java
props.put("max.request.size", 1048576);
```

Change to:

```java
props.put("max.request.size", 1024);
```

The JSON payload each record produces is > 1 KB, so no payload change is needed.

**Run**

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.producer.KafkaProducerClient
```

**See**

- Every callback fires with:

```
Error sending message: The message is 2114 bytes when serialized which is larger than 1024, which is the value of the max.request.size configuration.
```

**Admin take-home**

- Producer `max.request.size` and broker `message.max.bytes` MUST be raised together. Either alone breaks.
- Also revisit downstream:
  - `replica.fetch.max.bytes` on the broker (replication).
  - `fetch.max.bytes` / `max.partition.fetch.bytes` on consumers.
- If you can't raise the caps, use a schema+reference pattern: put the big payload in object storage, put the reference in Kafka.

---

## 7. Batching sweep — `linger.ms` × `batch.size`

**What**

- Show how batching turns per-record latency into throughput.
- Same workload, three different `(linger.ms, batch.size)` pairs.
- Records/sec climbs as batching gets more aggressive; per-record latency climbs a little.

**Change** — sections 4. Run once per pair.

| Run | `linger.ms` | `batch.size` |
|-----|-------------|--------------|
| a   | 0           | 16384        |
| b   | 5           | 16384        |
| c   | 50          | 131072       |

**Run** (three times, one per pair)

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.producer.KafkaProducerClient
```

**See**

- Count wall-clock time between the first and 100th `Sent key=...` line for each run.
- Run (a): 1 record per batch (baseline overhead).
- Run (c): batches fill to 128 KB before dispatch (bigger, less frequent produce requests).

**Admin take-home**

- `linger.ms=0` is the default in many misconfigured producers → every send is its own network round-trip → wasted overhead.
- Bumping to 5 ms trades tiny latency for a big throughput win.
- **How to spot an under-batched producer from the broker:**
  - `bytes-in-rate` per topic ÷ `messages-in-rate` = average batch bytes.
  - If a specific `client.id` has `records-per-request-avg ≈ 1` in JMX, they forgot to set `linger.ms`.

Undo the changes after the demo.

---

## 8. Compression codec

**What**

- Rotate `compression.type` through `none`, `snappy`, `lz4`, `zstd`.
- Compression happens **per batch, on the producer**. The broker stores what it receives.
- Payload shape matters more than the codec:
  - JSON, text, repeated structure → compresses a lot.
  - Random bytes, already-compressed content → barely compresses.

**Change** — one value in section 4.

```java
props.put("compression.type", "snappy");
```

Rotate the value: `"none"`, `"snappy"`, `"lz4"`, `"zstd"`.

**Run** (once per codec)

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.producer.KafkaProducerClient
```

**See**

Measure the segment size on any one broker after each codec's run:

```bash
du -sh kafka-lab/data/broker-1/transactions-0
```

Expect (for JSON-like payloads):

| Codec | On-wire bytes vs baseline | CPU cost |
|-------|---------------------------|----------|
| none   | 1×    | zero |
| snappy | ~0.4× | low |
| lz4    | ~0.4× | low |
| zstd   | ~0.2× | higher |

**Admin take-home**

- Broker `compression.type=producer` (default) means "store what the producer sent" — best throughput.
- Overriding at topic level forces broker to recompress on write — rarely a good idea.
- Consumer CPU cost order: `zstd > snappy ≈ lz4`. Ask trainees which end of the pipeline is CPU-bound before recommending.
- Broker JMX to watch: `bytes-in-rate` (post-compression on the wire) vs disk segment size (compressed at rest).

---

## 9. `acks=0` vs `1` vs `all`

**What**

- Rotate `acks` through 0, 1, and `all`.
- Kill the leader broker around record 30 in each run.
- Compare **producer-reported sends** vs **on-disk record count**.

| `acks` | Meaning | Failure mode |
|--------|---------|---------------|
| 0 | Fire-and-forget; no ack from broker | Silent loss on broker outage. Callback lies. |
| 1 | Leader ack only | Records acked by leader but not replicated die with the leader. |
| all | All in-sync replicas | Never silently loses; failures surface loudly. |

**Change** — two values in section 5.

```java
props.put("acks", "all");
props.put("enable.idempotence", "true");
```

- Rotate `acks`: `"0"`, `"1"`, `"all"`.
- **Also temporarily** change `enable.idempotence` to `"false"` for all three runs. (Idempotence forces `acks=all`; disabling it makes `acks` the only variable.)

**Run** (three times)

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.producer.KafkaProducerClient
```

Kill the leader broker around record 30 each time. Then count what actually landed:

```bash
./kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions --from-beginning --timeout-ms 5000 | wc -l
```

**See**

| Run | Producer says sent | On disk | Meaning |
|-----|--------------------|---------|---------|
| `acks=0` | 100 | often < 100 | Silent loss — callback lied. |
| `acks=1` | 100 | some missing | Leader acked, died before replicating. |
| `acks=all` | ~95 acked, ~5 failed | equals acked count | Never silently loses; failures surface. |

**Admin take-home**

- **`acks=1` is the sneaky one.** Everything looks fine until the leader dies with unreplicated writes.
- `acks=all` alone doesn't guarantee durability — it needs `min.insync.replicas` on the topic to make "in-sync" mean something. Scenario 10 shows this.
- Broker-side signal: `record-error-rate` on the producer's JMX matches this ratio.

Undo both changes after the demo.

---

## 10. `min.insync.replicas` floor

**What**

- `acks=all` waits for all *in-sync* replicas — not all replicas.
- `min.insync.replicas` is a topic-level (or broker-level) floor.
- If ISR count < floor, broker refuses writes with `NotEnoughReplicasException`.
- This is the true "durability contract" — `acks=all` alone is not enough.

**Change** — no code change. Tweak topic config on `transactions`.

Set the floor to 3 on the topic:

```bash
./kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type topics --entity-name transactions --add-config min.insync.replicas=3
```

> **Before you run:**
> - Stop one broker (`Ctrl+C` in its terminal) so ISR drops to 2 — below the floor of 3.
> - Verify the floor is set and ISR is short:
>   ```bash
>   ./kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic transactions
>   ```
>   Look for `Configs: min.insync.replicas=3` and `Isr: <only two broker ids>`.

**Run**

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.producer.KafkaProducerClient
```

**See**

- Every callback fires with:

```
Error sending message: Messages are rejected since there are fewer in-sync replicas than required.
```

**Admin take-home**

- **`acks=all` × `min.insync.replicas` is the only pair that makes a real durability contract.** Either alone is weaker.
- Recommend the strong-durability default:
  - `replication.factor=3`
  - `min.insync.replicas=2`
  - `acks=all`
  - `enable.idempotence=true`
- Do NOT recommend `min.insync.replicas=3` on `RF=3` — you lose availability the moment ANY broker outages.
- Restart the stopped broker, wait ~5 s, rerun — writes succeed once ISR catches up.

**Cleanup** (undo the topic override AND restart the broker before moving on):

```bash
./kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type topics --entity-name transactions --delete-config min.insync.replicas
```

---

## 11. Idempotence toggle

**What**

- Without idempotence:
  - A broker persists a record but its ack response is lost.
  - Producer retries → **duplicate on disk**.
- With idempotence:
  - Every batch carries `(PID, sequence)`.
  - Broker keeps a small window per producer per partition and drops replays.
  - No duplicates on disk.
- Dedup scope is a **single producer instance**. Restart = new PID = broker can't link.

**Change** — one value in section 5.

```java
props.put("enable.idempotence", "true");
```

Change to:

```java
props.put("enable.idempotence", "false");
```

**Run** (twice — once with each setting; kill and restart the leader mid-run to force retries)

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.producer.KafkaProducerClient
```

- Around record 30, `Ctrl+C` the leader broker; restart it ~5 s later.
- Count on-disk records:

```bash
./kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions --from-beginning --timeout-ms 5000 | wc -l
```

**See**

- Idempotence `true` → exactly 100 records on disk regardless of retries.
- Idempotence `false` → more than 100 if any retry got through — duplicates.

**Admin take-home**

- Idempotence costs one `InitProducerId` round-trip at producer start. Negligible.
- No reason to leave this off in production. Recommend it as a mandatory setting.
- Watch producer JMX `record-retry-rate` — spikes here without duplicates on disk = idempotence is working.

Undo the change after the demo.

---

## 12. Transactions

**What**

- Wrap a group of sends in a single transaction.
- Kafka writes ALL records (including aborted) with control markers.
- `read_committed` consumers get "skip" instructions from the broker for aborted records.
- Enables:
  - Atomic multi-topic writes.
  - Read-process-write pipelines with exactly-once semantics.

**Change** — this scenario adds ~10 lines. First, add to section 1:

```java
props.put("transactional.id", "demo-tx-1");
```

Then replace the entire 100-record `for` loop:

```java
for (int i = 0; i < 100; i++) {
    // ... existing send code ...
    TimeUnit.MILLISECONDS.sleep(1000);
}
```

with:

```java
producer.initTransactions();
for (int i = 0; i < 10; i++) {
    producer.beginTransaction();
    String key = "ord-" + i;
    producer.send(new ProducerRecord<>(TOPIC, key, "order:" + i));
    producer.send(new ProducerRecord<>(TOPIC, key, "created:" + i));
    if (i % 2 == 0) {
        producer.commitTransaction();
        System.out.println("[" + i + "] COMMITTED");
    } else {
        producer.abortTransaction();
        System.out.println("[" + i + "] ABORTED");
    }
    TimeUnit.MILLISECONDS.sleep(500);
}
```

Each transaction writes TWO records to the SAME `transactions` topic. The atomicity story still lands — the transaction either commits both records or aborts both.

**Run**

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.producer.KafkaProducerClient
```

- Open two more terminals and compare isolation levels:

```bash
./kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions --from-beginning
./kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions --from-beginning --isolation-level read_committed
```

**See**

- `read_uncommitted` (default) → 20 records visible (2 per transaction × 10 transactions).
- `read_committed` → 10 records visible (2 per committed transaction × 5 committed).

**Admin take-home**

- A transaction spans **multiple topics AND partitions** — atomicity is at the transaction level (this demo simplifies to one topic; the guarantee is the same across topics).
- Cost: one round-trip per commit/abort + coordinator state. Do NOT wrap tiny individual sends.
- Broker knobs to know:
  - `transaction.state.log.replication.factor` (default 3) — never lower it.
  - `transaction.state.log.min.isr` — for the internal `__transaction_state` topic.
- Two producers sharing `transactional.id` → the newer one **fences** the older (`ProducerFencedException`).

Undo the code changes to restore the baseline.

---

## 13. Timeout hierarchy

**What**

- Three timeouts govern producer behaviour:

| Timeout | Scope | Where fires |
|---------|-------|-------------|
| `max.block.ms` | Waiting on `send()` (buffer full / metadata missing) | Throws from `send()` synchronously |
| `request.timeout.ms` | Per broker round-trip | Retries loop; NEVER surfaces alone |
| `delivery.timeout.ms` | Overall `send()` → callback wall clock | Callback receives `TimeoutException` |

- Constraint enforced by client: `delivery.timeout.ms >= linger.ms + request.timeout.ms`.

**Change** — three timeout values in section 6, plus a topic-config tweak on `transactions`.

Section 6:

```java
props.put("max.block.ms", 60000);
props.put("request.timeout.ms", 30000);
props.put("delivery.timeout.ms", 120000);
```

Change to:

```java
props.put("max.block.ms", 5_000);
props.put("request.timeout.ms", 1_000);
props.put("delivery.timeout.ms", 3_000);
```

Set an impossible ISR floor on `transactions` so every send is rejected retriably (all brokers up):

```bash
./kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type topics --entity-name transactions --add-config min.insync.replicas=99
```

RF=3 with floor=99 is unsatisfiable → broker rejects every send with `NotEnoughReplicasException` until `delivery.timeout.ms` expires.

**Run**

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.producer.KafkaProducerClient
```

**See**

- Every send fails after ~3 s with:

```
Error sending message: Expiring N record(s) for transactions-0: 3000 ms has passed since batch creation
```

- The callback surfaces `TimeoutException` (batch expired) — NOT the underlying `NotEnoughReplicasException`.

**Admin take-home**

- When a team reports *"producer timeout"*, the **first** question is *which one*. Different roots, different fixes.
- `request.timeout.ms` never surfaces alone — it drives retries; something else ultimately throws.
- `delivery.timeout.ms` is the SLA callers see. Callbacks always fire within that budget.
- Tuning guidance:
  - `max.block.ms` too low → app throws `TimeoutException` from `send()`; investigate buffer.
  - `request.timeout.ms` too low → retry storm; investigate broker latency.
  - `delivery.timeout.ms` too low → premature callback failures; investigate root cause.

**Cleanup** (undo the topic override AND revert the three timeout values):

```bash
./kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type topics --entity-name transactions --delete-config min.insync.replicas
```

---

## 14. Client quota / throttle

**What**

- Attach a per-`client.id` produce quota via `kafka-configs.sh`.
- The broker enforces by **delaying** the produce response — no rejection.
- Records are never lost; latency climbs.
- Kinds of quota:
  - `producer_byte_rate` — bytes/sec produce cap.
  - `consumer_byte_rate` — bytes/sec fetch cap.
  - `request_percentage` — CPU time cap.

**Change** — one value in section 1.

```java
props.put("client.id", "payments-producer-1");
```

Change to:

```java
props.put("client.id", "quota-demo-client");
```

Attach a low quota (8 KB/s):

```bash
./kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type clients --entity-name quota-demo-client --add-config 'producer_byte_rate=8192'
```

Verify:

```bash
./kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type clients --entity-name quota-demo-client
```

**Run**

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.producer.KafkaProducerClient
```

**See**

- `Sent key=...` lines trickle out instead of streaming.
- Broker is delaying each response.
- `Ctrl+C` when the point has landed.

**Cleanup** (undo the quota):

```bash
./kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type clients --entity-name quota-demo-client --delete-config 'producer_byte_rate'
```

**Admin take-home**

- Quotas are **broker-side** enforcement. Client sees latency, then buffer pressure — never errors (until the buffer fills up and `max.block.ms` fires).
- The definitive quota signal: producer JMX `produce-throttle-time-avg > 0`.
- Common quota targets:
  - Multi-tenant clusters — cap noisy producers by `client.id`.
  - SASL-authenticated clusters — cap by `user`.
- **Never** set a quota without measuring first (scenario 16).

Undo the `client.id` change after the demo.

---

## 15. JMX metrics

**What**

- Every `KafkaProducer` exposes an in-JVM metrics registry via `producer.metrics()`.
- Same numbers published as JMX MBeans under:

```
kafka.producer:type=producer-metrics,client-id=<client.id>
```

- Prometheus JMX exporter, Grafana dashboards, DataDog integrations all walk this tree.
- The `client.id` from your producer config = the dashboard label. Choose deliberately.

**Change** — none.

**Run**

Terminal 1:

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.producer.KafkaProducerClient
```

Terminal 2:

```bash
jconsole
```

- Attach to the running JVM (usually shows as `org.codehaus.plexus.classworlds.launcher.Launcher`).
- Expand `kafka.producer → producer-metrics → client-id = payments-producer-1`.

**See** — the "must scrape" set:

| Metric | Alert when… |
|--------|-------------|
| `record-send-rate` | Should match your target rate. |
| `record-error-rate` | > 0 sustained — records failing async. |
| `record-retry-rate` | Elevated — cluster or network unstable. |
| `request-latency-avg` | > SLO ceiling — broker or network slow. |
| `buffer-available-bytes` | Approaching 0 — producer under-batched or overwhelmed. |
| `produce-throttle-time-avg` | > 0 — quota active on this `client.id`. |
| `records-per-request-avg` | Low → under-batched (see scenario 7). |
| `outgoing-byte-rate` | Bytes/s on the wire (post-compression). |

**Bonus classroom moment:** with the demo still running, kill a broker. Watch `record-retry-rate` climb, `request-latency-avg` spike. That's what an alert looks like live.

**Admin take-home**

- These are the numbers a production dashboard row should show. Nothing more, nothing less.
- Metric names are identical between the code (`producer.metrics()`), JMX, and every JMX exporter — one vocabulary.
- Every prior scenario resurfaces here as a metric:
  - Scenarios 4, 11 → `record-retry-rate`.
  - Scenario 5 → `buffer-available-bytes`.
  - Scenario 7 → `records-per-request-avg`.
  - Scenario 8 → `outgoing-byte-rate`.
  - Scenario 14 → `produce-throttle-time-avg`.

---

## 16. Benchmarking with `kafka-producer-perf-test.sh`

**What**

- The industry-standard CLI benchmark tool. Ships with the Kafka distribution.
- One command produces:
  - Records/sec throughput.
  - MB/sec throughput.
  - Average latency, max latency.
  - p50 / p95 / p99 / p99.9 latency percentiles — the numbers SLAs are written against.

**Change** — none. This is a separate CLI tool, not the Java class.

**Run** (baseline)

```bash
./kafka/bin/kafka-producer-perf-test.sh --topic transactions --num-records 100000 --record-size 200 --throughput -1 --producer-props bootstrap.servers=localhost:9092,localhost:9093,localhost:9094 acks=all
```

**See**

```
100000 records sent, 47846.9 records/sec (9.13 MB/sec), 52.2 ms avg latency,
351.0 ms max latency, 26 ms 50th, 210 ms 95th, 296 ms 99th, 340 ms 99.9th.
```

Decode:

- **records/sec, MB/sec** — throughput.
- **avg latency, max latency** — mean and worst per record.
- **50th / 95th / 99th / 99.9th** — percentile latencies in ms.

**Key flags**

| Flag | What it does |
|------|--------------|
| `--num-records N` | Total records to send. ≥ 100 000 for good stats. |
| `--record-size N` | Bytes per record (random payload). |
| `--throughput N` | Target records/sec; `-1` = unlimited (measures max). Positive N rate-limits and measures latency at that rate. |
| `--producer-props k=v ...` | Inline producer config. Space-separated `key=value`. |
| `--producer.config <file>` | Load props from a file. Preferred for repeatable runs. |
| `--payload-file <path>` | Use realistic payloads instead of random bytes (critical for compression benchmarks). |
| `--transactional-id <id>` | Turn on the transactional producer path. |

**Comparison recipes**

`acks=1` vs `acks=all`:

```bash
./kafka/bin/kafka-producer-perf-test.sh --topic transactions --num-records 500000 --record-size 200 --throughput -1 --producer-props bootstrap.servers=localhost:9092 acks=1 linger.ms=5 compression.type=snappy
./kafka/bin/kafka-producer-perf-test.sh --topic transactions --num-records 500000 --record-size 200 --throughput -1 --producer-props bootstrap.servers=localhost:9092 acks=all linger.ms=5 compression.type=snappy
```

All four codecs in one loop:

```bash
for CODEC in none snappy lz4 zstd; do echo "=== compression.type=$CODEC ==="; ./kafka/bin/kafka-producer-perf-test.sh --topic transactions --num-records 500000 --record-size 400 --throughput -1 --producer-props bootstrap.servers=localhost:9092 acks=all compression.type=$CODEC; done
```

Rate-limited latency (what's p99 at 30 000 records/sec?):

```bash
./kafka/bin/kafka-producer-perf-test.sh --topic transactions --num-records 500000 --record-size 200 --throughput 30000 --producer-props bootstrap.servers=localhost:9092 acks=all linger.ms=5
```

**Admin take-home**

- **An admin who doesn't know this tool is guessing.** Every capacity planning conversation, every producer config change, every "is the cluster overloaded?" question should be measured with this.
- Save the summary line before deploying anything to production — it becomes your baseline.
- Iron rule: **always run 3+ times, take the median.** JIT warmup, page cache, GC ruin single-shot numbers.
- **99.9th percentile latency is the SLA number.** Averages hide the tail.
- Random payloads (`--record-size`) exercise network + broker, but compression ratios are unrealistic. Use `--payload-file` with real samples.
- Tool is **single-threaded**. For parallel producer simulation, launch multiple instances with distinct `client.id`.

---

## 17. Schema Registry / Avro

**What**

- Production producers rarely send raw JSON. Typed payloads with schemas and evolution rules dominate.
- **Schema Registry** is a standalone service (Confluent's, Apicurio, Redpanda's) that stores Avro/Protobuf/JSON schemas by *subject*.
- The producer sends payloads prefixed with a 4-byte schema ID; consumers look up the schema by ID to decode.
- **Compatibility modes** govern schema evolution:
  - **BACKWARD** (default) — new schema can read old data.
  - **FORWARD** — old schema can read new data.
  - **FULL** — both; safest, most restrictive.
  - **NONE** — no checks; don't use.
- Registry is a **critical prod dependency**; its outage stops your producers.

**Change** — swap the JSON path for Avro. First add the dependency to `pom.xml`:

```xml
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-avro-serializer</artifactId>
    <version>7.5.0</version>
</dependency>
```

(Requires the Confluent Maven repo in the `<repositories>` block of `pom.xml`.)

Then change section 2 (Serialization) of `KafkaProducerClient.java`:

```java
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
```

Change to:

```java
props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
props.put("schema.registry.url", "http://localhost:8081");
```

The producer loop's `mapper.writeValueAsString(event)` becomes an Avro `GenericRecord` (or a code-generated `SpecificRecord`) construction. See the Confluent Avro quickstart for the full API.

> **Before you run:** Schema Registry must be running at the URL configured. Lab-01 does not include it. This is a **code walkthrough** — study the diffs; run against a real registry later.

**Run**

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.producer.KafkaProducerClient
```

**See**

- Against Lab-01 (no registry): `Error sending message: java.net.ConnectException: Connection refused` at the registry URL.
- Against a running registry: bytes on the wire have a 5-byte prefix (magic byte + 4-byte schema ID); Kafka UI shows Avro records with a schema-ID column.

**Admin take-home**

- Schema Registry is a **critical prod dependency**. Run it HA (multiple replicas, backed by a replicated Kafka topic `_schemas` with RF ≥ 3).
- **Compatibility mode is a business decision, not a tech one.** `BACKWARD` is the safest default for the ecosystem.
- Every schema change goes through the registry — it's your **contract test** for producer ↔ consumer compatibility.
- Avro is fastest and smallest for structured data. Protobuf is close (better cross-language ergonomics). JSON Schema is verbose and slow — use only if payloads must stay human-readable.
- Subject naming strategy: `TopicNameStrategy` (default) means one schema per topic; `RecordNameStrategy` allows multiple record types per topic.
- Watch registry HTTP latency alongside broker health — a slow registry stalls producers.

Undo the changes (or `git checkout`) after the demo.

---

## 18. Graceful shutdown — `producer.close()`

**What**

- When your app receives SIGTERM (rolling deploy, Kubernetes pod termination), it typically runs `producer.close()`.
- `producer.close()` with no arg waits `Long.MAX_VALUE` (forever) for in-flight records to complete — safe but can hang deploys.
- `producer.close(Duration.ofMillis(N))` waits AT MOST N ms — after which:
  - Records still in the buffer that haven't been sent are **silently dropped**.
  - In-flight records that haven't been acked are also lost.
- `producer.close(Duration.ZERO)` immediately aborts — everything in flight is lost.
- Getting this wrong = **silent data loss during rolling deploys.** The #1 cause of *"we ship reliably 99.9 % of the time except during releases"*.

**Change** — three targeted edits in `main()`. First, change the loop for a bursty send:

```java
for (int i = 0; i < 100; i++) {
```

Change to:

```java
for (int i = 0; i < 5000; i++) {
```

Remove the sleep line inside the loop:

```java
TimeUnit.MILLISECONDS.sleep(1000);
```

Change the close call at the end:

```java
producer.close();
```

Change to (for the aggressive-timeout run):

```java
producer.close(java.time.Duration.ofMillis(100));
```

**Run** (twice — once with the aggressive timeout, once with the default)

```bash
cd kafka-clients && mvn -q compile exec:java -Dexec.mainClass=com.example.producer.KafkaProducerClient
```

After each run, count on-disk records:

```bash
./kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions --from-beginning --timeout-ms 5000 | wc -l
```

**See**

| Run | Close call | Sent | On disk |
|-----|-----------|------|---------|
| a | `producer.close(java.time.Duration.ofMillis(100))` | 5000 | often 3000–4500 (records dropped) |
| b | `producer.close()` (default; wait forever) | 5000 | 5000 (all flushed) |

**Admin take-home**

- **Every rolling deploy is a scenario-19 event.** If the pod terminates before `producer.close()` finishes draining, records vanish.
- Recommend `producer.close(Duration.ofSeconds(30))` as the application default — enough for a full buffer to drain under normal broker latency, capped so deploys can't hang indefinitely.
- **Buffer size is your worst-case backlog.** Drain time ≈ `buffer.memory / outgoing-byte-rate`. Bigger buffer = bigger drain requirement.
- Kubernetes: set `terminationGracePeriodSeconds` ≥ your `close()` timeout + margin. Default 30 s is often too short for producers with a full buffer.
- Watch `record-drop-rate` in producer JMX during rolling deploys — it's the smoking gun.
- Loss during deploy is a **distinct failure mode** from the retry story in scenarios 4 / 9 / 11. Different symptom, different fix.

Reset the file (or `git checkout`) after the demo.

---

# Reference — failure signatures

Quick lookup for *"my producer is doing X — what fired?"*.

| Symptom | Which knob / broker signal fired | Fix |
|---------|----------------------------------|-----|
| `send()` blocks caller then throws `TimeoutException` | `max.block.ms` (buffer full OR metadata missing) | Raise `buffer.memory`; check upstream drain |
| Callback: `TimeoutException: Expiring N record(s) ...` | `delivery.timeout.ms` — retries exhausted the budget | Raise `delivery.timeout.ms` if broker recovery is genuine; investigate cluster |
| Callback: `NotEnoughReplicasException` | Broker: `min.insync.replicas` > current ISR | Restart the stopped broker; loosen the floor only if you can accept under-replicated writes |
| Callback: `RecordTooLargeException` | Client: `max.request.size` exceeded | Raise producer + broker `message.max.bytes` together |
| Callback: `NetworkException` / `NotLeaderOrFollowerException` (retriable) | Broker down or leader change | Wait — retry handles it; check `UnderReplicatedPartitions` |
| Silent record loss on broker outage | `acks=0` or `acks=1` — producer didn't wait for durability | Set `acks=all` + `min.insync.replicas=2` |
| Duplicates on disk after a broker blip | `enable.idempotence=false` | Set `enable.idempotence=true` — always |
| Callback: `ProducerFencedException` | Another producer with same `transactional.id` took over | Investigate — you shouldn't have two producers sharing an id |
| Latency spikes but no errors | Quota (`produce-throttle-time-avg > 0`) OR broker slowness | Check `produce-throttle-time-avg`; if zero → cluster issue |
| High `record-error-rate` + full buffer | Retry storm — cluster is failing OR quota too tight | Check `record-retry-rate`; adjust quota or fix broker |

---

# Tuning cheat sheet

Levers and the direction they move things.

### For throughput (records/sec)

- **Raise `linger.ms`** (0 → 5 → 50). More records per batch → fewer requests → higher throughput.
- **Raise `batch.size`** (16 KB → 128 KB). Fills more when linger permits.
- **Turn on `compression.type=snappy`** (or `zstd` if bandwidth-limited). Smaller batches on the wire.
- **Match `buffer.memory`** to peak burst (default 32 MB is usually fine).
- **Measure with `kafka-producer-perf-test.sh`** — scenario 16. Random-payload throughput ≠ prod throughput.

### For low latency (per-record)

- **`linger.ms=0`** (accepts throughput cost).
- **`acks=1`** if you can tolerate silent loss (usually you can't).
- **Tight `request.timeout.ms`** only if broker is genuinely fast — otherwise you cause retry storms.
- **Small batches** — but the network round-trip dominates at that point anyway.

### For durability (no data loss)

- `acks=all` × `min.insync.replicas=2` on `replication.factor=3`.
- `enable.idempotence=true` — always.
- `retries=MAX_VALUE`; let `delivery.timeout.ms` bound the total wall clock.
- `unclean.leader.election.enable=false` on the broker.

### For memory safety (no `OutOfMemoryError`)

- Bound `buffer.memory` explicitly (don't rely on defaults).
- Bound `max.block.ms` so blocked callers can time out and shed load.
- Bound `delivery.timeout.ms` so records don't queue forever.

---

# Quick concept reference

Four cross-cutting concepts that don't have their own scenario but come up across many.

## Send semantics — three ways an error surfaces

Every `producer.send(...)` returns a `Future<RecordMetadata>` AND optionally takes a callback. You get to pick how failures surface:

- **Fire-and-forget** — `producer.send(record);`
  - Return value ignored. Exception silently swallowed.
  - **The #1 cause of** *"why did we lose records?"* in production. Never do this.
- **`Future.get()`** — `producer.send(record).get();`
  - Blocks until the callback would fire.
  - Throws `ExecutionException` whose `.getCause()` is the actual exception (e.g. `RecordTooLargeException`).
  - Turns async producer into synchronous — kills throughput.
- **Callback** — `producer.send(record, (metadata, exception) -> { ... });`
  - Async; the callback runs on the producer I/O thread when the broker acks (or on final failure).
  - **The right pattern for production code.**
  - Same thread as `ProducerInterceptor.onAcknowledgement`.

Scenario 6 (`RecordTooLargeException`) demonstrates all three paths.

## Record headers

The demo attaches five headers to every record — visible in the code and in Kafka UI:

| Header | Purpose |
|--------|---------|
| `event-type` | What kind of event (`payment.initiated`) — routing without parsing payload. |
| `source` | Who produced it — helps trace which producer instance sent this. |
| `correlation-id` | Ties this record to a broader request/response chain (tracing). |
| `content-type` | Payload format (`application/json`) — future-proof for schema evolution. |
| `schema-version` | Payload version — needed when schemas change but topic doesn't. |

Headers are **first-class Kafka metadata**:
- Consumers read them without deserialising the payload.
- Tracing systems (`traceparent`, `tracestate`) rely on this pattern.
- Broker routing based on headers is possible with connectors / stream processors.
- Verify from CLI: `./kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions --from-beginning --property print.headers=true`.

## Ordering guarantees

Kafka guarantees ordering **per partition** (never across partitions).

The knobs that affect ordering within a partition:

- **`max.in.flight.requests.per.connection`** (default 5):
  - > 1 without idempotence → a failed retry can arrive out of order.
  - With `enable.idempotence=true`, up to 5 in flight still preserves order (broker uses sequence numbers to reorder).
- **`enable.idempotence=true`** (default `true` since Kafka 3.0):
  - Preserves ordering even with retries.
  - Recommended default for every production producer.
- **`retries`** (`MAX_VALUE` in this file):
  - Retries within `delivery.timeout.ms`.
  - Without idempotence, retries can cause both duplicates AND reordering.

**Rule of thumb:** for strict per-key ordering, use the same key on every related record, don't set a custom partitioner that ignores the key, and keep `enable.idempotence=true`.

## Retry semantics

Three knobs govern retry behaviour:

| Knob | Purpose |
|------|---------|
| `retries` | Maximum retry attempts per batch. Set to `MAX_VALUE` and let time bound it. |
| `retry.backoff.ms` | Wait between consecutive retry attempts (per batch). Default 100 ms; 1000 ms is safer under broker outage. |
| `delivery.timeout.ms` | Total wall-clock budget for the record. When exhausted, the batch expires (callback receives `TimeoutException`). |

- **Retriable errors** include `NetworkException`, `NotLeaderOrFollowerException`, `NotEnoughReplicasException`, and `TimeoutException` (per-request).
- **Non-retriable errors** include `RecordTooLargeException`, `SerializationException`, `AuthorizationException`. Retries won't help; fix the record or the auth.
- **Total time attempting** = min(`retries × (retry.backoff.ms + per-request time)`, `delivery.timeout.ms`). In practice, `delivery.timeout.ms` is the binding constraint.

Scenarios 4 (broker down), 11 (idempotence), and 13 (delivery timeout) all exercise this loop.

## Producer thread model

- Every `KafkaProducer` starts **one background thread**: the **Sender**. It is the only thread that talks to brokers.
- Your application threads (any number) call `producer.send(...)`; those threads only **queue records into the buffer** and return immediately.
- The Sender drains the buffer, batches per partition, dispatches produce requests, and delivers callbacks.
- **Callbacks run on the Sender thread.** Blocking work in a callback blocks ALL subsequent sends for that producer.

Practical implications:

- A slow callback = a slow producer. Don't do I/O, don't parse, don't wait on locks inside a callback.
- Kafka is designed for **one producer per role, shared by many app threads**. Don't create a `KafkaProducer` per request; create one at startup, share it.
- The producer is **thread-safe** — many threads can call `send()` on the same instance concurrently.

## Sticky partitioner (Kafka's default when `partitioner.class` is unset)

- Kafka's `DefaultPartitioner` is "sticky":
  - For records **with a key**: hash the key mod partitions → deterministic mapping.
  - For records **without a key**: **stick** to one partition until the current batch fills, then rotate.
- Sticky rotation increases batch fullness → better compression → higher throughput.
- Scenario 2's `PaymentTypePartitioner` deliberately gives up sticky behaviour because business ordering demands specific routing.

Rule of thumb:

- If per-key ordering matters and you can set a good key, **use the default partitioner** — don't write a custom one.
- Custom partitioners are for rare cases where the key can't be shaped and business rules dictate placement.

## Rack-awareness — `client.rack`

- In multi-AZ or stretch clusters, cross-zone bandwidth is expensive and slower than same-zone.
- Setting `client.rack=us-east-1a` on the producer tells the broker *"I'm in this rack; prefer replicas in this rack when acknowledging."*
- Requires broker-side `broker.rack=<rack-id>` on every broker AND a rack-aware replica placement policy on topics.
- Effect: writes prefer same-rack replicas for acks → lower `request-latency-avg`, lower egress cost.

Admin note:

- If your producers span multiple AZs and your Kafka bill has *"Data Transfer"* as its biggest line item, `client.rack` alone can cut it by 30–70 %.
- Combines well with `RackAwareReplicaSelector` on consumers.

## DNS resolution — `client.dns.lookup`

- Default in Kafka 3.0+ is `use_all_dns_ips` — the client resolves the bootstrap hostname to ALL DNS A records and tries each in turn.
- Older default was `default` — first IP only; fails hard if that broker is down.
- With `use_all_dns_ips`:
  - `bootstrap.servers=kafka.mycompany.com:9092` behind a DNS round-robin JUST WORKS.
  - Broker failures don't require producer restart.

Admin note:

- Verify: `dig +short kafka.mycompany.com` should return multiple A records.
- Combined with a load balancer or DNS round-robin in front of the brokers, HA becomes DNS-configured, not code-configured.
- Watch `connection-count` and `connection-close-rate` in JMX to see the client rotating through DNS entries.

## Health check from JMX — is my producer alive?

Two signals distinguish a healthy producer from a stuck one:

- `record-send-rate > 0` for the last N seconds — the producer is actively sending.
- `buffer-available-bytes > threshold` — the producer isn't drowning in queued records.

A dead producer:

- `record-send-rate = 0` AND `buffer-available-bytes` low (buffer full but nothing sending).
- Root causes: Sender thread stuck, broker unreachable, app deadlocked in a callback.

Recommended Kubernetes liveness probe:

- Expose the two metrics via Micrometer / Prometheus.
- Fail liveness when `record-send-rate = 0` for > 60 s while the target rate should be > 0.
- Restart the pod on failure — `KafkaProducer` is not designed to be resurrected in place.

Combine with:

- `record-error-rate` and `record-retry-rate` from scenario 15.
- Alerts on `produce-throttle-time-avg > 0` (scenario 14).
- Alerts on `buffer-available-bytes` approaching 0.

---

# Admin cheat sheet

## KNOW — concepts every admin must internalise

| Question | Where |
|---|---|
| What producer knobs exist? | Scenario 1 (baseline) + config quick reference at top of this doc |
| Three timeouts — which fires when? | Scenarios 5, 13 + failure signatures table |
| What does each `acks` level guarantee? | Scenario 9 |
| What is `min.insync.replicas` really doing? | Scenario 10 |
| Does the producer dedupe on retry? | Scenario 11 |
| Can the producer write atomically? | Scenario 12 |
| What client-side failure modes show up in prod? | Scenarios 4, 5, 6 + failure signatures |
| How do I measure a change's impact? | Scenario 16 (benchmarking) |
| How does typed-payload schema evolution work? | Scenario 17 |
| How do I close the producer safely during a rolling deploy? | Scenario 18 |

## WATCH — signals to scrape and alert on

| Signal | Scenario |
|---|---|
| Full "must-scrape" JMX set | 15 |
| Batching quality (`records-per-request-avg`) | 7 |
| Compression effectiveness | 8 |
| Quota pressure (`produce-throttle-time-avg`) | 14 |
| Retry storm signature (`record-retry-rate`) | 4, 11 |
| Under-replicated writes (`NotEnoughReplicasException`) | 10 |
| Silent loss risk | 9 (change `acks` and compare on-disk vs sent) |
| Records dropped during shutdown | 18 (`record-drop-rate` during rolling deploys) |
| Schema Registry unreachable | 17 (`ConnectException` on `schema.registry.url` at send time) |

## TUNE — levers admins actually pull

| Lever | Scenario | Quick guidance |
|---|---|---|
| Durability | 9, 10 | `acks=all` × `MIR=2` on RF=3 |
| Dedup | 11 | `enable.idempotence=true` — always |
| Throughput | 7 | `linger.ms≥5`, `batch.size≥16 KB` |
| Bytes on wire | 8 | `compression.type=snappy` by default, `zstd` if bandwidth-limited |
| Timeouts | 5, 13 | 3-layer hierarchy; know which fires |
| Enforcement | 14 | `producer_byte_rate` per `client.id` |
| Measure the impact | 16 | `kafka-producer-perf-test.sh` — median of 3 |
| Schema evolution | 17 | Schema Registry with `BACKWARD` compatibility as the default |
| Graceful shutdown budget | 18 | `producer.close(Duration.ofSeconds(30))` in app code |

## The one-liner every admin should walk away with

> `acks=all` × `min.insync.replicas=2` × `enable.idempotence=true` on `RF=3` is the strong-durability, high-availability default. Anything weaker in production needs justification.
