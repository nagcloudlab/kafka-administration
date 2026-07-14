# kafka-clients — Producer & Consumer Teaching Kit

Maven module used in the Kafka administration training. Each side (producer, consumer) is intentionally contained in **one guided class** for non-coder admins, with **one instructor README** next to the code.

---

## Prerequisites

A running local Kafka cluster from [`../Lab-01-Cluster-Setup.md`](../Lab-01-Cluster-Setup.md):
- ZooKeeper on `2181`
- Brokers on `9092`, `9093`, `9094`

Java 17 and Maven 3.9+ on the path.

---

## Build

```bash
mvn -q -f kafka-clients/pom.xml compile
```

---

## Producer

One file, one guide. The trainer edits the file to demonstrate each scenario; the guide lists the exact edits.

- Code: [`KafkaProducerClient.java`](src/main/java/com/example/producer/KafkaProducerClient.java)
- Instructor doc: [`README.md`](src/main/java/com/example/producer/README.md)

Run:

```bash
mvn -q compile exec:java -Dexec.mainClass=com.example.producer.KafkaProducerClient
```

The producer README walks 15 scenarios covering full-config walkthrough, partitioning, interceptors, broker failover, buffer/timeout limits, batching, compression, `acks` and ISR durability, idempotence, transactions, timeout hierarchy, client quotas, and JMX metrics.

---

## Consumer

The consumer curriculum uses one production-shaped base client for non-coder administrators: [`KafkaConsumerClient.java`](src/main/java/com/example/consumer/KafkaConsumerClient.java). It runs continuously with no menus or pauses.

Run:

```bash
mvn -q compile exec:java -Dexec.mainClass=com.example.consumer.KafkaConsumerClient
```

Use the consumer [`README.md`](src/main/java/com/example/consumer/README.md) as the single instructor document. It presents each concept in sequence, identifies the exact code/configuration to show or temporarily change, and explains the administrator impact in plain language.

---

## Admin lens — what to know, watch, tune

Condensed cheat sheet. Links point into the producer guide's scenario sections.

### KNOW — concepts that shape every production decision

| Question | Where |
|---|---|
| What producer knobs even exist? | Producer scenario 1 (full-config walkthrough) |
| Three timeouts — which fires when? | Producer scenarios 5, 13 |
| What guarantees does each `acks` level give? | Producer scenario 9 |
| What is `min.insync.replicas` really doing? | Producer scenario 10 |
| Does the producer dedupe on retry? | Producer scenario 11 |
| Can the producer write atomically across topics? | Producer scenario 12 |
| What client-side failure modes show up in prod? | Producer scenarios 4, 5, 6 |

### WATCH — signals to scrape and alert on

| Signal | Where |
|---|---|
| Full "must-scrape" JMX set | Producer scenario 15 |
| Batching quality (`records-per-request-avg`) | Producer scenario 7 |
| Compression effectiveness | Producer scenario 8 |
| Quota pressure (`produce-throttle-time-avg`) | Producer scenario 14 |
| Retry storm signature (`record-retry-rate`) | Producer scenarios 4, 11 |
| Under-replicated writes (`NotEnoughReplicasException`) | Producer scenario 10 |

### TUNE — levers admins actually pull

| Lever | Where |
|---|---|
| Durability: `acks` × `min.insync.replicas` | Producer scenarios 9, 10 |
| Dedup: `enable.idempotence` | Producer scenario 11 |
| Throughput: `linger.ms` × `batch.size` | Producer scenario 7 |
| Bytes on wire: `compression.type` | Producer scenario 8 |
| Timeouts: `max.block.ms` / `request.timeout.ms` / `delivery.timeout.ms` | Producer scenarios 5, 13 |
| Enforcement: per-client `producer_byte_rate` quota | Producer scenario 14 |
| Measure impact of any change | `kafka-producer-perf-test.sh` — the CLI every admin should know |

---

## Logging

`src/main/resources/simplelogger.properties`:
- Default: everything at `WARN`, our code at `INFO`.
- Uncomment `Sender` / `NetworkClient` / `Metadata` lines when a scenario requires seeing producer internals — the file's header comment names which scenarios use which loggers.
- Timestamp format: `HH:mm:ss.SSS`.

Application output uses `System.out` directly, so lines look like `Sent key=upi partition=0 offset=17` — the pacing is visible from the terminal render rate.

---

## Conventions

- Each side of the kit is **ONE guided class + ONE instructor README** next to the code. No subpackages.
- CLI commands (`kafka-topics.sh`, `kafka-configs.sh`, `kafka-console-consumer.sh`, `kafka-producer-perf-test.sh`) are always **one physical line** — copy-paste from any position in the code block should work.
- Callouts in the instructor READMEs use exactly four labels:
  - `> **Note:** …` — non-obvious behaviour worth saying out loud.
  - `> **Admin lens:** …` — production-facing insight.
  - `> **See also:** …` — cross-reference within the guide.
  - `> **Before you run:** …` — prerequisites for a Run block.
- `pom.xml` `groupId` (`com.example`) is a Maven identifier, unrelated to Java package names.
