# Scenario 4.1 — `min.insync.replicas` + `acks` (the durability triangle)

**Prereqs**
- Lab-01 done.
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

- **Three knobs, one guarantee:**
  - **RF** (replication factor) — how many copies of each partition exist. Set at topic-create time; changing it later requires a reassignment.
  - **min.insync.replicas** (min.ISR) — how many replicas must be **in the ISR** for the leader to accept a write. Enforced only when the producer sends with `acks=all`.
  - **acks** — producer-side: how many replicas must confirm before `send()` reports success.

- **What each `acks` value does at the wire level:**
  - `acks=0` — producer moves on immediately after writing bytes to the socket. No broker response required. Any broker failure = silent data loss. Never use for anything you care about.
  - `acks=1` (default in old versions) — leader writes to its local log, then acks. Followers may not have replicated yet. If leader dies before followers catch up, unacked writes are gone. Some durability, but a **window of loss**.
  - `acks=all` (aka `-1`) — leader waits for **every in-sync replica** to write, then acks. Combined with `min.ISR`, this is the durability guarantee.

- **When min.ISR is actually enforced:**
  - Only when producer uses `acks=all`. With `acks=0` or `acks=1`, min.ISR is silently ignored.
  - Configurable at topic level (overrides broker default). Broker-level default is `min.insync.replicas=1` — dangerous, and it ships that way in every distribution.
  - When `|ISR| < min.ISR` and producer sends with `acks=all` → leader rejects the write with `NotEnoughReplicasException` (or `NotEnoughReplicasAfterAppendException`).
  - Producer sees the error, retries per its policy, eventually times out with `TimeoutException` if ISR doesn't recover.

- **The durability math:**
  - To tolerate **F concurrent broker failures without producer errors:** `RF >= min.ISR + F`.
  - Example — RF=3:
    - `min.ISR=1` → F=2. Two brokers down; producer keeps working. **But** with only 1 replica in ISR, if that one dies before followers catch up, data is lost. Weak durability, high availability.
    - `min.ISR=2` → F=1. One broker down = fine; two down = producer fails. Leader failure = at least 1 other replica has the same data. **No data loss on any single failure.** Balanced.
    - `min.ISR=3` → F=0. Any ISR shrink causes producer failure. Strongest durability, worst availability. Brittle.
  - **Industry standard for durability-sensitive workloads:** `RF=3, min.ISR=2, acks=all, enable.idempotence=true`. Everything else is a trade-off.

- **Common misconfigurations:**
  - `RF=3, min.ISR=1` (shipping default) — silently at risk. Everything looks green until a leader dies during a follower-lag window.
  - `RF=3, min.ISR=3` — first ISR flap kills the producer. Nobody realises until the first slow follower.
  - `acks=1` on a payments topic — pretends to be durable. Isn't.
  - `acks=all` + `min.ISR=1` — pretends to be strong. Isn't (the "1" = just the leader).

## Symptom
- Producer log (min.ISR breach): `org.apache.kafka.common.errors.NotEnoughReplicasException`.
- Producer log (retries exhausted): `TimeoutException` on `send().get()` after `delivery.timeout.ms`.
- Broker log (leader for affected partition): `Number of insync replicas for partition ... is [1], below required minimum [2]`.
- `kafka-topics --describe`: `Isr` count < the topic's `min.insync.replicas`.

## Setup — 5 terminals

**Terminal 1 (control) — three topics, same RF, different min.ISR:**

```bash
# Create three topics
for name in b1-safe b1-brittle b1-risky; do
  $KAFKA/kafka-topics.sh --bootstrap-server $BS \
    --create --topic $name --partitions 3 --replication-factor 3
done

# Set min.ISR per topic
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type topics --entity-name b1-safe \
  --alter --add-config min.insync.replicas=2

$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type topics --entity-name b1-brittle \
  --alter --add-config min.insync.replicas=3

$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type topics --entity-name b1-risky \
  --alter --add-config min.insync.replicas=1

# Verify
for name in b1-safe b1-brittle b1-risky; do
  echo "=== $name ==="
  $KAFKA/kafka-configs.sh --bootstrap-server $BS \
    --entity-type topics --entity-name $name --describe | grep -i insync
done
```

**Terminal 2 (topic view — all three side by side):**

```bash
watch -n 1 "for t in b1-safe b1-brittle b1-risky; do
  echo === \$t ===
  $KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic \$t | tail -4
done"
```

- Baseline: every topic shows `Isr: 101,102,103` on every partition.

**Terminal 3 (producer to safe, `acks=all`):**

```bash
$KAFKA/kafka-console-producer.sh --bootstrap-server $BS \
  --topic b1-safe --producer-property acks=all
```

Type `hello-safe-1`, `hello-safe-2`, ... — one line every few seconds. Watch for errors.

**Terminal 4 (producer to brittle, `acks=all`):**

```bash
$KAFKA/kafka-console-producer.sh --bootstrap-server $BS \
  --topic b1-brittle --producer-property acks=all
```

Type `hello-brittle-1`, ...

**Terminal 5 (producer to risky, `acks=all`):**

```bash
$KAFKA/kafka-console-producer.sh --bootstrap-server $BS \
  --topic b1-risky --producer-property acks=all
```

Type `hello-risky-1`, ...

- Baseline: all three producers accept lines silently (no error output).

## Trigger — take one broker down (graceful, from 1.1)

Paste in Terminal 1:

```bash
./cluster.sh stop-broker 102
```

## Observe
- **T2 (topic view):** all three topics show `Isr: 101,103` on partitions where 102 was in ISR.
- **T3 (safe, min.ISR=2):** typing continues to succeed silently. `|ISR|=2 >= min.ISR=2`. Working as designed.
- **T4 (brittle, min.ISR=3):** first line typed after ISR shrink prints:
  ```
  ERROR Error when sending message to topic b1-brittle with key: null, value:
  ... : org.apache.kafka.common.errors.NotEnoughReplicasException:
  Messages are rejected since there are fewer in-sync replicas than required.
  ```
- **T5 (risky, min.ISR=1):** typing succeeds. `|ISR|=2 >= min.ISR=1`. **Looks fine.** But if you now also kill broker 103, this topic still accepts writes with only broker 101 (the leader) in ISR — no follower has your data. The topic is a silent time bomb.

## Reset (before Solution)

```bash
./cluster.sh start-broker 102
# Wait until Isr = 101,102,103 on every topic
$KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic b1-safe
```

## Solution

- **Pick the config for the workload — "higher isn't safer".**
  - **Payments / financial / anything with a downstream ledger:** `RF=3, min.ISR=2, acks=all, enable.idempotence=true`. One broker down = producer OK. Two down = producer errors (correct — block writes rather than lose them).
  - **Metrics / logs / analytics ingestion:** `RF=3, min.ISR=1, acks=1`. Trade durability for throughput and availability. Accept that a leader crash may lose the last few seconds of writes.
  - **Never** `min.ISR=3` on RF=3 in production. First slow follower kills the producer.

- **Set at topic level (do not rely on broker default):**
  ```bash
  $KAFKA/kafka-configs.sh --bootstrap-server $BS \
    --entity-type topics --entity-name payments \
    --alter --add-config 'min.insync.replicas=2'
  ```

- **Fix the broker-level default too — the shipping default of 1 is a footgun:**
  ```properties
  # broker-*.properties
  min.insync.replicas=2      # default: 1 — CHANGE THIS
  ```
  Any topic created without an explicit `min.insync.replicas` inherits the broker default. Leaving it at 1 means every new topic is silently at risk until someone remembers to override it.

- **Producer must set `acks=all` for min.ISR to matter.**
  ```properties
  # producer config
  acks=all                       # required for min.ISR enforcement
  enable.idempotence=true        # auto-forces acks=all + retries=INT_MAX + max.in.flight=5
  ```
  Setting `enable.idempotence=true` is the safer shortcut — it wires the correct combination and adds sequence-number dedup for retries.

- **Durability triangle summary:**

  | Priority | RF | min.ISR | acks | Tolerates | Data-loss window |
  |----------|----|---------|------|-----------|-------------------|
  | Critical | 3  | 2       | all  | 1 broker  | None on single failure |
  | Standard | 3  | 1       | all  | 2 brokers | Yes if leader dies with follower lag |
  | Throughput | 3 | 1     | 1    | 2 brokers | Yes, always a window |
  | Fire-and-forget | 3 | any | 0  | anything  | Silent, always |

## Verify

```bash
# 1. min.ISR is set correctly per topic
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type topics --entity-name b1-safe --describe | grep -i insync

# 2. Simulate the failure — re-stop broker 102 and confirm b1-safe still works
./cluster.sh stop-broker 102
echo "verify-safe" | $KAFKA/kafka-console-producer.sh --bootstrap-server $BS \
  --topic b1-safe --producer-property acks=all
# Expected: no error. Restart broker afterwards:
./cluster.sh start-broker 102

# 3. Broker log line proves min.ISR enforcement fired on brittle
grep "below required minimum" logs/broker-*.log | tail -5
```

## Takeaway

> **RF − min.ISR = failures you tolerate. Match to the workload's durability need.**

## Instructor notes
- Poll before triggering: *"RF=3, `min.ISR=3` — safer or more brittle than `min.ISR=2`?"* Most say "safer" (higher = stricter). The demo shows the opposite in practice.
- The visual is the three producer panes side by side. T3 stays quiet (working), T4 explodes (rejected), T5 stays quiet (working — but the "risky" name is the lesson).
- Emphasise: **`acks=all` is the trigger.** Without it, min.ISR is decoration.
- Real-world defaults are often wrong: broker-level `min.insync.replicas=1` is the shipping default and stays that way in most clusters until an incident forces a review.
- Bridge to 2.2: *"2.1 assumes brokers die gracefully. What if the leader dies AND the only follower with the data is also stale? That's unclean election."*

## Teardown

```bash
for name in b1-safe b1-brittle b1-risky; do
  $KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic $name
done
```
