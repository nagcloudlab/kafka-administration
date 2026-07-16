# Scenario 4.2 — Unclean leader election (silent data loss)

**Prereqs**
- Lab 3 done.
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

- **Two kinds of leader election:**
  - **Clean** — controller picks the new leader from the **ISR only**. Every ISR member has all committed messages, so the new leader's log is a strict extension of the old leader's. Zero data loss.
  - **Unclean** — if the ISR is empty (every in-sync replica offline), the controller picks an **out-of-sync** replica. The new leader's log is *behind* what the old leader had. Any messages the old leader accepted but hadn't replicated yet are gone.

- **When ISR goes empty:**
  - Requires all in-sync replicas to be down simultaneously.
  - Rare on healthy clusters, but happens: cascading failures, rolling restart done wrong, network partition, correlated disk failures.
  - With `unclean.leader.election.enable=false`: partition stays `Leader: -1` until a former ISR member comes back. Producer errors, consumer stalls.
  - With `unclean.leader.election.enable=true`: controller picks any surviving replica → partition back online → **silent data loss** for anything the old leader hadn't replicated.

- **The trade-off:**
  - **`unclean.leader.election.enable=false`** — Kafka default since 0.11. Durability over availability.
  - **`unclean.leader.election.enable=true`** — default in older versions. Availability over durability. Silent historical data loss.

- **What happens to the old leader when it comes back:**
  - It rejoins as a follower. Controller tells it the new leader's log is authoritative.
  - Old leader **truncates** its on-disk log to match the new leader's high-water mark → any messages beyond that are permanently gone from disk.
  - Log line: `Truncating to offset N`. If N < the old leader's previous log-end offset, that delta is your data loss.

- **Setting it:**
  - Broker-level default: `unclean.leader.election.enable=false`. Correct — do not change.
  - Topic-level override: `true` only for availability-priority workloads (metrics, ephemeral logs).
  - Payments / financial: never override to true.

## Symptom
- With unclean=true, after failure + recovery:
  - Controller broker log: `Unclean leader election for partition [X, N] enabled`.
  - New-leader broker log: `Selected as leader for partition [X, N] the replica NNN`.
  - Consumer sees the topic's high-water mark **decrease** compared to before.
  - Rejoining old-leader broker log: `Truncating to offset N` (where N < its previous LEO).
- With unclean=false, during the outage:
  - `kafka-topics --describe`: `Leader: -1`, `Isr: `, `Offline: X`.
  - Producer: `NotLeaderForPartitionException` → `TimeoutException`.
  - Consumer: `poll()` returns empty.

## Setup — 5 terminals

**Terminal 1 (control):**

```bash
# Two topics with the SAME preferred leader (101) so the failure sequence is identical
$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --create --topic c1-unclean --replica-assignment 101:102:103

$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --create --topic c1-clean --replica-assignment 101:102:103

# Override the unclean flag per topic
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type topics --entity-name c1-unclean \
  --alter --add-config unclean.leader.election.enable=true

$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type topics --entity-name c1-clean \
  --alter --add-config unclean.leader.election.enable=false

# Confirm both start with Leader=101, Isr=[101,102,103]
$KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic c1-unclean
$KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic c1-clean
```

**Terminal 2 (topic view — both):**

```bash
watch -n 1 "for t in c1-unclean c1-clean; do
  echo === \$t ===
  $KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic \$t | tail -3
done"
```

**Terminal 3 (offsets probe — the money shot):**

```bash
watch -n 2 "for t in c1-unclean c1-clean; do
  $KAFKA/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --bootstrap-server $BS --topic \$t 2>/dev/null
done"
```

- Output is `topic:partition:high-water-mark`. High-water-mark = total messages committed. Should never go down under normal conditions.

**Terminal 4 (broker 101 log — the doomed old leader):**

```bash
tail -F logs/broker-101.log
```

**Terminal 5 (broker 102 log — future unclean leader):**

```bash
tail -F logs/broker-102.log
```

## Trigger — the failure sequence

### Step 1 — produce 50 messages to each topic (paste in Terminal 1)

```bash
for i in $(seq 1 50); do echo "msg-$i"; done | \
  $KAFKA/kafka-console-producer.sh --bootstrap-server $BS --topic c1-unclean

for i in $(seq 1 50); do echo "msg-$i"; done | \
  $KAFKA/kafka-console-producer.sh --bootstrap-server $BS --topic c1-clean
```

**Checkpoint (T3):** both topics show `:0:50`.

### Step 2 — take out both followers

```bash
./cluster.sh stop-broker 102
./cluster.sh stop-broker 103
```

**Checkpoint (T2):** both topics show `Leader: 101, Isr: 101`.

### Step 3 — produce another 50 messages (only broker 101 has these)

```bash
for i in $(seq 51 100); do echo "msg-$i"; done | \
  $KAFKA/kafka-console-producer.sh --bootstrap-server $BS \
  --topic c1-unclean --producer-property acks=1

for i in $(seq 51 100); do echo "msg-$i"; done | \
  $KAFKA/kafka-console-producer.sh --bootstrap-server $BS \
  --topic c1-clean --producer-property acks=1
```

**Checkpoint (T3):** both topics show `:0:100`. Only broker 101 has offsets 51–100 on disk.

### Step 4 — kill the leader. ISR = [ ] on both topics

```bash
./cluster.sh stop-broker 101
```

**Checkpoint (T2):** both topics show `Leader: -1, Isr: `. Partition offline.

### Step 5 — bring 102 and 103 back. Watch the two topics diverge

```bash
./cluster.sh start-broker 102
./cluster.sh start-broker 103
```

Within seconds (as soon as one of them becomes controller):

- **c1-unclean** — T5 (`logs/broker-102.log`) shows:
  ```
  Unclean leader election for partition [c1-unclean,0] enabled
  Selected as leader for partition [c1-unclean,0] the replica 102
  ```
  T2 → `Leader: 102, Isr: 102,103`. T3 → `c1-unclean:0:50`. **The 51–100 range is gone.**

- **c1-clean** — T2 still shows `Leader: -1, Isr: `. Controller refuses to elect: the last-known ISR was [101], and 101 isn't back. **Partition stays offline.**

### Step 6 — bring broker 101 back (the old leader with all 100 messages)

```bash
./cluster.sh start-broker 101
```

- **c1-unclean** — T4 (`logs/broker-101.log`) shows:
  ```
  Truncating to offset 50
  ```
  Broker 101's disk copy of msg-51 to msg-100 is now **permanently truncated.** T3 stays `:0:50`.

- **c1-clean** — T2 flips to `Leader: 101, Isr: 101` (then 102, 103 rejoin). T3 → `c1-clean:0:100`. Full recovery, zero data loss.

## Observe — the difference on one screen

Paste in Terminal 1 (after Step 6):

```bash
echo "=== c1-unclean (unclean.leader.election.enable=true) ==="
$KAFKA/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server $BS --topic c1-unclean

$KAFKA/kafka-console-consumer.sh --bootstrap-server $BS \
  --topic c1-unclean --from-beginning --timeout-ms 3000 2>/dev/null | wc -l

echo
echo "=== c1-clean (unclean.leader.election.enable=false) ==="
$KAFKA/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server $BS --topic c1-clean

$KAFKA/kafka-console-consumer.sh --bootstrap-server $BS \
  --topic c1-clean --from-beginning --timeout-ms 3000 2>/dev/null | wc -l
```

Expected:
- **c1-unclean:** high-water-mark 50, consumer counts 50. **50 messages lost, silently.**
- **c1-clean:** high-water-mark 100, consumer counts 100. Zero data loss — but the partition was **offline** for the entire duration of Steps 4–6.

## Solution

- **Keep the broker-level default:**
  ```properties
  # broker-*.properties
  unclean.leader.election.enable=false   # Kafka default since 0.11 — DO NOT change
  ```

- **Override to `true` only per-topic, only when availability > durability:**
  ```bash
  # metrics / logs / non-critical — availability-priority
  $KAFKA/kafka-configs.sh --bootstrap-server $BS \
    --entity-type topics --entity-name metrics-ingest \
    --alter --add-config 'unclean.leader.election.enable=true'
  ```
  Payments, ledger, anything with a downstream ledger: never override.

- **If you must run with unclean=true, monitor for the actual loss event:**
  ```bash
  # Every occurrence is a data-loss event
  grep -E 'Unclean leader election.*enabled|Unclean leader election.*successful' \
    logs/broker-*.log

  # JMX rate — any non-zero value is a page
  # kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec
  ```

- **Also alert on the truncation** — this is when the data physically disappears:
  ```bash
  grep "Truncating to offset" logs/broker-*.log
  ```

- **Preventive design (combine with 2.1):** `RF=3, min.ISR=2, acks=all, unclean.leader.election.enable=false`. With min.ISR=2 you must lose 2 brokers within `replica.lag.time.max.ms` for ISR to actually go to [ ]. That is what unclean election exists for — and it's rare on a well-run cluster.

## Verify

```bash
# 1. Broker-level default is false (or absent = default false)
grep -r "unclean.leader.election" config/broker-*.properties || echo "not set — using Kafka default (false)"

# 2. No unclean elections have happened cluster-wide
$KAFKA/kafka-run-class.sh kafka.tools.JmxTool \
  --jmx-url service:jmx:rmi:///jndi/rmi://localhost:9101/jmxrmi \
  --object-name 'kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec' \
  --one-time true
# Expected: Count = 0

# 3. Topic-level overrides — only where intentional
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type topics --all --describe 2>/dev/null | \
  grep -B1 "unclean.leader.election.enable=true"
# Expected: only topics you explicitly opted in
```

## Takeaway

> **Unclean election trades durability for availability. Silent by default. Alert on every occurrence.**

## Instructor notes
- Poll before Step 5: *"Broker 101 is gone. 102 and 103 come back with only the first 50 messages. What should Kafka do — refuse to serve, or elect a stale replica?"* Split the room. The scenario shows what each choice costs.
- The visual is Terminal 3 (offsets probe). Watch `c1-unclean:0:100` become `c1-unclean:0:50` in real time after Step 5 — that IS the data loss.
- The `Truncating to offset 50` line in Step 6 is the moment the data is physically destroyed on disk. Read it out loud.
- Real-world story: many old Kafka clusters (pre-0.11) shipped with `unclean=true` as the default. Upgrades don't automatically flip it. Any topic created before 0.11 should be verified.
- Bridge to 2.3: *"2.1 was follower loss with min.ISR. 2.2 was ISR going empty. 2.3 asks: can we spread replicas so ISR going empty is nearly impossible?"*

## Teardown

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic c1-unclean
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic c1-clean

# Only if broker data dirs are wedged after all the ungraceful sequencing:
# ./cluster.sh stop
# rm -rf ./data/broker-101/* ./data/broker-102/* ./data/broker-103/*
# ./cluster.sh start
```
