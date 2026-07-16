# Scenario 3.3 — Under-replicated partitions (URP is not a page)

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

- **What URP measures**
  - `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions` — per-broker count of partitions where this broker is the **leader** AND `|Isr| < |Replicas|`.
  - Cluster-wide URP = sum across all brokers.
  - Fires when a follower falls behind by more than `replica.lag.time.max.ms` (default 30 s) — the leader removes it from ISR.

- **Two very different causes look identical:**
  - **Broker gone** (Scenario 3.1) — follower isn't fetching at all; one more failure = data loss.
  - **Follower slow** — replica is alive and fetching, just behind (network, disk saturation, GC, reassignment throttle). No data-loss risk yet.
  - Both show the same `Isr` shrink in `--describe` and the same URP metric.

- **The common mistake:** paging on URP > 0 without triage. Operators either wake up at 3 AM for a lagging follower, or (worse) stop responding to URP alerts entirely and miss real broker outages.

- **Mechanics of the ISR shrink:**
  - Every follower runs a replica-fetcher thread that sends `FetchRequest` to the leader.
  - The leader tracks `logEndOffsetLag` per follower.
  - If a follower hasn't caught up within `replica.lag.time.max.ms`, the leader writes a new `LeaderAndIsr` record excluding it → new ISR is persisted (ZK or KRaft log) → topic metadata is updated → URP increments.
  - When the follower catches up: leader adds it back → `Expanding ISR` log line → URP decrements.

## Symptom
- Broker JMX (leaders only): `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions > 0`.
- Leader broker log for the affected partition: `Shrinking ISR from Set(101, 102, 103) to Set(101, 103)`.
- `kafka-topics --describe`: `Isr` list shorter than `Replicas` list.
- The slow follower's log may show: `FetcherThread ... lag ... exceeds max lag`.

## Setup — 5 terminals

**Terminal 1 (control):**

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --create --topic a3-urp --partitions 6 --replication-factor 3

$KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic a3-urp
```

- Baseline: `Isr: 101,102,103` on all 6 partitions.

**Terminal 2 (live topic view):**

```bash
watch -n 1 "$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --describe --topic a3-urp"
```

**Terminal 3 (URP JMX probe on broker 101):**

```bash
$KAFKA/kafka-run-class.sh kafka.tools.JmxTool \
  --jmx-url service:jmx:rmi:///jndi/rmi://localhost:9101/jmxrmi \
  --object-name 'kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions' \
  --reporting-interval 2000
```

- Baseline: `Value = 0`. (Production would sum across all brokers.)

**Terminal 4 (high-volume producer, `acks=all`):**

```bash
$KAFKA/kafka-producer-perf-test.sh \
  --topic a3-urp \
  --num-records 5000000 \
  --record-size 1000 \
  --throughput -1 \
  --producer-props bootstrap.servers=$BS acks=all
```

- `throughput=-1` = unthrottled — pushes several MB/s so the throttled follower falls behind fast.

**Terminal 5 (broker 102 log):**

```bash
tail -F logs/broker-102.log
```

## Trigger — throttle broker 102's follower fetches to 1 KB/s

Paste in Terminal 1:

```bash
# Cap broker 102's follower fetch rate at 1024 bytes/sec
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type brokers --entity-name 102 \
  --alter --add-config 'follower.replication.throttled.rate=1024'

# Mark all replicas of a3-urp as throttled (on the follower side)
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type topics --entity-name a3-urp \
  --alter --add-config 'follower.replication.throttled.replicas=*'
```

## Observe
- **T4 (producer):** keeps producing. `records/sec` unchanged — producer doesn't care about follower lag.
- **T2 (topic view):** after ~30 s (one `replica.lag.time.max.ms`), partitions where 102 is a **follower** show `Isr: 101,103`. Partitions where 102 is the **leader** stay `Isr: 101,102,103` — 102's own inbound writes are not throttled, only its outbound fetches.
- **T3 (URP):** `Value` climbs above 0 on broker 101 (and would on 103 too if probed).
- **T5 (broker 102 log):** the leader for each affected partition logs `Shrinking ISR from Set(101, 102, 103) to Set(101, 103)`. If you don't see it on 102's log, check `logs/broker-101.log` / `logs/broker-103.log` — the message appears on the **leader's** log, not the demoted follower's.

## Triage — the three questions before you page

Paste in Terminal 1:

```bash
# Q1: Is the missing broker alive?
ss -tlnp 2>/dev/null | grep ':9093\b'
# Expected: LISTEN by a java PID → broker is up

# Q2: Is it fetching? (kafka-log-dirs shows per-replica lag)
$KAFKA/kafka-log-dirs.sh --bootstrap-server $BS --describe \
  --topic-list a3-urp | python3 -m json.tool | head -40
# Look for "offsetLag" on broker 102's replicas — non-zero and growing = slow, not dead

# Q3: What's constraining it? Any active throttles?
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type brokers --entity-name 102 --describe
```

**Interpret:**
- Q1 = PID present, Q2 = lag growing but finite, Q3 = active throttle → **slow follower.** Don't page. Fix the throttle / disk / network / GC.
- Q1 = no PID → **broker actually down.** Escalate — this is a 1.1-style incident.

## Solution

The solution is not a config knob — it's a **paging framework.**

- **Do not page on URP > 0 alone.** URP by itself is "check this," not "wake someone up."
- **Auto-page criteria** (any one is a real page):
  - `Isr count == min.insync.replicas` on any partition — one more follower loss = producer failures.
  - URP > 0 on **multiple brokers simultaneously** — systemic cause, not an isolated slow follower.
  - URP > 0 persistently for > 10 min — a real follower failure that didn't self-heal.
- **The real page metric** is `kafka.server:type=ReplicaManager,name=UnderMinIsrPartitionCount > 0`. This fires only when `|Isr| < min.insync.replicas` — i.e., producers are about to start failing. If you swap URP for UnderMinISR in your alerting, most 3 AM pages disappear.
- **Triage runbook** = the three questions above. Same shape as 1.2's diagnosis: OS layer first (`ss -tlnp`), then Kafka layer (`log-dirs`), then configuration (`configs.sh`).

**Remove the throttle (paste in Terminal 1):**

```bash
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type brokers --entity-name 102 \
  --alter --delete-config 'follower.replication.throttled.rate'

$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type topics --entity-name a3-urp \
  --alter --delete-config 'follower.replication.throttled.replicas'
```

## Verify

```bash
# 1. ISR expands back to full
$KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic a3-urp
# Expected: Isr: 101,102,103 on every partition

# 2. URP back to 0
$KAFKA/kafka-run-class.sh kafka.tools.JmxTool \
  --jmx-url service:jmx:rmi:///jndi/rmi://localhost:9101/jmxrmi \
  --object-name 'kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions' \
  --one-time true
# Expected: Value = 0

# 3. Log shows the reverse — "Expanding ISR"
grep "Expanding ISR" logs/broker-*.log | tail -5
```

## Takeaway

> **URP = "behind on data", not "data at risk". Triage before you page.**

## Instructor notes
- Poll before triggering: *"URP > 0 for 5 minutes — do you page?"* Most say yes. The demo shows the more nuanced answer.
- Compare with 1.1: same URP fires, completely different situation. Same alert, different runbook.
- The **real** paging metric is `UnderMinIsrPartitionCount` — not URP. If the room takes one thing from this scenario, it's the alert-metric swap.
- Point at Terminal 4 throughout — producer keeps working during the whole "outage." That's the "not at risk" evidence in real time.
- Bridge to Lab 4: *"Whether URP is safe depends on min.ISR. Lab 4.1 covers the durability triangle in full."*

## Teardown

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic a3-urp

# Remove any leftover throttle configs
$KAFKA/kafka-configs.sh --bootstrap-server $BS --entity-type brokers \
  --entity-name 102 --alter --delete-config 'follower.replication.throttled.rate' 2>/dev/null
$KAFKA/kafka-configs.sh --bootstrap-server $BS --entity-type topics \
  --entity-name a3-urp --alter --delete-config 'follower.replication.throttled.replicas' 2>/dev/null
```
