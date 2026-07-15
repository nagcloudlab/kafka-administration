# Scenario 3.1 — Controlled shutdown vs `kill -9`

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

- **Graceful path (SIGTERM):**
  - JVM shutdown hook calls `KafkaServer.shutdown()`.
  - Before touching the network listener, the broker sends a `ControlledShutdownRequest` to the active controller: *"I'm going down; migrate my leaderships."*
  - Controller writes new `LeaderAndIsr` records for every partition currently led by this broker (preferred replica chosen from the ISR) and pushes them to the surviving brokers.
  - Only after the controller confirms migration does the exiting broker close its socket.
  - Client-visible impact: one metadata refresh; next produce hits the new leader. Total: **< 100 ms**.

- **Ungraceful path (SIGKILL):**
  - JVM cannot trap SIGKILL — the shutdown hook never runs. The broker just disappears; the controller doesn't know yet.
  - Detection happens only when the ZK session expires (default **18 s**; ~10 s with `zookeeper.session.timeout.ms=10000`).
  - Until then, the controller keeps directing traffic to the dead broker.
  - Every produce during that window fails with `NotLeaderForPartitionException` — the client asks "you're the leader, take this," and nothing responds.
  - After session expiry, `PartitionStateMachine` fires `OnlinePartitionStateChange` for every affected partition → new leader election → metadata push → clients refresh and recover.
  - Client-visible impact: **5–15 s** of write errors, thousands of retry attempts, spiky p99.

- **The runbook error:** permitting `kill -9` "if slow" trades one broker's small delay during shutdown for cluster-wide client errors on every restart.

## Symptom
- Client: `NotLeaderForPartitionException`, retries, `TimeoutException` on `send()`.
- Broker JMX (surviving brokers): `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions > 0`.
- Controller JMX: `kafka.controller:type=KafkaController,name=OfflinePartitionsCount` briefly non-zero (SIGKILL only). Persistently non-zero = bigger incident (controller cannot elect).

## Setup — 4 terminals

**Terminal 1 (control):**

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --create --topic a1-ctrl-shutdown --partitions 6 --replication-factor 3

$KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic a1-ctrl-shutdown
```

- Baseline: `Isr: 101,102,103` on every partition.

**Terminal 2 (live topic view):**

```bash
watch -n 1 "$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --describe --topic a1-ctrl-shutdown"
```

**Terminal 3 (producer, 2000 rec/s, `acks=all`):**

```bash
$KAFKA/kafka-producer-perf-test.sh \
  --topic a1-ctrl-shutdown \
  --num-records 2000000 \
  --record-size 200 \
  --throughput 2000 \
  --producer-props bootstrap.servers=$BS acks=all
```

- Watch `records/sec` (~2000 = healthy) and `max latency`.

**Terminal 4 (broker log):**

```bash
tail -F logs/broker-101.log
```

## Trigger — Part 1: graceful (SIGTERM)

```bash
./cluster.sh stop-broker 101
```

## Observe — graceful
- **T4 (log):** `Starting controlled shutdown` → `Controlled shutdown request completed` → `shut down completed`.
- **T2 (topic view):** within ~2 s, `Leader=101` partitions flip to 102 or 103; `Isr` shrinks to `[102,103]`. Never `Leader: none`.
- **T3 (producer):** `records/sec` stays ~2000; `max latency` blips once and recovers.

## Reset (before Part 2)

```bash
./cluster.sh start-broker 101
$KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic a1-ctrl-shutdown
# wait until Isr = 101,102,103 everywhere
```

## Trigger — Part 2: ungraceful (SIGKILL)

```bash
kill -9 $(cat pids/broker-101.pid)
```

## Observe — ungraceful
- **T4:** nothing. No shutdown lines.
- **T2:** for ~10 s, `Leader=101` and `Isr=[101,102,103]` — controller hasn't noticed. Then ZK session expires → re-election → leader flips, ISR shrinks.
- **T3:** `records/sec` drops toward zero for the same ~10 s; `max latency` spikes to thousands of ms.

## Reset

```bash
rm -f pids/broker-101.pid
./cluster.sh start-broker 101
```

## Solution

1. **Always SIGTERM.**
   - `./cluster.sh stop-broker N` or `kill <pid>` — never `-9`.
   - `controlled.shutdown.enable=true` is on by default. Leave it.
   - **Drill quarterly.** The graceful path is invisible to clients only if it's actually exercised — rolling restarts are where a stale `kill -9` habit costs you.

2. **Give shutdown enough grace** in every `broker-*.properties`:
   ```properties
   controlled.shutdown.enable=true               # default: true
   controlled.shutdown.max.retries=3             # default: 3
   controlled.shutdown.retry.backoff.ms=5000     # default: 5000
   ```
   - Total budget: `max.retries × retry.backoff.ms` = up to **15 s** of migration attempts.
   - Healthy cluster: first attempt finishes in < 1 s.
   - While retrying, the broker keeps its listener **open** — clients see no outage yet.
   - After the last retry the broker exits anyway (falls back to ungraceful behaviour). Budget is a ceiling, not a promise.

3. **If SIGKILL is unavoidable** (stuck JVM, OOM-killer, hardware fault), shrink the controller's detection window so re-election is fast:
   ```properties
   replica.lag.time.max.ms=10000       # default: 30000
   zookeeper.session.timeout.ms=10000  # default: 18000 (ZK mode only)
   ```
   - **`replica.lag.time.max.ms`** — how long a follower may go without fetching before it's removed from ISR. Lower = faster ISR shrink after death. Also fires on GC pauses.
   - **`zookeeper.session.timeout.ms`** — how long ZK keeps a broker's session alive without heartbeat. Lower = faster broker-death detection. Also fires on any GC pause > this value.
   - **Do not go below `replica.lag.time.max.ms=6000`.** G1 mixed-GC pauses (200–500 ms every few minutes) will start causing spurious ISR flap. Baseline `UnderReplicatedPartitions` for a week before tightening further.

4. **Post-tune check.** ISR-shrink events should stay near zero on a healthy cluster:
   ```bash
   # Count "Shrinking ISR" events across all broker logs
   grep -c "Shrinking ISR" logs/broker-*.log
   ```

## Verify

```bash
# (a) T3 records/sec stays flat during a graceful stop.

# (b) Controller sees zero offline partitions
$KAFKA/kafka-run-class.sh kafka.tools.JmxTool \
  --jmx-url service:jmx:rmi:///jndi/rmi://localhost:9101/jmxrmi \
  --object-name 'kafka.controller:type=KafkaController,name=OfflinePartitionsCount' \
  --one-time true
# Expected: Value = 0

# (c) Broker log shows the full sequence
grep -E 'controlled shutdown|shut down completed' logs/broker-101.log | tail -5
```

## Takeaway

> **SIGTERM migrates leadership. SIGKILL migrates client errors.**

## Instructor notes
- Poll before triggering: *"Crash or restart — which is worse for the client?"* Most say crash. Demo makes the answer physical.
- Read the four `controlled shutdown` log lines out loud after Part 1 — that sequence is the whole point.
- Don't skip T3 during Part 2 — the `records/sec` drop is the visceral bit.
- Controlled shutdown > few seconds on a healthy cluster ⇒ **controller** bottleneck (different incident).
- Bridge to 1.2: *"`NodeExists` on start = same lifecycle, other side (registration)."*

## Teardown

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic a1-ctrl-shutdown
```
