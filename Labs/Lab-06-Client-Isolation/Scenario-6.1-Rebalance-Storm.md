# Scenario 6.1 — Consumer group rebalance storm

**Prereqs**
- Chapters 1–3 done.
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

- **What a rebalance is.**
  - Consumer group coordinator (a broker) tracks which consumer owns which partitions in a group.
  - When membership changes — join, leave, timeout — coordinator recomputes the assignment and sends `SyncGroupResponse` to each member with its new partition list.
  - During the rebalance, the group's processing is paused for at least some members.

- **What triggers a rebalance:**
  - **Member join** — a new consumer with the same `group.id` connects.
  - **Member leave** — a consumer sends `LeaveGroup` on clean shutdown, or the coordinator times it out.
  - **`session.timeout.ms` expired** — coordinator hasn't seen a heartbeat within this window (default 45 s).
  - **`max.poll.interval.ms` expired** — coordinator hasn't seen a `poll()` within this window (default 5 min). Consumer is considered stuck and evicted even if heartbeats are still coming.
  - **Metadata change** — new partitions added to a subscribed topic; new topic matches a subscription pattern.

- **Two rebalance protocols:**
  - **Eager** (older assignors like `RangeAssignor`, `RoundRobinAssignor`) — stop-the-world. Every consumer revokes **all** its partitions, coordinator computes new plan, each consumer resumes with a possibly-different set. Full group pauses for the duration.
  - **Cooperative** (`CooperativeStickyAssignor`, default in 2.4+) — incremental. Only partitions that need to move get revoked; other members keep processing without interruption. Two-phase: first rebalance revokes moving partitions, second rebalance assigns them.

- **Rebalance storm = the group never settles.**
  - Consumer keeps getting evicted → rejoins → other members must rebalance → then it happens again.
  - **Classic causes:**
    1. **GC pause > `session.timeout.ms`.** JVM pauses for e.g. 60 s (heap tuning issue). Coordinator times it out. Consumer wakes, tries to heartbeat, sees "rebalancing", rejoins. Repeat.
    2. **Slow message processing > `max.poll.interval.ms`.** Consumer's `poll()` loop calls a slow downstream (DB write, API call). Coordinator kicks it out for "not polling". Consumer catches up, next `poll()` sees an eviction error → rejoins → repeat.
    3. **Auto-scaler churn.** Kubernetes scales consumer pods up/down every 30 s in response to noisy metrics → constant membership churn → constant rebalances.
    4. **Rolling deploys without static membership.** Rolling restart evicts + rejoins each pod → N rebalances for N pods.

- **Cost during the storm:**
  - Consumer lag grows without bound — group is spending all its time rebalancing, not consuming.
  - Coordinator load spikes; if the coordinator is also handling many other groups, they all slow down.
  - Log spam: `Attempt to heartbeat failed since group is rebalancing` on every consumer.

## Symptom
- Consumer log spam: `[Consumer clientId=...] Rejoining group because we're missing the group generation` or `Attempt to heartbeat failed since group is rebalancing`.
- `kafka-consumer-groups.sh --describe --group X` output flips between `Stable` and `PreparingRebalance` / `CompletingRebalance` every few seconds.
- Consumer JMX: `kafka.consumer:type=consumer-coordinator-metrics,client-id=X,name=rebalance-rate-per-hour` > single digits per hour on a healthy group; hundreds during a storm.
- Consumer lag grows despite no obvious traffic spike.

## Setup — 5 terminals

**Terminal 1 (control):**

```bash
# 6-partition topic, 3-consumer group — clean 2-partitions-per-member baseline
$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --create --topic h1-rebalance --partitions 6 --replication-factor 3

# Produce steady traffic to make consumer lag visible
$KAFKA/kafka-producer-perf-test.sh \
  --topic h1-rebalance --num-records 10000000 \
  --record-size 200 --throughput 500 \
  --producer-props bootstrap.servers=$BS acks=1 &
PROD_PID=$!
echo "producer PID: $PROD_PID"
```

**Terminal 2 (group view — live):**

```bash
watch -n 1 "$KAFKA/kafka-consumer-groups.sh --bootstrap-server $BS \
  --describe --group storm-demo 2>&1 | head -20"
```

**Terminals 3, 4, 5 — three consumers in the same group.**

Each starts identically. Keep the terminals open; you'll `kill -STOP` / `-CONT` one below.

```bash
$KAFKA/kafka-console-consumer.sh --bootstrap-server $BS \
  --topic h1-rebalance --group storm-demo \
  --consumer-property session.timeout.ms=6000 \
  --consumer-property heartbeat.interval.ms=2000 \
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.RangeAssignor \
  --consumer-property client.id=c-<N>   # replace <N> with 3/4/5 per terminal
```

- `session.timeout.ms=6000` is the **minimum allowed** by broker defaults (`group.min.session.timeout.ms=6000`). Tight enough that a 10 s SIGSTOP will evict.

Once all three are running, Terminal 2 should show `Stable`, 6 partitions divided across `c-3 / c-4 / c-5` (2 each).

## Trigger — Part A: normal eviction (kill one consumer)

**In Terminal 5:** press `Ctrl+C`.

### Observe — eager rebalance
- **T2:** group state flips to `PreparingRebalance` → `CompletingRebalance` → `Stable`. Duration: 1–3 s. All 6 partitions now split across 2 members.
- **T3, T4 consumer logs:** *both* show `Revoking previously assigned partitions` and `Setting newly assigned partitions`. Even members that shouldn't have moved got interrupted — that's **eager**'s stop-the-world cost.

Restart Terminal 5's consumer with the same command.

## Trigger — Part B: cooperative rebalance

Stop all three consumers (Ctrl+C in T3, T4, T5), then restart each with the cooperative assignor:

```bash
$KAFKA/kafka-console-consumer.sh --bootstrap-server $BS \
  --topic h1-rebalance --group storm-demo \
  --consumer-property session.timeout.ms=6000 \
  --consumer-property heartbeat.interval.ms=2000 \
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor \
  --consumer-property client.id=c-<N>
```

Wait for T2 to show `Stable`. Then `Ctrl+C` in Terminal 5 again.

### Observe — cooperative rebalance
- **T2:** same `PreparingRebalance` → `Stable` sequence.
- **T3, T4 consumer logs:** only the partitions that **had to move** (from c-5) are re-assigned. T3 and T4 keep processing their existing partitions without a full revoke.
- The full "rebalance" now happens in **two smaller steps** — the first revokes moving partitions, the second assigns them. Group throughput barely dips.

## Trigger — Part C: rebalance storm (SIGSTOP a consumer)

Restart Terminal 5's consumer. Wait for `Stable`. Then find the JVM PID:

```bash
# In a spare shell
CONSUMER5_PID=$(pgrep -f 'client.id=c-5' | head -1)
echo "PID: $CONSUMER5_PID"
```

Simulate a GC pause (or hung processing):

```bash
kill -STOP $CONSUMER5_PID
sleep 10                # > session.timeout.ms of 6 s → coordinator will evict
kill -CONT $CONSUMER5_PID
```

### Observe — the storm
- **During the SIGSTOP (T-0 to T+10):**
  - T2 flips to `PreparingRebalance` at ~T+6s (session timeout).
  - c-5's partitions get redistributed to c-3 and c-4.
- **After SIGCONT (T+10):**
  - Terminal 5 (c-5) prints `Attempt to heartbeat failed since group is rebalancing`, then `Rejoining group`.
  - c-5 rejoins → **second rebalance** → partitions re-shuffled again.
- **Repeat the SIGSTOP+CONT loop** to see the storm:
  ```bash
  for i in 1 2 3 4; do
    kill -STOP $CONSUMER5_PID; sleep 10
    kill -CONT $CONSUMER5_PID; sleep 5
  done
  ```
- T2 shows the group flapping between `Stable` and `PreparingRebalance` continuously. Consumer lag on the topic grows.

## Solution

- **Use `CooperativeStickyAssignor`.** It's the default in newer clients but worth setting explicitly:
  ```properties
  # consumer config
  partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
  ```
  Even with pathological churn, cooperative keeps non-moving partitions processing.

- **Tune session and poll timeouts to real GC / processing behaviour.**
  ```properties
  session.timeout.ms=45000        # default 45s — usually correct; don't reduce below 30s without a reason
  heartbeat.interval.ms=15000     # ≤ session.timeout.ms / 3
  max.poll.interval.ms=300000     # default 5 min — bump if your poll() legitimately takes longer
  ```
  Anti-pattern: dropping `session.timeout.ms` to 6 s to "detect failures faster" — a single GC pause becomes a rebalance.

- **Use static membership (KIP-345) for stable deployments.**
  ```properties
  group.instance.id=payments-consumer-pod-3      # stable per pod; do NOT randomise
  ```
  With static membership, a consumer restart **within** `session.timeout.ms` does **not** trigger a rebalance — coordinator waits for the same instance-id to return. Perfect for rolling deploys.

- **Slow processing → tune `max.poll.records`, don't just bump `max.poll.interval.ms`.**
  ```properties
  max.poll.records=100            # default 500 — smaller batches = more frequent polls = safer under load
  ```
  If a batch takes 10 s to process 500 records, cut the batch to 100 and it takes 2 s. `max.poll.interval.ms` stays at 5 min as a safety net, not a routine budget.

- **Monitor rebalance rate.**
  ```
  # JMX per consumer:
  kafka.consumer:type=consumer-coordinator-metrics,client-id=X,name=rebalance-rate-per-hour
  ```
  Healthy: single digits per hour (deploys, occasional scaling). Storm: 100+ per hour. Alert threshold: > 20/hour sustained.

- **Broker-side sanity checks.**
  ```properties
  # broker-*.properties
  group.min.session.timeout.ms=6000        # default — enforces a floor
  group.max.session.timeout.ms=1800000     # default 30 min — cap so bad clients can't hold slots forever
  ```

## Verify

```bash
# 1. Group is Stable and evenly distributed
$KAFKA/kafka-consumer-groups.sh --bootstrap-server $BS --describe --group storm-demo

# 2. No recent rebalances in coordinator log
grep -E 'Preparing to rebalance group storm-demo|Stabilized group storm-demo' \
  logs/broker-*.log | tail -10

# 3. With static membership: restart a consumer and confirm NO rebalance
#    (Add --consumer-property group.instance.id=c-3-static to consumer 3; restart it.)
```

## Takeaway

> **Rebalance storms are a client-tuning problem. Short `session.timeout.ms` + real GC = storm. Use cooperative + static membership.**

## Instructor notes
- Poll before Part C: *"If a consumer's JVM pauses for 10 s and `session.timeout.ms=6 s`, what happens when the JVM wakes up?"* Most guess "nothing" or "just catches up." The demo shows the actual answer: **two** rebalances per pause.
- Watch T3 and T4 consumer output during Part A vs Part B — the "revoke everything" behaviour in eager vs the "keep processing" behaviour in cooperative is the demo's centrepiece.
- Real-world story: a customer set `session.timeout.ms=10000` to "detect failures faster" during a review. Their p99 GC pause was 12 s. Result: continuous rebalance storm. Fix was to reset session timeout to 45 s (default) and tune the heap.
- Static membership is the single biggest improvement for stable long-running deployments. Set `group.instance.id` per pod/host — never randomise. K8s StatefulSets pair naturally with static membership.
- Bridge to 4.2: *"4.1 was one consumer hurting its own group. 4.2 is one client hurting the whole cluster — and how to bound the blast radius with quotas."*

## Teardown

```bash
# Stop the producer
kill $PROD_PID 2>/dev/null

# Kill any remaining consumers
pkill -f 'client.id=c-[345]' 2>/dev/null

# Delete the topic
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic h1-rebalance
```
