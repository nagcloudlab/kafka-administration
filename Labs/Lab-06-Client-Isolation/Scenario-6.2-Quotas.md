# Scenario 6.2 — Quotas (protecting a shared cluster from one runaway client)

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

- **The multi-tenant problem.** One shared Kafka cluster serves N teams. One team ships a bug: a producer in a tight loop with no back-pressure. That producer saturates the broker's NIC (or disk, or CPU) → every *other* tenant's latency spikes and consumers fall behind. There's no isolation by default.

- **What quotas throttle:**
  - **`producer_byte_rate`** — max bytes/sec a client can *write*.
  - **`consumer_byte_rate`** — max bytes/sec a client can *read*.
  - **`request_percentage`** — max % of broker request-handler thread pool a client can occupy (CPU quota — protects against many-small-requests floods).
  - **`controller_mutation_rate`** — max create/delete topic/partition ops per sec (KRaft/newer versions; protects the controller).

- **Who quotas apply to:**
  - **`client-id`** — arbitrary string the client sends in every request; works without authentication. Anyone can set any client-id, so it's an "honour system" — good for internal cluster hygiene, not for security.
  - **`user`** — SASL principal name; requires SASL auth. Real isolation because the user can't spoof its principal.
  - **`user, client-id`** combo — finest-grained.
  - **Default (`--entity-default`)** — applies when no more-specific entity matches. Essential for defaulting new clients into a sane bucket.

- **How enforcement actually works:**
  - Broker maintains a rolling window (default 1 s, 11 samples) per quota entity.
  - When response would push the entity over the quota, broker adds `throttle_time_ms` to the response.
  - Client (Java, librdkafka) sleeps that long before sending the next request. Enforcement is **cooperative** — a rogue non-standard client could ignore it, but broker limits request rate on its side too.
  - Quota is per-broker, not per-cluster. `producer_byte_rate=1MB/s` on 3 brokers = a client can write ~3 MB/s total if perfectly balanced across leaders.

- **Precedence** (most specific wins):
  1. `(user, client-id)`
  2. `user`
  3. `client-id`
  4. `--entity-default` for `user`, `client-id`, or `(user, client-id)`
  5. `quota.producer.default` / `quota.consumer.default` (deprecated static broker config)

- **Common misconfigurations:**
  - No default quota set → a new team spins up a client with a novel client-id, no quota applies, they can burn the cluster.
  - `client-id` quota only, no `user` quota → any client can bypass by picking a new client-id.
  - Quota too tight → chronic `throttle_time_ms` in every response → producer latency spikes; consumer lag grows.
  - Quota too loose → same as no quota; one client can still hurt others.

## Symptom
- **Runaway client without quota:** broker `BytesInPerSec` at line rate on one client-id; other clients' produce latency spikes; consumer lag growing across unrelated topics.
- **Client under quota:** producer perf-test `records/sec` and `MB/sec` visibly lower than unthrottled baseline; JMX `kafka.server:type=ClientQuotaManager,name=throttle-time,client-id=X` > 0.
- **Chronic quota breach:** log line on broker: `Client <id> throttled for N ms`.

## Setup — 3 terminals

**Terminal 1 (control):**

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --create --topic i1-quota --partitions 3 --replication-factor 3
```

**Terminal 2 (quota + broker throughput probe):**

```bash
watch -n 2 "echo === Broker BytesIn/s ===; \
  for id in 101 102 103; do
    echo -n \"broker \$id: \"
    $KAFKA/kafka-run-class.sh kafka.tools.JmxTool \
      --jmx-url service:jmx:rmi:///jndi/rmi://localhost:91\$id/jmxrmi \
      --object-name 'kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec' \
      --one-time true 2>/dev/null | grep -oE 'OneMinuteRate=[0-9.]+' || echo n/a
  done; \
  echo; \
  echo === Active quotas ===; \
  $KAFKA/kafka-configs.sh --bootstrap-server $BS \
    --entity-type clients --describe 2>/dev/null"
```

**Terminal 3 (where we run producer perf-tests):**

Just an empty shell. Paste commands from the Trigger steps below.

## Trigger — Step 1: baseline (no quota)

**Terminal 3:**

```bash
# Baseline — no quota, single producer, unbounded throughput.
# Note the client-id we're using: "greedy-app"
$KAFKA/kafka-producer-perf-test.sh \
  --topic i1-quota \
  --num-records 500000 \
  --record-size 1000 \
  --throughput -1 \
  --producer-props bootstrap.servers=$BS acks=1 client.id=greedy-app
```

**Observe (T2 and T3):**
- T3 (perf-test summary line): typically **20–100 MB/s** on this localhost cluster.
- T2: broker `BytesIn/s` shows the burst; peaks divided across leader brokers.

## Trigger — Step 2: apply a `client-id` quota

**Terminal 1:**

```bash
# Cap greedy-app at 1 MB/s producer bytes AND 5 MB/s consumer bytes
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type clients --entity-name greedy-app \
  --alter --add-config 'producer_byte_rate=1048576,consumer_byte_rate=5242880'

# Confirm
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type clients --entity-name greedy-app --describe
```

## Trigger — Step 3: re-run producer as `greedy-app`

**Terminal 3:**

```bash
# SAME command as Step 1 — only the quota changed
$KAFKA/kafka-producer-perf-test.sh \
  --topic i1-quota \
  --num-records 500000 \
  --record-size 1000 \
  --throughput -1 \
  --producer-props bootstrap.servers=$BS acks=1 client.id=greedy-app
```

**Observe:**
- T3 (perf-test summary): now ~**1 MB/s**. Test takes ~500 s to finish 500 MB — cancel early if impatient.
- T2: broker `BytesIn/s` sits flat around 1 MB/s total (sum across brokers).

## Trigger — Step 4: prove the quota is scoped to client-id

**Terminal 3:**

```bash
# Same test, DIFFERENT client-id. No quota applies to this one.
$KAFKA/kafka-producer-perf-test.sh \
  --topic i1-quota \
  --num-records 500000 \
  --record-size 1000 \
  --throughput -1 \
  --producer-props bootstrap.servers=$BS acks=1 client.id=free-app
```

**Observe:**
- T3: back to baseline throughput (20–100 MB/s).
- T2: broker `BytesIn/s` spikes again.
- **The lesson:** client-id quotas are trivially bypassable by picking a new client-id. Real isolation needs user quotas (SASL) or a default quota.

## Trigger — Step 5: default quota (catches unknown clients)

**Terminal 1:**

```bash
# Default quota — applies to any client-id without a specific quota
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type clients --entity-default \
  --alter --add-config 'producer_byte_rate=2097152,consumer_byte_rate=10485760'

# Confirm
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type clients --entity-default --describe
```

**Terminal 3 (re-run as free-app):**

```bash
$KAFKA/kafka-producer-perf-test.sh \
  --topic i1-quota \
  --num-records 500000 \
  --record-size 1000 \
  --throughput -1 \
  --producer-props bootstrap.servers=$BS acks=1 client.id=free-app
```

**Observe:**
- T3: throughput now caps at ~2 MB/s (default quota). `free-app` picked up the default because it has no specific quota.
- `greedy-app` continues to use its own (1 MB/s) because specific overrides default.

## Solution

- **Always set a default quota.** No cluster in production should let a novel client-id write at line rate.
  ```bash
  # Producer + consumer defaults for all client-ids
  $KAFKA/kafka-configs.sh --bootstrap-server $BS \
    --entity-type clients --entity-default \
    --alter --add-config 'producer_byte_rate=5242880,consumer_byte_rate=20971520'
  ```

- **Override upward per known workload.** A well-behaved batch consumer needing 100 MB/s gets an explicit higher quota:
  ```bash
  $KAFKA/kafka-configs.sh --bootstrap-server $BS \
    --entity-type clients --entity-name batch-export \
    --alter --add-config 'consumer_byte_rate=104857600'
  ```

- **Use user quotas for real isolation (requires SASL).** In production, client-id is honour-only; user is authenticated. Combine both:
  ```bash
  # (user=payments-svc, client-id=any) — payments team can't burn > 50 MB/s cluster-wide
  $KAFKA/kafka-configs.sh --bootstrap-server $BS \
    --entity-type users --entity-name payments-svc \
    --alter --add-config 'producer_byte_rate=52428800'
  ```

- **Set a CPU (request) quota too.** Bandwidth quotas don't stop a client sending 100 K tiny requests/sec; that starves the request-handler pool. Add:
  ```bash
  $KAFKA/kafka-configs.sh --bootstrap-server $BS \
    --entity-type clients --entity-name greedy-app \
    --alter --add-config 'request_percentage=25'
  # max 25% of one broker's request thread pool
  ```

- **Quotas are per-broker, not per-cluster.** `producer_byte_rate=1MB/s` on a 6-broker cluster means the client can write up to 6 MB/s total if their traffic is perfectly balanced across leaders. Size accordingly.

- **Alert on chronic throttling.**
  ```
  # Per-client throttle time (broker JMX):
  kafka.server:type=Produce,user=X,client-id=Y,name=throttle-time
  kafka.server:type=Fetch,user=X,client-id=Y,name=throttle-time

  # If a client's throttle-time is > 100 ms sustained, either
  #   (a) their quota is too tight for their legitimate need — raise it, OR
  #   (b) they have a bug — fix it.
  ```

- **Remove quotas cleanly.** Deletion is by config-name, not entity:
  ```bash
  $KAFKA/kafka-configs.sh --bootstrap-server $BS \
    --entity-type clients --entity-name greedy-app \
    --alter --delete-config 'producer_byte_rate,consumer_byte_rate'
  ```

## Verify

```bash
# 1. Quota is applied for greedy-app
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type clients --entity-name greedy-app --describe
# Expected: producer_byte_rate=1048576, consumer_byte_rate=5242880

# 2. Default quota is applied
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type clients --entity-default --describe

# 3. Throttle metric fires when greedy-app produces
$KAFKA/kafka-run-class.sh kafka.tools.JmxTool \
  --jmx-url service:jmx:rmi:///jndi/rmi://localhost:9101/jmxrmi \
  --object-name 'kafka.server:type=Produce,client-id=greedy-app' \
  --one-time true 2>/dev/null | head -20
# Look for throttle-time > 0
```

## Takeaway

> **Quotas are the seat-belt of a shared cluster. Set a default; override only where justified. Client-id is hygiene; user is isolation.**

## Instructor notes
- Poll before Step 2: *"If I put a 1 MB/s quota on client-id `greedy-app`, and someone changes their client-id to `greedy-app-v2`, what happens?"* Room learns the honour-system truth in Step 4.
- The visible payoff is Terminal 2's `BytesIn/s` — dropping from ~50 MB/s to ~1 MB/s live as Step 3 runs. That's the seat-belt on video.
- Emphasise: **`entity-default` first, always.** In real deployments, forgetting the default is the #1 mistake — one new team registers a novel client-id, no quota, they burn the cluster during their canary deploy.
- The `request_percentage` (CPU) quota is often forgotten. Bandwidth quotas don't stop a metadata-flood or a create-topic loop. Add CPU quotas for suspected-abusive clients.
- Bridge back to Lab 3: *"Chapters 1–4 covered failures within the cluster — broker, replication, storage, clients. A real training deck now covers KRaft, security, and cross-region — those are separate labs."*

## Teardown

```bash
# Remove per-client quotas
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type clients --entity-name greedy-app \
  --alter --delete-config 'producer_byte_rate,consumer_byte_rate' 2>/dev/null

$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type clients --entity-default \
  --alter --delete-config 'producer_byte_rate,consumer_byte_rate' 2>/dev/null

# Delete the topic
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic i1-quota
```
