# Scenario 3.2 — Stale broker epoch (the incident, on purpose)

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

- **Broker registration in ZK mode:**
  - On start, each broker creates an **ephemeral** znode at `/brokers/ids/N` (where N = `broker.id`).
  - Ephemeral = tied to that broker's ZK session; disappears automatically when the session ends.
  - The znode's JSON payload carries listener protocols, endpoints, JMX port, and a **broker epoch** (registration timestamp in ms).
  - On restart, ZK issues a new session and creates a new ephemeral with a fresher epoch. The controller reads the new epoch and knows "this is broker N's next incarnation."

- **The trap (the incident):**
  - Operator runs `./cluster.sh start` while broker N is already up.
  - The fresh JVM tries to create `/brokers/ids/N` → `NodeExistsException` — because the **live** broker already owns that znode. The fresh JVM exits, correctly.
  - Wrong reflex: *"znode exists but I don't see a broker, must be stale — delete it."*
  - The delete succeeds. It also removes the **live** broker's registration.

- **What happens next (the zombie state):**
  - The still-running broker doesn't know its registration was revoked. It keeps sending fetch requests, heartbeats, and produce responses using its now-stale epoch.
  - The controller compares the epoch in every incoming request to its known-good registry. The epoch no longer matches anything → **the controller silently ignores everything the broker says.** That is exactly what `stale broker epoch (N), retrying` means — the broker asking to update its own ISR state, being told nothing.
  - Meanwhile the broker's Kafka listener is still bound to 9092. Clients can connect. Metadata requests reach the broker. But the broker's cache is stale (the controller stopped updating it), so its `MetadataResponse` omits itself as a live broker and lists no valid leaders.
  - Producers see this as "no live brokers for these partitions" → hang on metadata → `TimeoutException` after `max.block.ms`.

- **Why SIGTERM cannot recover it:**
  - Controlled shutdown requires the controller to accept a `ControlledShutdownRequest`. The controller has already fenced this broker, so the request is dropped.
  - The broker's shutdown thread waits for a response that never comes → SIGTERM hangs.
  - `kill -9` is the **correct** move here — the opposite lesson from 1.1.

## Symptom
- Broker startup log (double-start): `KeeperException$NodeExistsException` on `/brokers/ids/N`.
- After the mistaken delete, running brokers spam:
  - The broker with a live-but-fenced session: `Broker had a stale broker epoch (N), retrying`
  - Others touching `ProducerId` / `TransactionCoordinator`: `Our broker ID is not yet known by the controller, trying again`
- Producer: `send()` blocks up to `max.block.ms` (60 s default) → `TimeoutException: Topic ... not present in metadata`.
- Consumer: `poll()` returns empty forever.
- ZK: `ls /brokers/ids` returns `[]`; broker ports still bound (`ss -tlnp` shows LISTEN by java PIDs).

## Setup — 5 terminals

**Terminal 1 (control):**

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --create --topic a2-stale-epoch --partitions 3 --replication-factor 3
```

**Terminal 2 (ZK broker view):**

```bash
watch -n 1 "echo 'ls /brokers/ids' | \
  $KAFKA/zookeeper-shell.sh $ZK 2>/dev/null | tail -3"
```

- Baseline: `[101, 102, 103]`.

**Terminal 3 (console producer):**

```bash
$KAFKA/kafka-console-producer.sh --bootstrap-server $BS --topic a2-stale-epoch
```

- At the `>` prompt, type `hello-1`, `hello-2`, ... — one line every few seconds. Keep interactive.

**Terminal 4 (consumer from beginning):**

```bash
$KAFKA/kafka-console-consumer.sh --bootstrap-server $BS \
  --topic a2-stale-epoch --from-beginning
```

- Baseline: every line from T3 appears here.

**Terminal 5 (broker 101 log):**

```bash
tail -F logs/broker-101.log
```

## Trigger — Part 1: `NodeExists` on double-start

```bash
./cluster.sh start
```

## Observe — Part 1
- **T5:** `NodeExistsException` on `/brokers/ids/101`; the just-attempted broker JVM shuts down. The **already-running** broker 101 is untouched.
- `cluster.sh` says `broker 101 did not bind 9092 in time` — misleading; the real cause is in the log.
- **T2:** still `[101, 102, 103]`.
- **T3 / T4:** still working. No client impact.

## Diagnosis — before Part 2, verify who owns the port

Paste the whole block:

```bash
# Q1: PID listening on 9092
PID=$(ss -tlnp 2>/dev/null | awk '/:9092 /{sub(/.*pid=/,""); sub(/,.*/,""); print}')
echo "PID on 9092: $PID"

# Q2: is that PID a live Kafka broker?
ps -p "$PID" -o pid,etime,stat,cmd

# Q3: how fresh is the znode? Compare "timestamp" to now.
echo 'get /brokers/ids/101' | $KAFKA/zookeeper-shell.sh $ZK 2>/dev/null | tail -3
echo "now (ms): $(date +%s%3N)"
```

**Interpret:**
- Q1 returns a PID **and** Q2 shows `STAT=Sl`, `CMD=java ... kafka.Kafka` **and** Q3 timestamp is within minutes of "now (ms)" → **broker is alive; do not delete.**
- The fix is to stop trying to double-start, not to touch ZK.

## Trigger — Part 2: the wrong move (delete the live znode)

*Training moment only:*

```bash
printf 'delete /brokers/ids/101\ndelete /brokers/ids/102\ndelete /brokers/ids/103\n' | \
  $KAFKA/zookeeper-shell.sh $ZK
```

## Observe — Part 2
- **T2:** `[]`. All brokers unregistered.
- **T5:** broker 101 spams `stale broker epoch`. Peek at `logs/broker-102.log` / `logs/broker-103.log`: `Our broker ID is not yet known by the controller`.
- **T3:** next line typed blocks silently. After ~60 s → `TimeoutException: Topic ... not present in metadata`.
- **T4:** no new messages.
- Sanity — ports still bound:
  ```bash
  ss -tlnp 2>/dev/null | grep -E ':(9092|9093|9094)\b'
  ```
- Brokers are **alive-but-fenced zombies.**

## Solution

- **Zombie brokers cannot recover on their own.**
  - Their fetch/heartbeat threads keep retrying, spamming logs but making no forward progress.
  - Their controlled-shutdown path is fenced too, so SIGTERM hangs.
  - `kill -9` is the correct move here (opposite of 1.1).

- **Full recovery sequence** (Terminal 1):
  ```bash
  # SIGKILL all three broker JVMs
  for port in 9092 9093 9094; do
    pid=$(ss -tlnp 2>/dev/null | \
      awk -v p=":$port" '$4 ~ p"$" {sub(/.*pid=/,""); sub(/,.*/,""); print}')
    [[ -n "$pid" ]] && kill -9 "$pid"
  done

  # Confirm ports free
  ss -tlnp 2>/dev/null | grep -E ':(9092|9093|9094)\b' || echo "all ports free"

  # Clean stale pid files (cluster.sh would skip otherwise)
  rm -f pids/broker-101.pid pids/broker-102.pid pids/broker-103.pid

  # Fresh start — ZK still up, brokers re-register cleanly
  ./cluster.sh start
  ```

- **Prevention rule (long form):**
  - Never delete a znode under `/brokers/ids/*` without confirming the corresponding port has no live owner.
  - Ephemerals **look** stale during ZK failover — after a ZK restart, ephemerals from the previous life are held for one `session.timeout.ms` while sessions try to reconnect. During that window they are indistinguishable from truly orphaned znodes.
  - The only reliable staleness test is on the broker **host**: `ss -tlnp` (or `lsof -iTCP:9092`). If nothing owns the port, the znode is truly orphaned.

- **KRaft eliminates this class of incident.**
  - Broker registration is a Raft log entry in `__cluster_metadata`, not an ephemeral coordination primitive.
  - There's no znode to accidentally delete.
  - Epoch is bumped monotonically on every restart by the controller quorum; a stale epoch causes silent fencing, not zombie state.
  - Covered in a later chapter.

## Verify

```bash
# 1. Brokers registered
echo 'ls /brokers/ids' | $KAFKA/zookeeper-shell.sh $ZK 2>/dev/null | tail -3
# Expected: [101, 102, 103]

# 2. Topic healthy
$KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic a2-stale-epoch
# Expected: Isr = 101,102,103 on every partition

# 3. End-to-end proof
echo "verified" | $KAFKA/kafka-console-producer.sh --bootstrap-server $BS --topic a2-stale-epoch
# T4 should print: verified
```

## Takeaway

> **Ephemeral znodes belong to live sessions, not dead processes. Check the port before you delete.**

## Instructor notes
- Poll before Part 2: *"What happens if we delete an ephemeral znode owned by a live broker?"* Most guess "nothing" or "crash." Neither is right — the answer (fenced-but-alive) isn't obvious until they see it.
- Screenshot the fingerprint lines (`stale broker epoch`, `Our broker ID is not yet known by the controller`) — one grep across broker logs identifies this in seconds in prod.
- Diagnostic order matters: `ps → ss -tlnp → znode timestamp`. OS layer answers "alive?" faster than the coordinator layer.
- SIGTERM cannot recover a zombified broker — this is the one Lab 3 case where `kill -9` is correct. Opposite lesson of 1.1.
- Bridge to 1.3: *"1.1 gone. 1.2 fenced. 1.3 alive **and** registered but behind on data — the URP alert."*

## Teardown

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic a2-stale-epoch
```
