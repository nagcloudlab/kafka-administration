# Scenario 8.2 — ZK → KRaft migration (dual-write mode + cutover)

**Prereqs**
- Sc 8.1 done — you understand KRaft topology, `kafka-storage.sh format`, and can read a Raft quorum status.
- ZK cluster from Labs 1–7 available. Ideally it has a few topics + some data so the migration is not just "moving nothing".
- A fresh KRaft controller quorum you can stand up (Sc 8.1's `cluster-kraft.sh` will be adapted here to *controller-only* mode).

> This scenario has more prose than the failure-mode scenarios. Migration is a **procedure**, not a demo — the whole point is knowing the phases and what to check at each step.

**Paste at the top of every new terminal:**

```bash
cd ~/kafka-administration
export KAFKA=./kafka-lab/kafka/bin
export BS=localhost:9092,localhost:9093,localhost:9094
export ZK=localhost:2181
```

---

## Problem

- **Why migrate:** ZooKeeper is deprecated (fully removed in Kafka 4.0). Every Kafka 3.x cluster still on ZK will need to migrate before upgrading past 3.9. Rolling restart with a small window of dual-write is the official path (KIP-866).

- **The migration model:**
  - **Dual-write mode** — during migration, every metadata change (topic create, ISR update, config alter, ACL grant, etc.) is written to **both** ZooKeeper AND the KRaft metadata log. Brokers keep talking to ZK for legacy paths; the new KRaft controller quorum ingests ZK state.
  - **Migration state machine** — a per-cluster state tracked in ZK (`/migration`) and in KRaft. States progress: `NONE → PRE_MIGRATION → MIGRATION → POST_MIGRATION`.
  - **Cutover** — once every ZK broker is running in dual-write mode AND every metadata record has been replicated into KRaft, you flip brokers to KRaft-only. The old ZK ensemble becomes read-only, then irrelevant.
  - **Reversible** up to the cutover step. Once brokers no longer write to ZK, you cannot go back without a restore.

- **The five phases (each is a discrete Ops action):**

  | # | Phase | What you do | Reversible? |
  |---|-------|------------|-------------|
  | 1 | **Prep** | Bump ZK brokers to a compatible IBP; ensure inter-broker version is `3.4+`; check disk on future controller nodes | yes |
  | 2 | **Provision** | Stand up KRaft controller quorum with `zookeeper.metadata.migration.enable=true` — they're waiting for ZK state | yes |
  | 3 | **Enable dual-write** | Rolling-restart ZK brokers with `zookeeper.metadata.migration.enable=true` + `controller.quorum.voters=...` — brokers now dual-write | yes (revert broker config, restart) |
  | 4 | **Wait for sync** | Watch `MigrationState` JMX metric on the KRaft controller advance `PRE_MIGRATION → MIGRATION → POST_MIGRATION` | yes |
  | 5 | **Cutover** | Rolling-restart brokers WITHOUT ZK config + `process.roles=broker` — brokers now KRaft-only. Take down ZK. | **NO** |

- **Common failure modes:**
  - Broker restarted with dual-write config but KRaft controllers not up yet → broker waits forever for the migration to be visible; log fills with `MigrationClient... waiting for KRaft controller`.
  - Two brokers on different `inter.broker.protocol.version` — one migration-aware, one not — → metadata inconsistencies. Bump IBP everywhere BEFORE enabling migration.
  - ACL / user credentials in ZK using authorizers or SASL that KRaft doesn't yet support → migration halts. Verify with `kafka-acls.sh --list` and `kafka-configs.sh --entity-type users --describe` on both sides during dual-write.
  - Cutover before `MigrationState=POST_MIGRATION` → data loss (records that hadn't yet been dual-written are gone). **Never cut over on a hunch — always confirm via metric.**

## Symptom

Migration-time signs to know:

- **Working correctly:** brokers log `Migration state: MIGRATION → POST_MIGRATION` (per-record progress), no errors, ZK `/migration` znode present and updating.
- **Stuck at PRE_MIGRATION:** KRaft controllers can't reach ZK. Check `zookeeper.connect` on the controllers; check ZK is healthy.
- **Stuck at MIGRATION:** one or more brokers haven't been restarted into dual-write mode. `kafka-metadata-quorum.sh` still shows only KRaft controllers; brokers aren't participating. Restart the laggard broker with the migration config.
- **Broker refuses to start after cutover:** likely still has ZK config in properties. Remove `zookeeper.connect` and the migration flag; add `process.roles=broker` and `controller.quorum.voters`.

## Setup — 4 terminals

**Terminal 1 (control):**

```bash
# Verify starting state: ZK cluster from Labs 1-7 is up
./kafka-lab/cluster.sh status

# Note current topics + ACLs so we can verify they survive the migration
$KAFKA/kafka-topics.sh --bootstrap-server $BS --list
$KAFKA/kafka-acls.sh --bootstrap-server $BS --list 2>/dev/null | head -20 || echo "(no ACLs)"
```

**Terminal 2 (KRaft controller log tail — we'll open it after Step 2):**

```bash
# Placeholder; the file appears in Step 2:
# tail -F kafka-lab-migrate-ctrls/logs/controller-1.log
```

**Terminal 3 (broker 101 log tail):**

```bash
tail -F kafka-lab/logs/broker-101.log
```

**Terminal 4 (migration state probe — we'll poll from Step 4):**

```bash
# Placeholder — command in Step 4
```

## Trigger — Phase 1: Prep (bump IBP on the ZK cluster)

**Terminal 1:**

```bash
# Explicit IBP to 3.9 (or your Kafka version). Required for the migration path.
for id in 101 102 103; do
  grep -q '^inter.broker.protocol.version' kafka-lab/config/broker-$id.properties || \
    echo 'inter.broker.protocol.version=3.9' >> kafka-lab/config/broker-$id.properties
done

# Rolling restart to pick up the IBP change (graceful, from Sc 3.1)
cd kafka-lab
for id in 101 102 103; do
  ./cluster.sh stop-broker              $id
  sleep 3
  ./cluster.sh start-broker-monitoring  $id
  sleep 5
done
cd ..
```

## Trigger — Phase 2: Provision the KRaft controller quorum

We provision **controller-only** nodes (no broker role) — they'll become the target controller quorum. This is a separate cluster from Sc 8.1's combined-mode setup.

```bash
mkdir -p kafka-lab-migrate-ctrls/{config,data,logs,pids}
cd kafka-lab-migrate-ctrls
ln -sf ../kafka-lab/kafka kafka
LAB="$(pwd)"

# Generate a cluster UUID; must MATCH the ZK cluster's cluster.id
# (Not the ZK ensemble's ID — the Kafka cluster.id, which every ZK broker knows)
ZK_CLUSTER_ID=$(echo 'get /cluster/id' | ../kafka-lab/kafka/bin/zookeeper-shell.sh $ZK 2>/dev/null | \
  grep -oE '"[A-Za-z0-9_-]{22}"' | tr -d '"' | head -1)
echo "ZK cluster.id we must reuse: $ZK_CLUSTER_ID"

for id in 4 5 6; do                       # separate node.id range from the ZK brokers (101/102/103)
  ctrl_port=$((9094 + id - 3))            # 9095, 9096, 9097
  cat > config/controller-$id.properties <<EOF
process.roles=controller
node.id=$id

listeners=CONTROLLER://:$ctrl_port
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT

controller.quorum.voters=4@localhost:9095,5@localhost:9096,6@localhost:9097
log.dirs=$LAB/data/controller-$id

# ==== MIGRATION-CRITICAL flags ====
zookeeper.metadata.migration.enable=true
zookeeper.connect=$ZK
inter.broker.protocol.version=3.9
EOF
done

# Format each controller's log dir with the SAME cluster UUID as the ZK cluster
for id in 4 5 6; do
  ./kafka/bin/kafka-storage.sh format -t "$ZK_CLUSTER_ID" \
    -c config/controller-$id.properties
done

# Small start-only helper
cat > start-controllers.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
for id in 4 5 6; do
  nohup ./kafka/bin/kafka-server-start.sh config/controller-$id.properties \
    > logs/controller-$id.log 2>&1 &
  echo $! > pids/controller-$id.pid
  sleep 2
done
echo "controllers up:"
for id in 4 5 6; do echo "  controller-$id pid=$(cat pids/controller-$id.pid)"; done
EOF
chmod +x start-controllers.sh
./start-controllers.sh
cd ..
```

**Terminal 2 (open the controller log now):**

```bash
tail -F kafka-lab-migrate-ctrls/logs/controller-4.log | \
  grep --line-buffered -E 'Migration|MigrationState|Raft|Ready'
```

Expected quickly: a Raft election happens; one controller becomes leader. The migration state should be `PRE_MIGRATION` because brokers haven't been restarted yet.

## Trigger — Phase 3: Enable dual-write on each ZK broker

Add the migration flags to each ZK broker's config, one at a time, rolling-restart.

**Terminal 1:**

```bash
for id in 101 102 103; do
  cat >> kafka-lab/config/broker-$id.properties <<EOF

# ==== MIGRATION-CRITICAL (Sc 8.2 dual-write) ====
zookeeper.metadata.migration.enable=true
controller.quorum.voters=4@localhost:9095,5@localhost:9096,6@localhost:9097
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT,SSL:SSL
EOF

  cd kafka-lab
  ./cluster.sh stop-broker              $id
  sleep 3
  ./cluster.sh start-broker-monitoring  $id
  sleep 8       # give it time to register with the KRaft controller
  cd ..
done
```

**Terminal 3:** broker 101 log should show:

```
INFO ... MigrationClient ... registering with KRaft controller
INFO ... Successfully registered broker with cluster ID <UUID>
```

## Observe — Phase 4: Watch the migration state advance

**Terminal 4:**

```bash
while true; do
  echo "--- $(date +%T) ---"
  $KAFKA/kafka-run-class.sh kafka.tools.JmxTool \
    --jmx-url service:jmx:rmi:///jndi/rmi://localhost:9101/jmxrmi \
    --object-name 'kafka.controller:type=KafkaController,name=MigrationState' \
    --one-time true 2>/dev/null | grep -oE 'Value=[a-zA-Z_]+'
  sleep 3
done
```

Expected progression (over seconds-to-minutes depending on ZK metadata size):

- `Value=PRE_MIGRATION`  — controllers up, brokers not yet dual-writing.
- `Value=MIGRATION`       — migration actively ingesting ZK records into the metadata log.
- `Value=POST_MIGRATION`  — all ZK state has been replicated. Safe to proceed.

Also verify the topic and ACL state survived:

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS --list
$KAFKA/kafka-acls.sh --bootstrap-server $BS --list 2>/dev/null | head
```

Same output as before the migration.

## Trigger — Phase 5: Cutover (one-way; skip if you want to rehearse rollback)

**Do NOT run this until `MigrationState=POST_MIGRATION` is confirmed.**

For each broker in turn:

```bash
for id in 101 102 103; do
  # Comment out ZK bits, add process.roles=broker (no ZK, no dual-write)
  sed -i \
    -e 's/^zookeeper.connect=/# zookeeper.connect=/' \
    -e 's/^zookeeper.metadata.migration.enable=/# zookeeper.metadata.migration.enable=/' \
    kafka-lab/config/broker-$id.properties
  grep -q '^process.roles=' kafka-lab/config/broker-$id.properties || \
    echo 'process.roles=broker' >> kafka-lab/config/broker-$id.properties

  cd kafka-lab
  ./cluster.sh stop-broker              $id
  sleep 3
  ./cluster.sh start-broker-monitoring  $id
  sleep 5
  cd ..
done
```

After every broker is KRaft-only, take down the ZK-migration-mode flag on the controllers and take ZooKeeper down:

```bash
# Remove the migration flag from each controller (they're now permanent KRaft controllers)
for id in 4 5 6; do
  sed -i '/^zookeeper\./d;/^zookeeper.metadata.migration.enable=/d' \
    kafka-lab-migrate-ctrls/config/controller-$id.properties
done

# Rolling restart of controllers
for id in 4 5 6; do
  kill $(cat kafka-lab-migrate-ctrls/pids/controller-$id.pid) 2>/dev/null || true
  sleep 3
  cd kafka-lab-migrate-ctrls && ./start-controllers.sh && cd ..
done

# Stop ZooKeeper — no longer used
cd kafka-lab && ./cluster.sh stop-zk && cd ..
```

## Observe

- `kafka-metadata-quorum.sh --bootstrap-controller localhost:9095 describe --status` — quorum healthy, leader elected.
- `ss -ltnp | grep :2181` — nothing. ZK is off.
- All topics still visible: `$KAFKA/kafka-topics.sh --bootstrap-server $BS --list`.
- All prior ACLs still enforced (from Sc 7.2 grants).

## Solution

- **Migration checklist to burn into the runbook:**
  1. Bump `inter.broker.protocol.version=3.4+` on every broker. Wait one week for stability.
  2. Take a ZK snapshot / backup.
  3. Provision controller quorum. Verify they're up and elected.
  4. Rolling restart ONE broker with migration flags. Verify dual-write is working on THAT broker (log line + JMX). Only then proceed to the next broker.
  5. Wait for `POST_MIGRATION`. Do not proceed on time; proceed on metric.
  6. Cutover one broker at a time. If any refuses, roll back that one broker's config and figure out why before continuing.
  7. Only decommission ZK **after** all brokers are KRaft-only for at least 24 hours.

- **Rollback (before cutover, Phase 5):** revert each broker's config to remove the migration flags + `controller.quorum.voters`, restart. Brokers go back to ZK-only. KRaft controllers can be shut down and their data dirs wiped.

- **Rollback after cutover:** not supported. Requires restoring the ZK snapshot from step 2 and reverting every broker's config. Practically: you'd rebuild the cluster and re-mirror.

- **Skipping migration entirely:** if you can afford downtime, provision a fresh KRaft cluster (Sc 8.1) and use MirrorMaker 2 to move topics + offsets over. Then switch client bootstrap servers. Simpler, slower, cheaper in cognitive load.

## Verify

```bash
# 1. No dual-write flag left in broker configs
grep -c 'zookeeper' kafka-lab/config/broker-*.properties
# Expected: 0 (or only commented lines)

# 2. No ZK listener
ss -ltnp 2>/dev/null | grep :2181 || echo "no ZK"

# 3. KRaft quorum healthy
./kafka-lab/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-controller localhost:9095 describe --status

# 4. Cluster is functional
$KAFKA/kafka-topics.sh --bootstrap-server $BS --list
$KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic __consumer_offsets | head -6

# 5. Sc 3.2 (stale broker epoch) is now impossible — there's no znode to delete
echo 'ls /brokers/ids' | ./kafka-lab/kafka/bin/zookeeper-shell.sh $ZK 2>&1 | \
  head -3 || echo "ZK is gone — Sc 3.2 can no longer happen"
```

## Takeaway

> **Migration = phases, not steps. Bump IBP → provision controllers → dual-write → wait for POST_MIGRATION → cutover. The metric drives every decision.**

## Instructor notes
- This scenario is the longest in the whole training. Consider running it once **before** the class and demoing the state transitions on video/screenshots rather than live — the wait for POST_MIGRATION is dead air if the cluster has meaningful metadata.
- The MigrationState JMX metric IS the tempo of this scenario. Draw it on the whiteboard as a state machine before showing the terminal.
- Emphasize the **one-way arrow** at cutover. Once brokers stop writing to ZK, ZK stops being authoritative. Everyone remembers this after they've been burned by it once.
- Real-world tip: for large clusters, migration can take hours. Schedule it during a quiet window; monitor via Grafana with the JMX metric on the dashboard.
- Bridge to next labs (planned): *"With KRaft you've eliminated ZK. Next up: cross-cluster replication (MM2 / Cluster Linking), tiered storage, and observability at scale — each their own lab."*

## Teardown

For classroom cleanup after a rehearsal:

```bash
# Stop KRaft controllers
for id in 4 5 6; do
  kill $(cat kafka-lab-migrate-ctrls/pids/controller-$id.pid) 2>/dev/null
done
rm -rf kafka-lab-migrate-ctrls

# Restore original ZK broker configs
for id in 101 102 103; do
  # Remove migration + cutover additions
  sed -i \
    -e '/^# ==== MIGRATION-CRITICAL/,/^$/d' \
    -e '/^process.roles=broker$/d' \
    -e 's/^# zookeeper\./zookeeper./' \
    kafka-lab/config/broker-$id.properties
done

# Bring the ZK cluster back up as it was
cd kafka-lab && ./cluster.sh start
```
