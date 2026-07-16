# Scenario 10.1 — Enable tiered storage and watch segments offload

**Prereqs**
- Lab-05 done (segments, retention, compaction).
- Cluster up: `./cluster.sh start-monitoring` in `kafka-lab/`.
- Kafka 3.9+ distribution.

**Paste at the top of every new terminal:**

```bash
cd ~/kafka-administration/kafka-lab
export KAFKA=./kafka/bin
export BS=localhost:9092,localhost:9093,localhost:9094
export ZK=localhost:2181
```

---

## Problem

- **Why tiered storage exists.** Kafka's default is "all data on broker disk, forever (up to `retention.ms`)". That works until:
  - You want long retention (30d, 90d, 1y) — broker disks fill up. Big brokers = expensive brokers.
  - You do compliance replay — need 6 months of history for one topic; brokers scale for the hot tail.
  - You add capacity — instead of adding brokers, you'd rather add cheaper object-storage.
- **KIP-405** splits a topic's log into two tiers:
  - **Local tier** — on broker disk, always. Recent segments. Bounded by `local.retention.ms` / `local.retention.bytes`.
  - **Remote tier** — object storage (S3, GCS, Azure Blob, or here `LocalTieredStorage`). Older segments. Bounded by `retention.ms` / `retention.bytes` (the topic's total-history cap).
- **Consumer semantics are unchanged.** A consumer reading old offsets triggers a broker to fetch from the remote tier transparently. Slower than local reads (network hop) but works.

- **What Kafka runs internally:**
  - **RemoteStorageManager (RSM)** — plugin the broker talks to for read/write to remote. `S3RemoteStorageManager`, `LocalTieredStorage`, etc.
  - **RemoteLogMetadataManager (RLMM)** — plugin the broker uses to track which segments live on remote. Default: `TopicBasedRemoteLogMetadataManager` — metadata itself in a Kafka topic `__remote_log_metadata`.
  - **Remote log manager threads** — background workers on each broker that copy rolled segments to remote, prune expired segments, and answer remote-fetch requests.

- **The lifecycle of a segment:**
  1. Producer writes to active segment on local disk.
  2. Segment reaches `segment.ms` / `segment.bytes` and rolls (becomes read-only, non-active).
  3. Remote log manager thread copies it to remote storage via the RSM.
  4. RLMM records "segment X for partition Y is at remote location Z".
  5. Once copied, segment is eligible for local deletion when local retention says so.
  6. Consumer at old offset → broker reads from remote via RSM → returned to consumer.

- **Two retention knobs (per topic):**
  ```properties
  retention.ms = 30d                   # total history (local + remote). Data older than this is deleted from BOTH tiers.
  local.retention.ms = 12h             # keep only 12h on local disk. Older stays in remote until retention.ms expires.
  ```
  - `local.retention.ms=-1` — keep everything on local (default). No point enabling tiering.
  - `local.retention.ms=0` — offload as soon as segment rolls; local disk holds only the active segment.

- **Common misconfigurations:**
  - Enabling `remote.log.storage.system.enable=true` on brokers but forgetting `remote.storage.enable=true` on the **topic** → nothing offloads. Silent.
  - `local.retention.ms > retention.ms` → local is bigger than total. Kafka clamps but doesn't warn cleanly.
  - Undersized RSM connection pool → remote fetches slow, tail consumers lag.
  - Deleting remote-metadata Kafka topic (`__remote_log_metadata`) — brokers lose track of remote segments; they're still on remote but unreachable. Recover with a purge tool + reindex — days of work.

## Symptom
- **Configured but nothing offloading:** `local.retention.ms` is default (-1), or the topic is missing `remote.storage.enable=true`. Check broker log for `RemoteLogManager` activity — silent means it's not working.
- **Consumer reads slow for old offsets:** normal — expect ~500 ms-per-segment overhead for remote fetch. Local reads are µs.
- **Broker log spam:** `Failed to copy segment to remote` — plugin credentials/perms issue.
- **Disk still full after enabling:** offload lag; check `RemoteLogManager` JMX for `TotalRemoteBytes` growing while local shrinks.

## Setup — 3 terminals

**Terminal 1 (control):**

```bash
cd ~/kafka-administration/kafka-lab

# The LocalTieredStorage plugin ships in a test-jar in the Kafka distribution.
# Locate it so we can put it on the classpath.
find kafka/libs -name 'kafka-storage*.jar' -o -name 'kafka-storage-api*.jar'  | \
  head; find kafka/libs -name '*local*tiered*.jar' 2>/dev/null | head
# On some distributions the class is packaged under kafka/libs already; on others it's
# in kafka/test-jars/. Both should work with the config below.
```

If your Kafka 3.9 distribution doesn't ship `LocalTieredStorage`, download from Maven Central:
```bash
# Only if the find above turned up nothing:
curl -L -o kafka/libs/kafka-storage-test.jar \
  https://repo1.maven.org/maven2/org/apache/kafka/kafka-storage/3.9.0/kafka-storage-3.9.0-test.jar
```

**Terminal 2 (segment inspector — leave running):**

```bash
watch -n 3 '
echo === LOCAL segments per broker ===
for id in 101 102 103; do
  n=$(ls kafka-lab/data/broker-$id/tier-demo-0/*.log 2>/dev/null | wc -l)
  echo "  broker $id local segments (partition 0): $n"
done

echo
echo === REMOTE segments ===
find /tmp/kafka-tiered-storage 2>/dev/null | wc -l | \
  xargs -I{} echo "  files under /tmp/kafka-tiered-storage: {}"
'
```

**Terminal 3 (broker 101 remote-log activity):**

```bash
tail -F kafka-lab/logs/broker-101.log | \
  grep --line-buffered -E 'RemoteLogManager|RemoteStorageManager|remote|Tiered'
```

## Trigger — Step 1: enable tiered storage cluster-wide

**Terminal 1:**

```bash
mkdir -p /tmp/kafka-tiered-storage

for id in 101 102 103; do
  # Skip if already added (idempotent)
  grep -q 'remote.log.storage.system.enable' config/broker-$id.properties && continue

  cat >> config/broker-$id.properties <<EOF

# ==== Tiered Storage (Sc 10.1) ====
remote.log.storage.system.enable=true

# --- Remote Storage Manager (plugin) ---
remote.log.storage.manager.class.name=org.apache.kafka.server.log.remote.storage.LocalTieredStorage
remote.log.storage.manager.impl.prefix=rsm.config.
rsm.config.dir=/tmp/kafka-tiered-storage

# --- Remote Log Metadata Manager (uses an internal Kafka topic) ---
remote.log.metadata.manager.class.name=org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager
remote.log.metadata.manager.impl.prefix=rlmm.config.
rlmm.config.remote.log.metadata.topic.replication.factor=3
rlmm.config.remote.log.metadata.topic.num.partitions=5
EOF
done

tail -12 config/broker-101.properties
```

## Trigger — Step 2: rolling restart

```bash
for id in 101 102 103; do
  ./cluster.sh stop-broker              $id
  sleep 3
  ./cluster.sh start-broker-monitoring  $id
  sleep 6
done
```

**Terminal 3** should show lines like:

```
INFO [RemoteLogManager=101] Started RemoteLogManager
INFO Creating topic-based RLMM
INFO [RemoteStorageManager] LocalTieredStorage initialized with rootDir=/tmp/kafka-tiered-storage
```

If instead you see `ClassNotFoundException: org.apache.kafka.server.log.remote.storage.LocalTieredStorage` — the plugin isn't on the broker classpath. Add it (§ Setup Step 0) and restart.

## Trigger — Step 3: create a topic with tiering enabled, aggressive retention

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --create --topic tier-demo --partitions 3 --replication-factor 3 \
  --config remote.storage.enable=true \
  --config segment.ms=15000 \
  --config local.retention.ms=30000 \
  --config retention.ms=600000

# Confirm the per-topic settings
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type topics --entity-name tier-demo --describe | tr ',' '\n' | grep -i 'retention\|segment\|remote'
```

Config explained:
- `segment.ms=15000` — roll segments every 15 s (small so the demo is watchable).
- `local.retention.ms=30000` — keep only 30 s on local disk.
- `retention.ms=600000` — total 10 min of history (local + remote combined).
- `remote.storage.enable=true` — this topic uses the remote tier.

## Trigger — Step 4: produce steadily

```bash
$KAFKA/kafka-producer-perf-test.sh \
  --topic tier-demo \
  --num-records 200000 \
  --record-size 1000 \
  --throughput 200 \
  --producer-props bootstrap.servers=$BS acks=all
```

At 200 records/sec × 1 KB = ~200 KB/s. Segments roll every 15 s, so each is ~3 MB. Total run ~15 min.

## Observe (watch T2 and T3 as the producer runs)

- **First 30 s** — active segment fills, no rolls yet. Local segment count = 1 per partition; `/tmp/kafka-tiered-storage/` empty.
- **~30-45 s** — first segments roll. Local count = 2, then 3.
- **~60-75 s** — first rolled segments are older than `local.retention.ms=30 s`. Broker's RemoteLogManager offloads them. **T3** shows lines like:
  ```
  Copying segment 000000000000000000.log to remote for tier-demo-0
  Successfully copied segment
  ```
- **T2** — remote file count grows in `/tmp/kafka-tiered-storage/`; local segment count stays low (usually 1-2 per partition — the active + maybe one recently-rolled awaiting offload).

- **~10 min in** — total history exceeds `retention.ms=10 min`. Oldest remote segments get deleted. Steady-state: local disk small, remote disk holds up to 10 min of history.

## Observe — consumer reads old data transparently

Stop the producer (Ctrl+C). Then consume from the beginning:

```bash
$KAFKA/kafka-console-consumer.sh --bootstrap-server $BS \
  --topic tier-demo --from-beginning --timeout-ms 5000 | wc -l
```

Expected: prints a message count matching the retained window. The broker fetched OLD offsets from `/tmp/kafka-tiered-storage/` and returned them just like local reads — consumer had no idea.

**Time the difference** — local vs remote fetch:

```bash
# Recent consumer (all data local)
time $KAFKA/kafka-console-consumer.sh --bootstrap-server $BS \
  --topic tier-demo --from-beginning --timeout-ms 3000 \
  --property print.timestamp=true 2>/dev/null | tail -100 > /dev/null

# Old consumer (traverses remote)
# The `--from-beginning` on a large offset range forces the broker to fetch old
# remote segments — you'll see it take noticeably longer than the recent read.
```

## Solution

- **Enable per topic, not cluster-wide.** `remote.log.storage.system.enable=true` on brokers only lets you tier; individual topics opt in via `remote.storage.enable=true`. Compact topics can't be tiered (yet).

- **Sizing recipe:**
  - `local.retention.ms` = your consumer's "typical tail-lag SLO" × safety factor.
    - Real-time consumers rarely fall > 5 min behind → `local.retention.ms=1h`. Reads are local; latency stays low.
    - Batch consumers process the last 24h once a day → `local.retention.ms=25h`. Still local for their pass.
    - Archive replay consumers touch old data occasionally → they'll hit remote; slow is fine.
  - `retention.ms` = your compliance / replay SLO. Weeks to months.
  - Rule of thumb: `local.retention.ms` should hold ≥ 99% of read traffic. Remote is for the long tail.

- **Choose the RSM plugin:**
  - **Apache LocalTieredStorage** (this lab) — testing only; not for real clusters.
  - **Aiven Open Source** — S3, GCS, Azure. Actively developed. Good starting point.
  - **Confluent Tiered Storage** — Confluent Platform / Cloud only.
  - **Custom** — implement `RemoteStorageManager`; usually not worth it.

- **Monitor these JMX metrics:**
  - `kafka.log.remote:type=RemoteLogManager,name=RemoteFetchBytesPerSec` — how much reading from remote.
  - `kafka.log.remote:type=RemoteLogManager,name=RemoteCopyBytesPerSec` — how much offloading.
  - `kafka.log.remote:type=RemoteLogManager,name=RemoteCopyErrorPerSec` — non-zero = plugin misbehaving. Alert.
  - `kafka.log.remote:type=RemoteLogManager,name=RemoteFetchErrorPerSec` — same, but on consumer path. Non-zero → consumers see failures.
  - `kafka.log.remote:type=RemoteLogManager,name=RemoteLogSizeBytes` — total bytes in remote per partition. Grows to `retention.bytes`.

- **Cost model reality check:**
  - Local NVMe: ~$0.20/GB-month.
  - S3 Standard: ~$0.023/GB-month.
  - Cross-AZ egress (if broker reads from remote in different AZ than S3 endpoint): $0.01/GB.
  - For a 100 TB dataset with 1% hot: local-only ≈ $20k/month; tiered ≈ $2.3k/month. But egress on frequent replays can eat the savings.

## Verify

```bash
# 1. Tiered storage is enabled on brokers
grep -c 'remote.log.storage.system.enable=true' config/broker-*.properties
# Expected: 3

# 2. Topic has tiering on
$KAFKA/kafka-configs.sh --bootstrap-server $BS --entity-type topics \
  --entity-name tier-demo --describe | grep -o 'remote.storage.enable=true'

# 3. Segments were offloaded (files exist under /tmp/kafka-tiered-storage)
find /tmp/kafka-tiered-storage -name '*.log' | head -3
find /tmp/kafka-tiered-storage -name '*.log' | wc -l
# Expected: > 0

# 4. Local disk stays bounded — count local segments per partition
for id in 101 102 103; do
  for p in 0 1 2; do
    n=$(ls kafka-lab/data/broker-$id/tier-demo-$p/*.log 2>/dev/null | wc -l)
    echo "broker $id partition $p local .log files: $n"
  done
done
# Expected: 1-3 per partition (active + recently rolled awaiting offload)

# 5. RemoteLogManager active in JMX
$KAFKA/kafka-run-class.sh kafka.tools.JmxTool \
  --jmx-url service:jmx:rmi:///jndi/rmi://localhost:9101/jmxrmi \
  --object-name 'kafka.log.remote:type=RemoteLogManager,name=RemoteCopyBytesPerSec' \
  --one-time true 2>/dev/null | head -3
```

## Takeaway

> **Local for the hot 1%, remote for the long tail. `local.retention.ms` sets the boundary; consumer semantics don't change.**

## Instructor notes
- Poll before Step 4: *"If I enable tiered storage on Monday and the topic already has 30d of data, what happens?"* Answer: existing segments are eligible for offload as soon as they exceed `local.retention.ms`. The remote log manager back-fills them opportunistically over hours. **Not** an instant migration.
- Watch `/tmp/kafka-tiered-storage/` filling as segments offload — the "remote" is just files, easy to `ls` and eyeball. In production, replace with `s3://bucket/prefix` and `aws s3 ls` walks the same shape.
- Compact topics can't be tiered — the log cleaner needs random-access reads that plugins don't optimise for. Trainees who ask "can I tier my compacted user-profile topic?" — answer: no, only delete-policy topics.
- The `RemoteLogSizeBytes` JMX metric is worth putting on the Grafana Storage dashboard as a follow-up. Skipped here to keep the JMX config compact.
- Bridge to Lab 11: *"You now have data on multiple tiers, across multiple clusters. Observability has to keep up — alerts, SLOs, and per-tier metrics. Next lab."*

## Teardown

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic tier-demo

# Full disable — remove the tiered-storage lines and rolling restart
for id in 101 102 103; do
  sed -i '/^# ==== Tiered Storage (Sc 10.1) ====$/,/^$/d' config/broker-$id.properties
done
# The internal __remote_log_metadata topic stays; delete manually if you want a fully clean cluster:
# $KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic __remote_log_metadata

for id in 101 102 103; do
  ./cluster.sh restart-broker-monitoring $id
  sleep 5
done

rm -rf /tmp/kafka-tiered-storage
```
