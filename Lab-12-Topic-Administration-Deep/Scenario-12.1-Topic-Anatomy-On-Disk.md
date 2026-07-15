# Scenario 12.1 — Topic anatomy on disk (segments, indexes, offsets)

**Prereqs**
- Labs 1 and 3 done.
- Cluster on 9092–9094 running: `./cluster.sh start-monitoring` in `kafka-lab/`.

**Paste at the top of every new terminal:**

```bash
cd ~/kafka-administration/kafka-lab
export KAFKA=./kafka/bin
export BS=localhost:9092,localhost:9093,localhost:9094
export ZK=localhost:2181
```

---

## Problem

- **You will one day need to know exactly what's on disk.** Every real Kafka incident eventually forces an admin to look inside a partition directory: a disk-full alert, a corrupt segment error, a `kafka-dump-log.sh` investigation, a suspicious "cleaner is dead" log line, or a request from the compliance team to prove a specific message existed at a specific offset.
- **A topic's physical layout on a broker:**
  ```
  data/broker-101/                      # broker's log.dirs
  └── orders.events-0/                  # one directory per partition-replica on this broker
      ├── 00000000000000000000.log         # segment data (records + headers)
      ├── 00000000000000000000.index       # offset → position lookup
      ├── 00000000000000000000.timeindex   # timestamp → offset lookup
      ├── 00000000000000000000.snapshot    # producer-id state (idempotence)
      ├── leader-epoch-checkpoint          # leader epoch history
      ├── partition.metadata               # topic-id + version
      ├── 00000000000000042817.log         # ROLLED segment (base offset = 42817)
      ├── 00000000000000042817.index
      └── 00000000000000042817.timeindex
  ```
- **Segment naming**: the digits in the filename are the **base offset** — the offset of the first record in that segment. `00000000000000042817.log` contains records from offset 42817 onward, until the segment rolled.
- **Three file types per segment**:
  - **`.log`** — the actual record data. Sequential, append-only, immutable once rolled.
  - **`.index`** — sparse (offset → byte position) map. Not every record indexed; only ~every 4 KB. Lets the broker skip most of a segment when fetching a specific offset.
  - **`.timeindex`** — sparse (timestamp → offset) map. Lets consumers seek by time.
- **Rolling triggers** (any one causes the active segment to close and a new one to open):
  - `segment.bytes` reached (default 1 GB)
  - `segment.ms` elapsed since the segment was created (default 7 days, `-1` = disabled)
  - Broker restart (the current segment stays as-is; new segment starts at next write)
- **Retention operates on ROLLED segments only.** The currently-active segment is never eligible for deletion regardless of `retention.ms`.

- **Common misconceptions this scenario dispels:**
  - *"Retention deletes messages older than N days"* → No. It deletes whole segments whose newest record is older than N days.
  - *"I can grep the .log file"* → No. It's a binary format; use `kafka-dump-log.sh`.
  - *"Deleting a .log file frees space"* → No. Broker still holds a file handle; segment index gets confused. Never delete log files manually.
  - *"An offset points to a file position"* → Only approximately. The `.index` maps offset → position for indexed offsets; broker binary-searches then scans.

## Symptom

Nothing goes wrong here — this is investigative. But the fingerprint symptoms that force an admin to open a partition directory:

- Disk-full alert on a broker → which topic / partition is the largest?
- `Number of alive brokers < min.insync.replicas` errors from a producer → is the segment size unusual for that topic?
- Consumer's `poll()` returns weirdly old records → is the timeindex behaving?
- `RemoteLogManager` errors (from Lab 10) → is the log directory shape as expected?

## Setup — 3 terminals

**Terminal 1 (control):**

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --create --topic anatomy-demo --partitions 3 --replication-factor 3 \
  --config segment.bytes=524288 \
  --config segment.ms=60000
```

- `segment.bytes=524288` (512 KB) — small segments so multiple roll during the demo.
- `segment.ms=60000` (60 s) — segments roll on time too.

**Terminal 2 (directory watch — leave running):**

```bash
watch -n 2 "echo '=== broker-101 ==='; ls -la data/broker-101/anatomy-demo-*/ 2>/dev/null | grep -E '\.(log|index|timeindex|snapshot)$'"
```

**Terminal 3 (broker log tail):**

```bash
tail -F logs/broker-101.log | grep --line-buffered -E 'anatomy-demo|Rolled|Deleting segment'
```

## Trigger — Step 1: produce enough data to force segment rolls

```bash
# 2000 records × 1 KB each × 3 partitions = ~2 MB per partition
# With segment.bytes=512 KB, that's ~4 segments per partition
$KAFKA/kafka-producer-perf-test.sh \
  --topic anatomy-demo \
  --num-records 2000 --record-size 1000 --throughput 200 \
  --producer-props bootstrap.servers=$BS acks=all
```

**T2 watch**: over ~10 s, you'll see:
- Initially one segment `00000000000000000000.log`.
- Then a second segment appears (`00000000000000000???.log` — base offset = last offset of the previous segment + 1).
- Eventually 3-4 segments per partition.

**T3 log**: `Rolled new log segment at offset N for anatomy-demo-0 in Xms (kafka.log.LocalLog)`.

## Observe — Step 2: dissect one partition

**Terminal 1:**

```bash
# 1. List every file in partition 0 with sizes
ls -lah data/broker-101/anatomy-demo-0/

# 2. Total bytes in this partition-replica (should approximate what you produced)
du -sh data/broker-101/anatomy-demo-0/
```

Expected output shape:
```
-rw-r--r-- 1 nag nag  524K Jul 15 04:12 00000000000000000000.log
-rw-r--r-- 1 nag nag   10M Jul 15 04:12 00000000000000000000.index      # pre-allocated
-rw-r--r-- 1 nag nag   10M Jul 15 04:12 00000000000000000000.timeindex
-rw-r--r-- 1 nag nag  524K Jul 15 04:12 00000000000000000632.log        # rolled — base offset 632
-rw-r--r-- 1 nag nag   10M Jul 15 04:12 00000000000000000632.index
-rw-r--r-- 1 nag nag   10M Jul 15 04:12 00000000000000000632.timeindex
-rw-r--r-- 1 nag nag   32K Jul 15 04:12 00000000000000001240.log        # ACTIVE (partial)
...
partition.metadata
leader-epoch-checkpoint
```

Notes:
- Index files are **pre-allocated** to 10 MB by default (see `segment.index.bytes`). They sparsely fill up as records are written; the file size on disk vs actual data can differ.
- The **active segment** is whichever `.log` is currently being appended to. Retention never touches it.

## Observe — Step 3: read a segment with `kafka-dump-log.sh`

**Terminal 1:**

```bash
# Look at the RECORD-level detail in the first segment (offsets 0..N)
$KAFKA/kafka-dump-log.sh \
  --files data/broker-101/anatomy-demo-0/00000000000000000000.log \
  --print-data-log | head -30
```

Expected output shape:
```
Dumping data/broker-101/anatomy-demo-0/00000000000000000000.log
Starting offset: 0
baseOffset: 0 lastOffset: 6 count: 7 baseSequence: 0 lastSequence: 6 producerId: 1000 producerEpoch: 0 partitionLeaderEpoch: 0 isTransactional: false isControl: false position: 0 CreateTime: 1721001234567 size: 1123 magic: 2 compresscodec: none crc: 2394872193 isvalid: true
| offset: 0 CreateTime: 1721001234567 keySize: -1 valueSize: 1000 sequence: 0 headerKeys: [] payload: <binary...>
| offset: 1 CreateTime: 1721001234789 keySize: -1 valueSize: 1000 sequence: 1 headerKeys: [] payload: <binary...>
...
```

Each `baseOffset ... lastOffset` line is a **record batch** — Kafka groups records into batches on the wire and on disk. Compression, idempotence, transactions all apply per-batch. `position:` is the byte offset within the .log file.

## Observe — Step 4: peek at the index files

```bash
# The .index file maps offset → byte position
$KAFKA/kafka-dump-log.sh \
  --files data/broker-101/anatomy-demo-0/00000000000000000000.index | head
```

Expected:
```
Dumping ... .index
offset: 42 position: 4823
offset: 87 position: 9645
offset: 131 position: 14467
...
```

- **Sparse.** Not every offset is indexed — only every ~4 KB of log data (see `index.interval.bytes`).
- When a consumer requests offset 100, the broker binary-searches this index to find the *nearest indexed offset ≤ 100* (offset 87 in the example), seeks to position 9645 in the .log, then scans forward until it finds offset 100.

Timestamp index:
```bash
$KAFKA/kafka-dump-log.sh \
  --files data/broker-101/anatomy-demo-0/00000000000000000000.timeindex | head
```

```
timestamp: 1721001234567 offset: 42
timestamp: 1721001235012 offset: 87
...
```

- Same sparse pattern, keyed by CreateTime. Used by `kafka-console-consumer --offset-time` and `consumer.offsetsForTimes()`.

## Observe — Step 5: leader-epoch history and partition metadata

```bash
# Compact history of leadership epochs (used during replica truncation on ISR rejoin)
cat data/broker-101/anatomy-demo-0/leader-epoch-checkpoint

# Topic id + version (topic UUID lives here, matches the value in --describe)
cat data/broker-101/anatomy-demo-0/partition.metadata

# Compare with what --describe says
$KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic anatomy-demo | head -1
```

Expected: the `TopicId:` from `--describe` matches the id inside `partition.metadata`. This is the topic's **stable identity** — surviving a delete-and-recreate would produce a *different* topic id, which is how brokers detect "you deleted the topic and now the data doesn't belong here".

## Observe — Step 6: verify that a specific offset is where the index says it is

```bash
# Ask GetOffsetShell for the high-water mark
$KAFKA/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server $BS --topic anatomy-demo

# Now use kafka-console-consumer with --offset to jump to a specific offset
$KAFKA/kafka-console-consumer.sh --bootstrap-server $BS \
  --topic anatomy-demo --partition 0 --offset 100 --max-messages 3 \
  --property print.offset=true 2>&1 | head -6
```

Expected: three records printed starting at offset 100. The broker walked its `.index` for partition 0 to find offset 100 quickly.

## Solution

- **Never touch log files by hand while the broker is running.** Kafka holds file handles and index positions in memory. Deleting a `.log` file corrupts the partition; the broker will refuse to start with `CorruptRecordException` on next restart. If you MUST reclaim space urgently, stop the broker first, then `rm` (and be prepared for that broker to require a full replica catch-up from the ISR).

- **Use the right tool per question:**

  | Question | Tool |
  |----------|------|
  | Total disk used per topic | `du -sh data/broker-N/topic-*/` |
  | Which segment holds offset X? | Filename with base offset ≤ X, next base offset > X |
  | What's the record at offset X? | `kafka-dump-log.sh --files ... --print-data-log` |
  | High-water-mark per partition | `kafka-run-class.sh kafka.tools.GetOffsetShell` |
  | Timestamp for offset X | `kafka-dump-log.sh` (CreateTime shown per record) |
  | Offset at timestamp T | `.timeindex` — walk with `kafka-dump-log.sh` |
  | Topic ID / version | `partition.metadata` file, or `--describe` |
  | Compaction dirty ratio | JMX `kafka.log:type=LogCleanerManager,name=max-dirty-percent` |
  | Segment rolls history | Broker log: `grep 'Rolled new log segment' logs/broker-*.log` |

- **Segment sizing rules of thumb:**
  - Default `segment.bytes=1 GB` is fine for high-volume topics.
  - Small (`segment.bytes=64 MB`) helps compaction and retention respond faster but multiplies file count.
  - Never below 16 MB in production — too much overhead in indexes and rolling.
  - For test/demo topics, `segment.bytes=524288` (512 KB) is fine and makes rolls visible.

- **Index sizing** (`segment.index.bytes`, default 10 MB):
  - Rarely tuned. The index is pre-allocated but sparse; if a segment is smaller than the index can address, the index is truncated on roll.
  - Reducing it saves memory-mapped-file overhead in high-partition-count clusters.

- **Investigating disk fill**:
  ```bash
  # Which topics eat the most disk on this broker?
  du -sh data/broker-101/*/ | sort -h | tail -10

  # Which partitions in the biggest topic?
  du -sh data/broker-101/<big-topic>-*/ | sort -h
  ```

- **`__consumer_offsets` and `__transaction_state`** have the same on-disk shape but are compacted (`cleanup.policy=compact`). Their partition dirs also contain `.log` / `.index` / `.timeindex`; the log cleaner rewrites segments to drop older keyed values.

## Verify

```bash
# 1. Topic exists and has files on all 3 brokers
for id in 101 102 103; do
  echo "broker $id: $(ls data/broker-$id/anatomy-demo-*/*.log 2>/dev/null | wc -l) .log files"
done

# 2. Segments rolled (should be > 1 per partition)
ls data/broker-101/anatomy-demo-0/*.log | wc -l

# 3. dump-log reads without corruption
$KAFKA/kafka-dump-log.sh \
  --files data/broker-101/anatomy-demo-0/00000000000000000000.log \
  --print-data-log 2>&1 | grep -c 'isvalid: true'

# 4. Partition metadata topic id matches --describe
grep -o 'topic_id.*' data/broker-101/anatomy-demo-0/partition.metadata
$KAFKA/kafka-topics.sh --bootstrap-server $BS --describe --topic anatomy-demo | grep TopicId
```

## Takeaway

> **A topic on disk is directory-of-partition-dirs, each with numbered segment triplets (log/index/timeindex). Rolling is triggered by size or time; retention deletes whole rolled segments, never the active one; every "look inside" tool goes through `kafka-dump-log.sh`.**

## Instructor notes
- Show Terminal 2's live watch during Step 1 — the moment a new `.log` file appears (when a segment rolls) is the visual proof of segment-based storage.
- Read one `kafka-dump-log.sh` record batch line aloud — the "baseOffset / lastOffset / count / position" attributes are the internal vocabulary trainees will meet again in every future storage discussion.
- The 10 MB pre-allocated `.index` file often confuses students ("why does an empty index take 10 MB?"). Show `ls -lS` vs `du -sh` — `ls` shows apparent size, `du` shows actual disk usage. Kafka uses sparse memory-mapped files.
- Real-world story: an on-call engineer once tried to free disk space by `rm 00000000000000000000.log` on a live broker. The broker kept running (file handle still open) until restart, then refused to start with `NoSuchFileException`. Replica catch-up took 4 hours. Point at the "Never touch log files" rule.
- Bridge to 12.2: *"Now you know what's on disk. Next: how many DIFFERENT topics are on disk with DIFFERENT configs — and how those configs get resolved when broker default meets topic override meets producer setting."*

## Teardown

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic anatomy-demo
```
