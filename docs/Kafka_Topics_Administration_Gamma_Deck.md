# Kafka Topics Administration
## Deep-Dive Training for Kafka Administrators

**From logical data streams to production-safe topic operations**

Audience: Kafka Administrators, Platform Engineers, DevOps/SRE Teams  
Running example: `orders.events`  
Format: Concept → Internals → Commands → Operations → Troubleshooting

---

# Learning Objectives

By the end of this module, administrators will be able to:

- Explain topics, partitions, offsets, replicas, leaders and ISR
- Design topic partitioning and replication for real workloads
- Create, inspect, modify and delete topics safely
- Configure retention, compaction, durability and segment behavior
- Expand partitions and change replica assignments
- Diagnose unavailable, under-replicated and skewed partitions
- Apply topic-level security, naming and governance standards
- Build a production topic lifecycle runbook

---

# Why Topics Matter to Administrators

A Kafka topic is not merely a named message queue.

It determines:

- How data is distributed across brokers
- How much parallelism producers and consumers can achieve
- How failures affect availability and durability
- How much disk space the workload consumes
- How long data remains available
- Whether old values are deleted or compacted
- How safely a cluster can scale and recover

**A poor topic design becomes a long-term operational problem.**

---

# Running Scenario: Order Event Platform

An e-commerce platform publishes order lifecycle events:

- `OrderCreated`
- `PaymentConfirmed`
- `OrderPacked`
- `OrderShipped`
- `OrderDelivered`
- `OrderCancelled`

Proposed topic: `orders.events`

Administrative requirements:

- Preserve event order for each order ID
- Survive a broker failure
- Retain events for seven days
- Support multiple independent consumer applications
- Handle future traffic growth

---

# Topic: The Logical View

A topic is a named, append-only stream of records.

Each record commonly contains:

- Key
- Value
- Timestamp
- Headers
- Topic name
- Partition number
- Offset

Examples:

- `orders.events`
- `payments.events`
- `inventory.changes`
- `customer.profile.compacted`

Applications use the topic name; Kafka stores the data inside partitions.

---

# Topic vs Traditional Queue

| Characteristic | Traditional Queue | Kafka Topic |
|---|---|---|
| Message removal | Often removed after consumption | Retained by policy |
| Consumer position | Managed mainly by broker | Tracked as offsets |
| Replay | Limited or special | Native capability |
| Multiple applications | Often competing consumers | Independent consumer groups |
| Ordering | Queue dependent | Guaranteed within a partition |
| Storage model | Message queue | Distributed append-only log |

Kafka separates **data retention** from **consumer progress**.

---

# A Topic Is Divided into Partitions

`orders.events` with three partitions:

- Partition 0: ordered log with offsets 0, 1, 2, 3...
- Partition 1: separate ordered log with offsets 0, 1, 2, 3...
- Partition 2: separate ordered log with offsets 0, 1, 2, 3...

Partitions provide:

- Horizontal scalability
- Parallel reads and writes
- Distribution across brokers
- Independent leadership and replication

**The partition—not the entire topic—is Kafka’s unit of ordering and parallelism.**

---

# Understanding Offsets

An offset is the position of a record inside one partition.

Important facts:

- Offsets begin at 0 and increase monotonically
- Offset values are unique only within a partition
- Consumers use offsets to track progress
- An offset is assigned after a record reaches the partition log
- Deleted records can create unavailable historical ranges
- Offsets are not reset when old segments are removed

`orders.events-2@1057` means topic `orders.events`, partition 2, offset 1057.

---

# Ordering Guarantees

Kafka guarantees record order **within a single partition**.

Kafka does not provide one automatic global order across all topic partitions.

For the order platform:

- Use `orderId` as the record key
- Records with the same key normally map to the same partition
- Events for order `ORD-9001` remain ordered within that partition

If the topic partition count changes, the key-to-partition mapping may change for future records.

---

# How a Producer Selects a Partition

Typical decision flow:

1. Explicit partition provided → use it
2. Key provided → partitioner calculates a partition
3. No key → client partitioner distributes records for throughput

Common keyed model:

`partition = hash(key) % partitionCount`

Administrative implication:

- Partition count is part of application behavior
- Increasing partitions can change future key placement
- Administrators must coordinate partition changes with application owners

---

# The Hot Partition Problem

A topic can look healthy overall while one partition is overloaded.

Common causes:

- Low-cardinality keys
- One extremely active customer or tenant
- Constant key used by mistake
- Time-based keys that concentrate current traffic
- Custom partitioner defect

Symptoms:

- Uneven bytes-in across brokers
- Higher latency for one partition
- Consumer lag concentrated on one partition
- One broker reaches disk or network limits first

Adding partitions alone may not fix poor key distribution.

---

# Choosing the Number of Partitions

Partition count influences:

- Producer throughput
- Maximum consumer-group parallelism
- Broker metadata and file-handle load
- Leader election and recovery work
- Reassignment duration
- End-to-end ordering boundaries

Estimate from both directions:

- Producer requirement: required throughput ÷ sustainable throughput per partition
- Consumer requirement: required throughput ÷ sustainable consumption per consumer

Choose the larger estimate, then validate with load testing.

---

# Partition Sizing Example

Requirement:

- Peak incoming traffic: 120 MB/s
- Tested sustainable write rate: 15 MB/s per partition
- Consumer application needs 12 parallel workers

Calculations:

- Producer need: 120 ÷ 15 = 8 partitions
- Consumer need: 12 partitions for 12 active workers

Initial choice: **12 partitions**

Then validate:

- Key distribution
- Broker count and replica placement
- Growth headroom
- Recovery and reassignment time

---

# Deep Dive: Partition Planning Is a Capacity Decision

There is no universal “correct number” of partitions.

The decision must balance five dimensions:

1. **Throughput** — Can partitions carry peak writes and reads?
2. **Parallelism** — How many consumers must work simultaneously?
3. **Ordering** — Which records must remain together?
4. **Operations** — Can the cluster manage, recover and rebalance them?
5. **Growth** — Will the design survive future demand?

**Visual:** Five connected circles around “Partition Count.”

---

# Start with Measured Inputs

Collect evidence before calculating:

| Input | Example |
|---|---:|
| Peak records per second | 60,000 |
| Average serialized record size | 2 KB |
| Peak ingress | 120 MB/s |
| Required consumer groups | 4 |
| Slowest group processing capacity | 10 MB/s per instance |
| Ordering key | `orderId` |
| Expected annual growth | 40% |
| Replication factor | 3 |

Use **peak**, not daily average, and measure serialized/compressed traffic where possible.

---

# The Core Partition Formula

Calculate producer and consumer requirements separately:

### Write-side requirement

`Pwrite = Peak topic ingress ÷ Tested write capacity per partition`

### Read-side requirement

For each consumer group:

`Pread = Required group throughput ÷ Tested processing capacity per consumer instance`

### Initial result

`Partitions = max(Pwrite, highest Pread, required concurrency)`

Then adjust for ordering, skew, growth, brokers and operational limits.

---

# Worked Example: Write-Side Calculation

For `orders.events`:

- Peak incoming traffic: 120 MB/s
- Tested safe partition write rate: 15 MB/s

`Pwrite = 120 ÷ 15 = 8 partitions`

Do not use the highest rate seen in a short test.

Use a sustainable value that meets:

- Target produce latency
- Replication requirements
- Acceptable CPU and disk use
- No growing request queue
- No ISR instability

---

# Worked Example: Read-Side Calculation

Fraud-detection group must process 120 MB/s.

One fraud consumer instance safely processes 10 MB/s.

`Pread = 120 ÷ 10 = 12 partitions`

Results:

- Write side requires 8
- Read side requires 12
- Initial partition count becomes **12**

Why? Within one consumer group, a partition can be actively assigned to only one consumer at a time.

---

# Consumer Parallelism Rule

For one consumer group:

`Maximum useful active consumers ≤ Number of partitions`

Example: 12 partitions

| Consumers in group | Result |
|---:|---|
| 4 | About 3 partitions per consumer |
| 12 | About 1 partition per consumer |
| 16 | 12 active; approximately 4 idle |

More consumer groups do not share this limit with one another. Each group independently reads all required partitions.

---

# Multiple Consumer Groups Change Read Load

Suppose `orders.events` has four consumer groups:

- Fraud detection
- Inventory reservation
- Customer notifications
- Analytics ingestion

Each group reads the topic independently.

Approximate aggregate read traffic:

`Topic ingress × number of full-volume groups`

At 120 MB/s and four full-volume groups:

`120 × 4 = 480 MB/s` of logical consumer reads before protocol and replication overhead.

Partition count must fit broker network and disk-read capacity—not only producer throughput.

---

# Record Rate vs Byte Rate

Evaluate both dimensions.

Two workloads can each produce 100 MB/s:

- Workload A: 100,000 records/s × 1 KB
- Workload B: 1,000 records/s × 100 KB

They stress Kafka differently:

- High record rate increases request, serialization and per-record overhead
- Large records increase memory, network burst and fetch-size pressure
- Compression and batching can significantly change measured capacity

Benchmark with realistic keys, headers, sizes and batch behavior.

---

# Compression Changes the Calculation

Assume raw traffic is 200 MB/s and compression ratio is 2.5:1.

Approximate stored/transmitted compressed rate:

`200 ÷ 2.5 = 80 MB/s`

But do not size only from this ideal value.

Compression effectiveness depends on:

- Record similarity
- Batch size
- Compression codec
- Producer linger and batching
- Already-compressed payloads
- CPU availability

Measure actual producer and broker byte metrics during load tests.

---

# Replication Multiplies Internal Traffic

With replication factor 3, each leader write must also reach two followers.

Simplified cluster write traffic:

`Ingress × replication factor`

For 120 MB/s at RF 3:

`120 × 3 ≈ 360 MB/s` of total leader-plus-replica data movement

This is a planning approximation, not exact wire usage.

Account for protocol overhead, acknowledgements, compression, batching and cross-rack traffic.

---

# Broker Distribution Check

After calculating partitions, test how they distribute.

Example:

- 12 partitions
- Replication factor 3
- 6 brokers
- Total replicas = `12 × 3 = 36`
- Average replicas per broker = `36 ÷ 6 = 6`
- Average leaders per broker = `12 ÷ 6 = 2`

This arithmetic checks count balance—but administrators must also check byte size, traffic, rack placement and other topics already on each broker.

---

# Partition Count and Storage Sizing

Approximate retained primary data:

`Ingress per second × retention seconds`

Approximate replicated data:

`Primary retained data × replication factor`

Example:

- Average ingress: 20 MB/s
- Retention: 7 days = 604,800 seconds
- RF: 3

Primary data ≈ 12.1 TB  
Replicated data ≈ 36.3 TB before overhead and safety margin

Partition count distributes this data; it does not reduce total retained bytes.

---

# Average Partition Size Matters

Approximate partition size:

`Primary retained topic data ÷ partition count`

If the retained primary data is 12 TB:

| Partitions | Average per partition |
|---:|---:|
| 6 | 2 TB |
| 12 | 1 TB |
| 24 | 500 GB |

Very large partitions take longer to copy during reassignment and recovery.

Very small partitions create more metadata, files and operational overhead.

---

# Recovery-Time Constraint

Partition sizing must support the recovery objective.

Simplified replica catch-up estimate:

`Recovery time = Partition data to copy ÷ Safe transfer rate`

Example:

- Partition replica size: 500 GB
- Safe recovery transfer: 50 MB/s

Estimated time:

`500 GB ÷ 50 MB/s ≈ 2.8 hours`

Real recovery may be slower because production traffic, disk contention and multiple replicas share resources.

---

# Growth Headroom

Do not size only for today.

If current calculation gives 12 partitions and expected growth is 40%:

`12 × 1.4 = 16.8`

A practical initial choice could be **18 partitions**, after validating broker impact.

Headroom should reflect:

- Forecast traffic growth
- Seasonal peaks
- Product launches
- Consumer growth
- Operational uncertainty

Avoid extreme over-partitioning “just in case.”

---

# Why You Cannot Simply Double Later

Increasing partitions is technically easy, but semantically important.

With keyed partitioning:

`hash(key) % oldPartitionCount`

becomes:

`hash(key) % newPartitionCount`

The same key may map to a different partition for future records.

Consequences:

- Historical and new records for one key may exist in different partitions
- Cross-expansion per-key processing order can be affected
- Stateful consumers may require special handling

Plan adequate headroom when stable key placement is essential.

---

# Key Cardinality Sets a Practical Limit

Partitions cannot create useful distribution when keys have low cardinality.

Example:

- Topic has 24 partitions
- Producer uses `country` as key
- Only 5 country values produce most traffic

Only a few partitions may receive nearly all records.

Better options may include:

- Higher-cardinality business key
- Composite key such as `country + customerId`
- Intentional bucketing such as `tenantId + bucket`

Never change key strategy without reviewing ordering requirements.

---

# Skew Safety Factor

Perfectly even distribution is rare.

If the busiest partition receives 1.8 times the average traffic, size using the busiest partition—not the average.

Example:

- Average planned load: 10 MB/s per partition
- Observed skew factor: 1.8
- Hottest partition: approximately 18 MB/s
- Tested safe capacity: 15 MB/s

The design is unsafe even if total topic throughput looks acceptable.

Measure per-partition rates during realistic peak traffic.

---

# A Practical Benchmarking Procedure

1. Reproduce production broker, disk, network and replication settings
2. Use realistic record size, keys, headers and compression
3. Test expected producer acknowledgement mode
4. Run all major consumer groups or equivalent read load
5. Increase traffic gradually to peak and burst levels
6. Hold the load long enough to expose disk and cache effects
7. Introduce a broker failure or maintenance event
8. Record sustainable per-partition rate at the required latency SLO

Use the safe result—not the failure point—in sizing formulas.

---

# Benchmark Metrics to Capture

At every load level, record:

- Produce throughput and request latency
- Consumer throughput and fetch latency
- Broker CPU, disk utilization and network utilization
- Request queue time
- ISR shrink/expand events
- Replica lag
- Under-replicated partitions
- JVM pause behavior
- Page-cache and disk-read effects
- Consumer lag per partition

A throughput number without latency and replication health is incomplete.

---

# Small, Medium and High-Volume Examples

| Workload | Peak ingress | Consumers needed | Possible starting point* |
|---|---:|---:|---:|
| Small audit stream | 2 MB/s | 3 | 3–6 partitions |
| Order events | 120 MB/s | 12 | 12–18 partitions |
| Telemetry stream | 600 MB/s | 40 | 40–60 partitions |

\*Examples only—not universal recommendations.

Final values require workload benchmarks, key analysis, growth planning, broker capacity and recovery checks.

---

# When to Add Partitions

Adding partitions may be appropriate when:

- Consumer concurrency is genuinely limited by partition count
- Per-partition throughput is near tested safe capacity
- Key distribution is healthy
- Brokers have capacity for additional partition overhead
- Ordering impact is understood and accepted
- The change has been tested with clients

Adding partitions is not a substitute for fixing slow consumers, bad keys, overloaded brokers or insufficient disks.

---

# When to Create a New Topic Instead

Consider a separate topic when workloads differ in:

- Retention policy
- Security classification
- Ownership
- Schema lifecycle
- Ordering requirements
- Traffic profile
- Availability or durability target
- Consumer population

Example:

- `orders.events` — seven-day immutable history
- `orders.state` — compacted latest state

One oversized multipurpose topic creates operational coupling.

---

# Partition Decision Flow

1. Measure peak byte rate and record rate
2. Benchmark safe write capacity per partition
3. Calculate write-side partitions
4. Benchmark the slowest high-volume consumer
5. Calculate read-side partitions
6. Select the larger result
7. Validate key cardinality and skew
8. Add reasonable growth headroom
9. Check brokers, storage and recovery time
10. Review ordering impact and approve

**Visual:** Present as a vertical decision path with green validation gates.

---

# Partition Planning Worksheet

| Decision field | Value |
|---|---|
| Peak records/s | |
| Average and P99 record size | |
| Peak compressed MB/s | |
| Tested safe write MB/s/partition | |
| Write-side partition requirement | |
| Slowest consumer MB/s/instance | |
| Read-side partition requirement | |
| Key cardinality/skew factor | |
| Growth headroom | |
| Proposed partitions | |
| Average retained partition size | |
| Estimated replica recovery time | |

Store this worksheet with the topic creation request.

---

# Partition Count: Final Review Questions

Before approval, can the team answer:

- What calculation produced this number?
- Which benchmark supports per-partition capacity?
- How many consumers must run concurrently?
- What ordering guarantee does the application require?
- Is the key distribution sufficiently uniform?
- What happens during the next two years of growth?
- How large will each retained partition become?
- Can a failed replica recover within the target time?
- Can the cluster carry the total replica and consumer traffic?

If these answers are unknown, the partition count is a guess.

---

# More Partitions Are Not Free

Excessive partitions can increase:

- Controller metadata workload
- Open files and log segment count
- Memory usage
- Leader-election work
- Replication connections and fetch activity
- Operational complexity
- Recovery time after broker failure

Avoid a universal rule such as “always create 50 partitions.”

Partition count must follow workload evidence and cluster capacity.

---

# Replication Factor

Replication factor is the number of copies of each partition.

Example: 3 partitions with replication factor 3

- 3 logical partitions
- 9 partition replicas in total
- Each partition has one leader and two followers

Replication factor 3 is a common production choice because data can remain available after losing one broker, assuming healthy replica placement and ISR.

Replication factor cannot exceed the number of eligible brokers at creation time.

---

# Leader and Follower Replicas

For each partition:

- One replica is the leader
- Producers write to the leader
- Consumers normally fetch from the leader, unless configured for eligible follower fetching
- Followers copy records from the leader
- The controller manages leadership and replica assignments

Example:

`orders.events-0  Leader: 2  Replicas: 2,3,1  ISR: 2,3,1`

The first replica in the assignment is the preferred replica.

---

# In-Sync Replicas: ISR

ISR is the set of replicas considered sufficiently caught up with the leader.

Healthy state:

- Replicas: `1,2,3`
- ISR: `1,2,3`

Degraded state:

- Replicas: `1,2,3`
- ISR: `1,2`

Possible reasons for ISR shrink:

- Broker failure
- Network latency or packet loss
- Slow or failed disk
- Broker overload
- Long garbage-collection pauses
- Replication throttling or resource contention

---

# Replicas, ISR and Offline Replicas

Do not confuse these fields:

| Field | Meaning |
|---|---|
| Replicas | Assigned broker copies for the partition |
| ISR | Assigned replicas currently considered in sync |
| Leader | Replica accepting reads/writes |
| Offline replicas | Assigned replicas currently unavailable |

An ISR smaller than the replica list indicates reduced redundancy.

An unavailable leader makes the partition unavailable.

---

# Durability Triangle

Strong write durability commonly combines:

- Topic replication factor = 3
- Topic `min.insync.replicas = 2`
- Producer `acks = all`

Effect:

- The write succeeds only when the ISR condition is satisfied
- If ISR falls below 2, new writes fail instead of silently weakening durability
- Availability is intentionally traded for protection against acknowledged-data loss

`min.insync.replicas` matters most when producers use `acks=all`.

---

# What Happens When a Broker Fails?

Assume partition 0 has:

- Leader: Broker 1
- Followers: Brokers 2 and 3
- ISR: `1,2,3`

If Broker 1 fails:

1. Controller detects the failure
2. An eligible in-sync follower is elected
3. Clients refresh metadata
4. Reads and writes continue through the new leader
5. Failed replica remains outside ISR until it returns and catches up

Recovery quality depends on replication health before the failure.

---

# Unclean Leader Election

If no in-sync replica is available, Kafka faces a trade-off:

- Keep the partition unavailable
- Elect an out-of-sync replica and risk data loss

`unclean.leader.election.enable=true` favors availability with possible loss of acknowledged records.

`false` favors consistency and durability by waiting for an eligible replica.

Administrator decision:

- Define policy by workload criticality
- Do not enable it casually at topic or broker level
- Document recovery expectations with application owners

---

# Rack-Aware Replica Placement

Set a distinct `broker.rack` value for each failure domain.

Examples:

- Availability zone
- Data-center rack
- Cloud failure domain

With replication factor 3 across three racks, Kafka attempts to spread replicas across racks.

Benefits:

- Better tolerance of rack/AZ failure
- Reduced correlated replica loss

Validate that rack labels reflect real infrastructure topology.

---

# Topic Naming Standards

Use a predictable naming convention, for example:

`<domain>.<entity>.<event-type>.<version>`

Examples:

- `commerce.orders.events.v1`
- `payments.transactions.events.v1`
- `inventory.stock.compacted.v1`

Define standards for:

- Allowed characters and case
- Environment separation
- Ownership metadata
- Data classification
- Versioning
- Dead-letter and retry topics
- Internal topics and reserved prefixes

---

# Topic Creation: Production Decisions First

Before creating a topic, confirm:

- Business owner and technical owner
- Expected throughput and record size
- Partition key and ordering scope
- Partition count
- Replication factor
- Retention or compaction requirement
- Durability requirement
- Maximum record size
- Security and ACL requirements
- Monitoring, cost and deletion policy

Treat topic creation as a controlled infrastructure change.

---

# Creating a Topic

```bash
bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --create \
  --topic orders.events \
  --partitions 12 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000
```

Important:

- Use explicit production settings
- Capture the command in version-controlled automation
- Verify the resulting assignment and effective configuration

---

# Listing and Describing Topics

List topics:

```bash
bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --list
```

Describe one topic:

```bash
bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --describe \
  --topic orders.events
```

Inspect:

- Partition count
- Replication factor
- Topic overrides
- Leader distribution
- Replica assignment
- ISR state

---

# Reading `--describe` Output

Example:

```text
Topic: orders.events  Partition: 0  Leader: 2
Replicas: 2,3,1  Isr: 2,3,1
```

Interpretation:

- Partition 0 leader is Broker 2
- Copies reside on Brokers 2, 3 and 1
- All three replicas are in sync

Warning patterns:

- `Leader: -1` → no active leader
- ISR smaller than replicas → under-replicated
- Same broker leading too many partitions → leadership skew

---

# Filtering for Problem Partitions

Useful checks:

```bash
bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --describe \
  --under-replicated-partitions
```

```bash
bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --describe \
  --unavailable-partitions
```

```bash
bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --describe \
  --under-min-isr-partitions
```

Use monitoring for continuous detection; CLI checks are valuable for investigation.

---

# Topic Configuration Precedence

Kafka has broker defaults and optional topic-level overrides.

Effective topic setting:

1. Topic override, when configured
2. Otherwise the related broker default

Examples:

- Broker: `log.retention.hours`
- Topic: `retention.ms`
- Broker: `log.segment.bytes`
- Topic: `segment.bytes`

Do not assume every topic uses the broker default. Inspect dynamic overrides explicitly.

---

# Inspecting Effective Topic Overrides

```bash
bin/kafka-configs.sh \
  --bootstrap-server kafka-1:9092 \
  --entity-type topics \
  --entity-name orders.events \
  --describe
```

Use `--describe --all` where supported to display effective values and their sources.

Administrator questions:

- Which settings are explicitly overridden?
- Are overrides still required?
- Do similar topics have inconsistent policies?
- Did an emergency change become permanent configuration drift?

---

# Modifying Topic Configuration

Add or change settings:

```bash
bin/kafka-configs.sh \
  --bootstrap-server kafka-1:9092 \
  --entity-type topics \
  --entity-name orders.events \
  --alter \
  --add-config retention.ms=1209600000,min.insync.replicas=2
```

Remove an override and return to the broker default:

```bash
bin/kafka-configs.sh \
  --bootstrap-server kafka-1:9092 \
  --entity-type topics \
  --entity-name orders.events \
  --alter \
  --delete-config retention.ms
```

Always record the previous value and rollback command.

---

# Cleanup Policies

Kafka supports two principal topic cleanup policies:

- `delete`: remove old log segments according to time or size policy
- `compact`: retain the latest value for each key over time

Policies can be combined:

`cleanup.policy=compact,delete`

Choice depends on data semantics:

- Event history → usually delete-based retention
- Current state/changelog → usually compaction
- Bounded changelog → compaction plus delete retention

---

# Time-Based Retention

Key setting:

`retention.ms`

Example: seven days

`retention.ms=604800000`

Important:

- Retention is enforced at log-segment level
- Records are not deleted exactly at their individual expiry time
- Segment rolling and periodic retention checks affect removal timing
- Consumers can replay only data still retained
- Longer retention directly affects disk capacity and recovery time

Retention is a storage policy—not a guarantee that consumers have processed the data.

---

# Size-Based Retention

Key setting:

`retention.bytes`

It limits retained log size **per partition**, not for the entire topic.

Approximate topic data limit:

`retention.bytes × partition count × replication factor`

Actual disk usage also includes:

- Active segments
- Index files
- Replica overhead
- Compaction and deletion timing
- Other topics and internal logs

Use size retention carefully because high-volume periods can shorten the effective time window.

---

# Log Segments

Each partition log is split into segment files.

Relevant settings:

- `segment.bytes`
- `segment.ms`
- `segment.jitter.ms`
- `segment.index.bytes`

Why segments matter:

- Retention deletes whole inactive segments
- Compaction works on eligible segments
- Smaller segments can make cleanup more responsive
- Too many small segments increase files, indexes and operational overhead

The active segment is normally not deleted by retention.

---

# Log Compaction

Compaction retains the latest known value for each key.

Example records:

```text
C101 -> Silver
C102 -> Gold
C101 -> Gold
```

After compaction, the latest value for `C101` is `Gold`.

Key points:

- Keys are essential
- Compaction runs asynchronously
- Duplicate/older values can remain until cleaning occurs
- Record order and offsets are preserved for retained records
- Compaction does not mean “keep exactly one record immediately”

---

# Tombstones in Compacted Topics

A record with a key and `null` value is a tombstone.

Example:

`C101 -> null`

Meaning: delete the logical key from the compacted state.

`delete.retention.ms` controls how long tombstones remain available for downstream consumers before they may be removed.

Operational risks:

- Consumers offline longer than the tombstone window may rebuild incorrect state
- Missing keys prevent meaningful compaction
- Application teams must distinguish a null value from an absent event

---

# Compaction Tuning Controls

Important topic settings include:

- `min.cleanable.dirty.ratio`
- `min.compaction.lag.ms`
- `max.compaction.lag.ms`
- `delete.retention.ms`
- `segment.ms`

Trade-offs:

- Aggressive cleaning reduces duplicate history but increases disk I/O
- Delayed cleaning can improve efficiency but uses more disk
- Very large segments can delay cleaning eligibility

Observe cleaner backlog and disk throughput before changing compaction settings.

---

# Message Size Controls

Topic-level `max.message.bytes` limits the largest record batch accepted by the topic.

Related settings must remain compatible:

- Producer `max.request.size`
- Broker request/message limits
- Consumer `max.partition.fetch.bytes`
- Replica fetch limits

If limits are inconsistent:

- Producers may receive record-too-large errors
- Consumers may struggle to fetch large batches
- Replication can be affected by incorrect broker-side sizing

Prefer object storage plus references for very large payloads.

---

# Compression and Topics

Kafka compression is normally selected by the producer.

Topic `compression.type` can:

- Preserve producer-selected compression using `producer`
- Enforce a configured codec

Compression can improve:

- Network throughput
- Disk usage
- Page-cache efficiency

But it consumes CPU and performance depends on batch quality.

Administrators should test record size, batching, codec and CPU impact together.

---

# Increasing Partition Count

```bash
bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --alter \
  --topic orders.events \
  --partitions 18
```

Important constraints:

- Kafka supports increasing—not reducing—the partition count
- Existing data is not redistributed automatically
- New partitions begin empty
- Future keyed records may map differently
- Consumer groups rebalance
- Global topic ordering remains impossible

Treat expansion as an application-visible change.

---

# Before Increasing Partitions

Check all of the following:

- Why is expansion required?
- Is the bottleneck really partition count?
- Does the producer use keyed partitioning?
- Does business logic assume stable key placement?
- Can consumers handle a rebalance?
- Is broker capacity sufficient for more leaders and replicas?
- Will monitoring and alerting discover the new partitions?
- Is rollback possible? Remember: partition count cannot be reduced.

Load imbalance caused by bad keys requires a keying strategy fix.

---

# Replica Reassignment

Replica reassignment is used to:

- Move replicas between brokers
- Rebalance after adding brokers
- Drain a broker before decommissioning
- Move replicas between log directories
- Change the replication factor

Operational phases:

1. Capture current assignment
2. Build and review proposed assignment
3. Execute with appropriate throttling
4. Monitor progress and cluster health
5. Verify completion
6. Remove obsolete throttles

---

# Reassignment Plan Example

```json
{
  "version": 1,
  "partitions": [
    {
      "topic": "orders.events",
      "partition": 0,
      "replicas": [2, 3, 4],
      "log_dirs": ["any", "any", "any"]
    }
  ]
}
```

The replica order matters:

- First broker is the preferred replica
- Every broker ID must be valid
- The same broker cannot appear twice for one partition
- Placement should respect racks, capacity and balance

---

# Execute and Verify Reassignment

Execute:

```bash
bin/kafka-reassign-partitions.sh \
  --bootstrap-server kafka-1:9092 \
  --reassignment-json-file reassignment.json \
  --execute \
  --throttle 50000000
```

Verify:

```bash
bin/kafka-reassign-partitions.sh \
  --bootstrap-server kafka-1:9092 \
  --reassignment-json-file reassignment.json \
  --verify
```

Save the original assignment for rollback before execution.

---

# Changing Replication Factor

Replication factor is changed through replica reassignment—not through a simple `kafka-topics --alter` flag.

To increase RF from 2 to 3:

- Add a third distinct broker to each partition’s replica list
- Execute the reassignment
- Wait for new replicas to copy data and join ISR
- Verify every partition has the intended assignment

Plan for:

- Network transfer
- Disk I/O
- Catch-up time
- Temporary cluster load
- Rack-aware placement

---

# Reassignment Throttling

Moving replicas can compete with production traffic for network and disk.

Use reassignment throttles to limit impact.

Example:

`--throttle 50000000`

Operational trade-off:

- Too high → client latency and broker saturation
- Too low → reassignment runs for a long time

Monitor:

- Produce/fetch latency
- Replication lag
- Network and disk utilization
- Under-replicated partitions
- Reassignment progress

Clean up throttles after completion.

---

# Preferred Leader Election

The first replica in the assignment is the preferred leader.

After failures and recovery, leadership may remain imbalanced.

Preferred leader election can restore leadership to preferred replicas when they are eligible.

Before initiating:

- Confirm preferred replicas are in ISR
- Inspect broker load
- Avoid peak traffic periods
- Monitor leader-election activity and latency

Leadership balance affects CPU, network and request load—not only metadata appearance.

---

# Topic Deletion

```bash
bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --delete \
  --topic orders.events
```

Deletion is destructive.

Production safeguards:

- Require owner approval
- Confirm exact cluster and topic name
- Verify dependencies and active consumer groups
- Record retention/compliance obligations
- Export required data or metadata
- Use least-privilege ACLs
- Prefer automation with review gates

Never use broad shell patterns in destructive topic workflows.

---

# Topic Data Is Not a Conventional Backup

Replication protects against broker failure; it is not a complete backup strategy.

Replication does not automatically protect against:

- Accidental topic deletion
- Incorrect retention changes
- Malicious deletion
- Application producing corrupt data
- Cluster-wide disaster
- Operator error replicated across all copies

Disaster-recovery options may include cross-cluster replication, immutable downstream storage and tested recovery runbooks.

---

# Internal Topics

Kafka components create internal topics, commonly including:

- Consumer offsets
- Transaction state
- Kafka Connect configuration, offsets and status
- Kafka Streams state/changelog and repartition topics

Administrators must:

- Protect internal topics from accidental deletion
- Use suitable replication factors
- Monitor their availability and ISR
- Understand ownership before modifying configuration
- Avoid applying generic retention policies blindly

Internal-topic failure can disrupt the platform even when business topics appear healthy.

---

# Topic-Level Security

Topic administration requires both authentication and authorization.

Typical privileges:

- Describe topic
- Read topic
- Write topic
- Create topic
- Alter topic/configuration
- Delete topic
- Cluster-level permissions for some administrative operations

Security principles:

- Separate application and administrator identities
- Grant access using naming patterns carefully
- Restrict delete and alter permissions
- Audit administrative changes
- Test ACLs before production rollout

---

# Preventing Uncontrolled Topic Creation

Automatic topic creation can produce topics with unintended defaults or typing errors.

Production approach:

- Consider disabling uncontrolled automatic creation
- Create topics through reviewed automation
- Require explicit partition, replication and retention settings
- Attach ownership and classification metadata outside Kafka
- Detect unapproved topics continuously

Example risk:

Application writes to `order.events` instead of `orders.events`, silently creating a poorly configured topic.

---

# Monitoring Topic Health

Core topic/partition signals:

- Offline partition count
- Under-replicated partition count
- Under-min-ISR partition count
- ISR shrink/expand rate
- Leader count by broker
- Partition count by broker
- Bytes in/out by topic and broker
- Produce and fetch request latency
- Log size and disk utilization
- Consumer lag by group/topic/partition

Alerts should indicate both technical severity and affected business topics.

---

# Healthy, Degraded and Unavailable

| State | Example | Meaning |
|---|---|---|
| Healthy | Replicas 1,2,3; ISR 1,2,3 | Full redundancy |
| Degraded | Replicas 1,2,3; ISR 1,2 | Service may work, redundancy reduced |
| Under min ISR | ISR smaller than configured minimum | `acks=all` writes may fail |
| Unavailable | Leader is absent | Partition cannot serve normally |

Do not wait for unavailability. Under-replication is an early operational warning.

---

# Troubleshooting Under-Replicated Partitions

Investigation sequence:

1. Identify affected topics, partitions and brokers
2. Check whether a broker is offline or restarting
3. Inspect disk capacity, errors and latency
4. Inspect network connectivity and saturation
5. Check CPU, memory and JVM pauses
6. Check replication lag and fetcher behavior
7. Review recent reassignments or configuration changes
8. Verify the replica rejoins ISR after recovery

Avoid forcing leader elections before understanding replica freshness.

---

# Troubleshooting an Offline Partition

Questions to answer:

- Which brokers host the assigned replicas?
- Are any assigned replicas alive?
- Were replicas in ISR before the incident?
- Is the controller healthy and processing metadata changes?
- Is there an ongoing reassignment?
- Are broker disks readable?
- Is unclean election disabled?
- Would forcing recovery create data loss?

Escalate recovery decisions when availability and durability objectives conflict.

---

# Troubleshooting Disk Growth

Check:

- Which topics and partitions consume the most space?
- Did throughput or average record size increase?
- Was `retention.ms` increased?
- Is `retention.bytes` unlimited?
- Are segments rolling?
- Is the log cleaner falling behind?
- Are deletion files waiting for removal?
- Are replicas or leaders unevenly placed?
- Did a reassignment temporarily duplicate data?

Do not delete Kafka log files manually while the broker is running.

---

# Troubleshooting Partition Skew

Partition skew can mean several different things:

- Record-count skew
- Byte-size skew
- Traffic skew
- Leader skew
- Replica-count skew
- Consumer-lag skew

Possible remedies:

- Fix producer key strategy
- Use a custom partitioner only with strong justification
- Reassign replicas for broker balance
- Elect preferred leaders for leadership balance
- Split a high-volume business entity logically
- Increase partitions after impact analysis

Diagnose the type of skew before choosing a remedy.

---

# Common Administrator Mistakes

- Creating topics with broker defaults without reviewing them
- Assuming replication factor alone guarantees durability
- Increasing partitions without reviewing key ordering
- Treating ISR shrink as harmless
- Retaining unlimited data without capacity planning
- Using compaction without meaningful keys
- Forgetting to remove reassignment throttles
- Deleting topics without checking active dependencies
- Changing several risk-sensitive settings at once
- Managing production topics only through ad hoc CLI commands

---

# Topic Design Review Checklist

Approve a new topic only after confirming:

- Clear name, purpose and owner
- Data classification and access policy
- Partition key and ordering requirement
- Peak throughput and record-size estimate
- Partition count with calculation
- Replication factor and rack placement
- `min.insync.replicas` and producer acknowledgement policy
- Cleanup, retention and compaction semantics
- Capacity and cost estimate
- Monitoring, alerting and lifecycle plan

---

# Production Change Runbook

For every topic change:

1. Define objective and success criteria
2. Capture current configuration and assignment
3. Assess application and capacity impact
4. Prepare exact commands and rollback path
5. Obtain peer/owner approval
6. Execute during the approved window
7. Monitor topic, brokers and clients
8. Verify final configuration and health
9. Update infrastructure code and documentation
10. Close only after post-change observation

---

# Hands-On Lab: Topic Lifecycle

Perform the following in a non-production cluster:

1. Create `orders.events` with 6 partitions and RF 3
2. Set seven-day retention and `min.insync.replicas=2`
3. List and describe the topic
4. Produce keyed order events
5. Observe key-to-partition mapping and offsets
6. Increase partitions from 6 to 9
7. Observe future key placement and consumer rebalance
8. Modify retention to 14 days
9. Remove the override and verify inherited defaults
10. Delete a separate disposable test topic safely

---

# Hands-On Lab: Failure and Recovery

1. Identify leaders and ISR for every partition
2. Stop a broker hosting multiple leaders
3. Observe leader movement and ISR shrink
4. Test producer behavior with `acks=all`
5. Compare ISR with `min.insync.replicas`
6. Restart the broker
7. Observe replica catch-up and ISR expansion
8. Check whether leadership remains balanced
9. Record the incident timeline and commands

Expected learning: replication health before failure determines recovery quality.

---

# Hands-On Lab: Retention and Compaction

Create two topics:

- `orders.history.lab` with `cleanup.policy=delete`
- `customer.state.lab` with `cleanup.policy=compact`

Exercises:

- Produce repeated keys and updated values
- Produce tombstones to the compacted topic
- Inspect topic configuration
- Examine partition log directories in the lab environment
- Observe segment rolling and asynchronous cleanup
- Explain why cleanup is not immediate
- Document when each policy should be used

---

# Administrator Scenario Review

Scenario:

- RF = 3
- `min.insync.replicas = 2`
- Producer uses `acks=all`
- Only one replica remains in ISR

What happens?

- The partition may still have a leader
- Reads of committed/visible data may continue
- New `acks=all` writes fail because ISR is below the minimum
- The correct response is to restore replica health—not weaken durability automatically

This is an intentional safety mechanism.

---

# Key Takeaways

- A topic is implemented as distributed partition logs
- Ordering exists within a partition, not across the entire topic
- Partition count controls scale but adds operational cost
- Replication, ISR, `min.insync.replicas` and producer acknowledgements work together
- Retention and compaction solve different data-lifecycle problems
- Partition count can increase but cannot be reduced
- Replication factor changes require reassignment
- Topic operations must be automated, monitored and governed
- Healthy topics are the foundation of a healthy Kafka platform

---

# Official References

- Apache Kafka Documentation: https://kafka.apache.org/documentation/
- Kafka 4.3 Basic Operations: https://kafka.apache.org/43/operations/basic-kafka-operations/
- Kafka Topic Configurations: https://kafka.apache.org/configuration/topic-configs/
- Kafka Design: https://kafka.apache.org/43/design/design/

Use the documentation matching the Kafka version deployed in your environment.

---

# Thank You
## Questions and Discussion

**Next recommended administration module:**

Kafka Broker Configuration, Storage and Cluster Operations
