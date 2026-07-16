# Workshop 05 — Monitoring, Capacity, and Incident Diagnosis

## Learning outcomes

- Build a repeatable health baseline.
- Correlate CLI evidence, logs, JVM state, disk, and client symptoms.
- Diagnose instead of blindly restarting.
- Calculate basic failure-aware capacity.

## Demo 1 — Baseline evidence pack

```bash
./lab.sh status
./lab.sh health
./lab.sh describe
./lab.sh groups
./lab.sh log-dirs
./lab.sh broker-configs
./lab.sh metadata-version
jps -lv
df -h
```

- Expected baseline:
  - all expected processes and endpoints;
  - no unavailable or under-replicated partitions;
  - full ISR;
  - stable/understood consumer lag;
  - adequate disk headroom.

## Metric-to-question map

- Offline partitions: “Can clients access every partition?”
- Under-replicated partitions: “Has redundancy degraded?”
- ISR shrink/expand: “Are replicas repeatedly falling behind?”
- Active controller count: “Is exactly one ZooKeeper-mode controller active?”
- Request latency/queue time: “Is broker service capacity saturated?”
- Network/request handler idle: “Do thread pools have headroom?”
- Consumer lag rate: “Can applications keep up?”
- Disk usage/latency: “Can logs and recovery traffic progress safely?”
- Heap/GC: “Is JVM pause/memory pressure affecting service?”
- ZooKeeper latency/outstanding requests: “Is metadata coordination healthy?”

## Demo 2 — Broker crash correlation

- Capture baseline time.
- Locate only the disposable broker PID:

```bash
jps -lv | grep broker2.properties
kill -9 PID
```

- Gather evidence:

```bash
./lab.sh health
./lab.sh describe orders
./lab.sh groups
```

- Expected correlation:
  - process disappears;
  - leaders move;
  - ISR shrinks;
  - under-replication appears;
  - clients continue while two ISR replicas remain.
- Restart Broker 2 and prove full ISR recovery.

## Demo 3 — Slow process, not dead process

- Lab-only fault injection:

```bash
jps -lv | grep broker2.properties
kill -STOP PID
# inspect health and client behavior
kill -CONT PID
```

- Explain: a listening process can still be unhealthy; process checks alone are insufficient.

## Capacity exercise

- Storage approximation:
  - daily ingress × retention days × replication factor × safety factor.
- Add headroom for:
  - segment/compaction overhead;
  - index files;
  - rebalance/reassignment copies;
  - burst traffic;
  - one-broker failure.
- Check separately:
  - disk capacity and throughput;
  - network ingress, egress, and replication;
  - partition count/controller overhead;
  - file descriptors;
  - page cache and JVM heap.

## Senior incident workflow

- Declare impact and affected scope.
- Preserve timestamps, logs, metrics, and recent-change evidence.
- Check quorum/controller, offline partitions, ISR, and disks.
- Check clients, lag, latency, quotas, and dependencies.
- Restore redundancy using the least risky action.
- Verify baseline, monitor recurrence, and record root cause/follow-up.

## Participant challenge

- Receive the symptom “orders consumer is 50,000 behind.”
- Produce five hypotheses covering broker, consumer, downstream, quota, and traffic changes.
- Name one command/metric that confirms or rejects each hypothesis.

## Must-know points

- Green processes do not prove a healthy Kafka service.
- Under-replication is a symptom; find why the replica fell behind.
- Consumer lag can be an application/downstream issue with healthy brokers.
- Kafka relies heavily on OS page cache; oversized heap can reduce performance.
- Alert on actionable symptoms with duration, severity, and an owner.

## Original tools used by `lab.sh`

- Broker protocol/discovery proof:

```bash
kafka-broker-api-versions.sh --bootstrap-server BROKERS
```

- Partition health:

```bash
kafka-topics.sh --bootstrap-server BROKERS --describe --unavailable-partitions
kafka-topics.sh --bootstrap-server BROKERS --describe --under-replicated-partitions
```

- Consumer lag, log directories, dynamic broker configs, and feature state:

```bash
kafka-consumer-groups.sh --bootstrap-server BROKERS --all-groups --describe
kafka-log-dirs.sh --bootstrap-server BROKERS --describe
kafka-configs.sh --bootstrap-server BROKERS --entity-type brokers --all --describe
kafka-features.sh --bootstrap-server BROKERS describe
```

- Operating-system evidence is deliberately not hidden:

```bash
jps -lv
df -h
ss -ltnp
```
