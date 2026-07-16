# Workshop 01 — Build, Prove, Break, and Recover the Cluster

## Learning outcomes

- Build three ZooKeeper nodes and three Kafka brokers as six visible processes.
- Prove quorum, replication factor, leadership, ISR, writes, reads, and persistence.
- Compare controlled shutdown with process failure.
- Finish with a completely healthy baseline.

## Terminal layout

- Terminals 1–3: `zk1`, `zk2`, `zk3`.
- Terminals 4–6: `broker1`, `broker2`, `broker3`.
- Terminal 7: administration and producer.
- Terminal 8: consumer.

## Pre-class gate

```bash
cd /path/to/kafka-admin/workshop1
./lab.sh setup
./lab.sh preflight base
```

- Expected proof: Java and Kafka 3.9.1 are reported; every base port is free.
- If preflight fails: stop the named conflicting process; do not change ports during class.

## Teach the configuration

- Show `config/zk1.properties`:
  - `clientPort=12181` is the Kafka/client port.
  - `server.1/2/3` defines peer and election ports.
  - `dataDir=common/data/zk1` persists metadata.
  - `myid=1` binds this process to `server.1`.
- Show `config/broker1.properties`:
  - `broker.id=1` is unique and persistent.
  - `zookeeper.connect` lists all ensemble members for discovery/failover.
  - RF=3 and `min.insync.replicas=2` define durability behavior.
  - `controlled.shutdown.enable=true` supports planned maintenance.
  - `unclean.leader.election.enable=false` favors consistency over forced availability.

## Demo 1 — Form ZooKeeper quorum

- Ask first: “How many of three nodes are required for quorum?”
- Start one command per terminal:

```bash
./lab.sh start-zk zk1
./lab.sh start-zk zk2
./lab.sh start-zk zk3
```

- Run:

```bash
./lab.sh status
```

- Expected proof:
  - exactly one `leader`;
  - exactly two `follower` nodes;
  - the leader is elected, not permanently assigned.
- Underlying proof: the helper sends ZooKeeper `srvr` four-letter commands to ports `12181–12183`.

## Demo 2 — Start brokers and prove health

```bash
./lab.sh start-broker broker1
./lab.sh start-broker broker2
./lab.sh start-broker broker3
```

```bash
./lab.sh status
./lab.sh health
```

- Expected proof:
  - three broker endpoints from `kafka-broker-api-versions.sh`;
  - no unavailable partitions;
  - no under-replicated partitions.

## Demo 3 — Deterministic replication proof

```bash
./lab.sh create-topic orders 6
./lab.sh describe orders
./lab.sh produce-sequence orders 10
./lab.sh consume-count orders 10
```

- Expected proof:
  - six partitions;
  - three replicas per partition;
  - all assigned replicas in ISR;
  - records `proof-001` through `proof-010` consumed.
- Real tools/options:
  - `kafka-topics.sh --create --partitions 6 --replication-factor 3`;
  - `kafka-console-producer.sh --producer-property acks=all`;
  - `kafka-console-consumer.sh --from-beginning --max-messages 10`.

## Demo 4 — Controlled broker maintenance

- Ask participants to predict which leaders will move.
- Press `Ctrl-C` in Broker 2.
- Prove degradation:

```bash
./lab.sh describe orders
./lab.sh health
./lab.sh produce-sequence orders 3
```

- Expected proof:
  - leaders formerly on Broker 2 move;
  - Broker 2 leaves ISR;
  - writes continue with two ISR members.
- Restart Broker 2 and repeat `health` until ISR is complete.
- Rule: never roll the next broker while ISR is incomplete.

## Demo 5 — ZooKeeper leader loss

- Identify the current ZooKeeper leader with `status`.
- Stop that one node with `Ctrl-C`.
- Run:

```bash
./lab.sh status
./lab.sh create-topic quorum-proof 3
```

- Expected proof:
  - remaining two nodes elect a leader;
  - metadata changes still work.
- Restart the stopped node before touching another.

## Demo 6 — Persistence proof

- Stop brokers first, then ZooKeeper nodes.
- Restart ZooKeeper first, then brokers.
- Run:

```bash
./lab.sh describe orders
./lab.sh consume-count orders 10
```

- Expected proof: topic metadata and records survive process restarts.

## Participant challenge

- Determine which broker leads the most partitions.
- Stop that broker gracefully.
- Prove the cluster remains writable.
- Recover it and prove every partition returns to full ISR.

## Must-know points

- ZooKeeper quorum and Kafka ISR are different quorums serving different purposes.
- RF=3 alone does not guarantee durable writes; producer acknowledgements and min ISR matter.
- A replica can exist on disk but not be eligible for leadership because it is outside ISR.
- `bootstrap.servers` performs discovery; clients do not permanently depend on the first address.
- One laptop proves process-level behavior, not host/rack/zone resilience.

## Clean finish

```bash
./lab.sh health
```

- Leave three brokers and three ZooKeepers healthy for Workshops 2–6.
- Use `./lab.sh reset` only when deliberately erasing all training data.

## Original tools used by `lab.sh`

- `start-zk zk1`:

```bash
../common/kafka_2.13-3.9.1/bin/zookeeper-server-start.sh workshop1/config/zk1.properties
```

- `start-broker broker1`:

```bash
../common/kafka_2.13-3.9.1/bin/kafka-server-start.sh workshop1/config/broker1.properties
```

- `create-topic orders 6`:

```bash
../common/kafka_2.13-3.9.1/bin/kafka-topics.sh \
  --bootstrap-server 127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094 \
  --create --if-not-exists --topic orders --partitions 6 --replication-factor 3
```

- `health`:

```bash
../common/kafka_2.13-3.9.1/bin/kafka-broker-api-versions.sh --bootstrap-server BROKERS
../common/kafka_2.13-3.9.1/bin/kafka-topics.sh --bootstrap-server BROKERS --describe --unavailable-partitions
../common/kafka_2.13-3.9.1/bin/kafka-topics.sh --bootstrap-server BROKERS --describe --under-replicated-partitions
```

- `produce-sequence` and `consume-count`:

```bash
kafka-console-producer.sh --bootstrap-server BROKERS --topic orders --producer-property acks=all
kafka-console-consumer.sh --bootstrap-server BROKERS --topic orders --from-beginning --max-messages 10 --timeout-ms 15000
```

- `BROKERS` in these examples means the complete `19092,19093,19094` bootstrap list shown above.
- Open [common/native-lab.sh](../common/native-lab.sh) to trace the wrapper implementation line by line.
