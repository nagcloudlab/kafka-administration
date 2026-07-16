# Workshop 07 — MirrorMaker 2: Replication, Checkpoints, and Failover

## Learning outcomes

- Run independent source and target Kafka clusters.
- Prove topic records, configs, heartbeats, checkpoints, and translated offsets.
- Demonstrate source outage and target reads.
- Explain asynchronous RPO, cutover, and failback risks.

## Terminal map

- Source: existing three ZooKeepers and three brokers from Workshop 1.
- Target terminals 1–3: `target1`, `target2`, `target3`.
- Terminal 4: MirrorMaker 2.
- Terminal 5: source administration/producer.
- Terminal 6: target proof commands.

## Entry gate

```bash
./lab.sh health
./lab.sh preflight mm
```

- Source must be healthy.
- Target ports `39092–39094` and controller ports `39101–39103` must be free.

## Demo 1 — Prepare observable source state

```bash
./lab.sh create-topic orders 6
./lab.sh produce-sequence orders 10
./lab.sh consume-group-count training-group orders 5
./lab.sh offsets source orders
./lab.sh groups
```

- Expected source evidence:
  - ten numbered records;
  - a committed `training-group` position;
  - visible per-partition end offsets.
- Stop all `training-group` members before offset synchronization.

## Demo 2 — Build the target

```bash
./lab.sh setup-mm-target
```

- Real tools:
  - `kafka-storage.sh random-uuid`;
  - `kafka-storage.sh format --cluster-id ID --config workshop7/config/targetN.properties`.
- Start three terminals:

```bash
./lab.sh start-mm-target target1
./lab.sh start-mm-target target2
./lab.sh start-mm-target target3
```

- Target topology:
  - three combined KRaft nodes to fit a laptop;
  - brokers `39092–39094`;
  - controllers `39101–39103`;
  - RF=3 and min ISR=2.
- Production difference: size and separate roles/failure domains according to supported design.

## Demo 3 — Start MirrorMaker 2

```bash
./lab.sh start-mm2
```

- Real tool: `connect-mirror-maker.sh workshop7/config/mm2.properties`.
- Configuration points:
  - one-way `source->target.enabled=true`;
  - topic allowlist: `orders|payments|audit-events`;
  - group allowlist: `training-group`;
  - heartbeat/checkpoint/group-sync interval: five seconds;
  - RF=3 for internal topics;
  - identity naming for a deliberate one-way topology.

## Demo 4 — Four independent proofs

```bash
./lab.sh mm-status
./lab.sh offsets source orders
./lab.sh offsets target orders
./lab.sh mm-target-consume orders 10000
```

- Topic proof:
  - target `orders` exists with six partitions and RF=3.
- Record proof:
  - `proof-001` through `proof-010` appear at target.
- Internal-control proof:
  - heartbeat, checkpoint, offset-sync, and Connect internal topics exist.
- Offset proof:
  - inactive `training-group` position appears at target after sync interval.

## Demo 5 — Live incremental replication

```bash
./lab.sh produce-sequence orders 5
./lab.sh offsets source orders
./lab.sh offsets target orders
./lab.sh mm-target-consume orders 10000
```

- Expected proof: the latest numbered records reach target after asynchronous delay.
- Teaching question: “What is the observed RPO if source fails now?”

## Demo 6 — Failover rehearsal

- Confirm source/target offsets and inactive target group first.
- Stop source brokers.
- Leave target and its records running.
- Consume from target and verify availability.
- Explain missing automation:
  - client/DNS routing;
  - secrets and ACLs;
  - Schema Registry compatibility;
  - downstream dependencies;
  - explicit failback and duplicate handling.

## Participant challenge

- Add `audit-events` to source and prove it replicates.
- Create `not-approved`; prove it does not replicate.
- Explain why an active group on target prevents automatic synced-offset overwrite.
- Calculate observed record lag using source/target `offsets` output.

## Must-know points

- MirrorMaker 2 uses Kafka Connect mirror-source, heartbeat, and checkpoint connectors.
- Record replication, config/ACL replication, and offset translation are separate.
- Replication is asynchronous; RPO is workload and health dependent.
- Identity naming makes failover intuitive but requires strict loop prevention.
- A DR system needs regular cutover and failback tests, not only a running connector.

## Cleanup

- Stop MirrorMaker, then target nodes.

```bash
./lab.sh reset-mm-target
```

## Original tools used by `lab.sh`

- Generate target cluster identity and format each target node:

```bash
kafka-storage.sh random-uuid
kafka-storage.sh format --ignore-formatted --cluster-id CLUSTER_ID \
  --config workshop7/config/target1.properties
```

- Start target and MirrorMaker 2:

```bash
kafka-server-start.sh workshop7/config/target1.properties
connect-mirror-maker.sh workshop7/config/mm2.properties
```

- Prove target topics, assignments, group offsets, and records:

```bash
kafka-topics.sh --bootstrap-server TARGET_BROKERS --list
kafka-topics.sh --bootstrap-server TARGET_BROKERS --describe --topic orders
kafka-consumer-groups.sh --bootstrap-server TARGET_BROKERS --group training-group --describe
kafka-get-offsets.sh --bootstrap-server TARGET_BROKERS --topic orders
kafka-console-consumer.sh --bootstrap-server TARGET_BROKERS --topic orders \
  --from-beginning --timeout-ms 10000
```

- `TARGET_BROKERS` means `127.0.0.1:39092,127.0.0.1:39093,127.0.0.1:39094`.
- Inspect `config/mm2.properties`; it is passed directly to the original MirrorMaker launcher.
