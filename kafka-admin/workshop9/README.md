# Workshop 09 — KRaft Controllers, Metadata Quorum, and Migration

## Learning outcomes

- Replace ZooKeeper concepts with a three-controller KRaft metadata quorum.
- Keep three dedicated brokers separate from controllers.
- Prove cluster identity, leader/high-watermark/follower lag, controller failover, and broker service continuity.
- Explain migration boundaries without pretending migration is a file copy.

## Terminal map

- Terminals 1–3: `controller1`, `controller2`, `controller3`.
- Terminals 4–6: KRaft `broker1`, `broker2`, `broker3`.
- Terminal 7: administration.
- Stop every ZooKeeper-mode process first.

## Entry gate

```bash
./lab.sh preflight kraft
```

- Expected proof: broker ports `29092–29094`, controller ports `29101–29103`, and JMX ports are free.

## Demo 1 — One cluster ID, six formatted nodes

```bash
./lab.sh setup-kraft
cat ../common/work/kraft-cluster-id
```

- Real tools:
  - `kafka-storage.sh random-uuid`;
  - `kafka-storage.sh format --cluster-id ID --config workshop9/config/kraft/...`.
- Configuration distinctions:
  - controllers: `process.roles=controller`, node IDs `101–103`;
  - brokers: `process.roles=broker`, node IDs `1–3`;
  - every node references the same controller voters and cluster ID;
  - controllers store metadata log; brokers store record logs.
- Warning: formatting an existing directory with the wrong identity is destructive.

## Demo 2 — Form controller quorum first

```bash
./lab.sh start-controller controller1
./lab.sh start-controller controller2
./lab.sh start-controller controller3
```

- Start brokers afterward:

```bash
./lab.sh start-kraft-broker broker1
./lab.sh start-kraft-broker broker2
./lab.sh start-kraft-broker broker3
```

## Demo 3 — Metadata quorum proof

```bash
./lab.sh kraft-status
```

- Real tool:

```text
kafka-metadata-quorum.sh --bootstrap-controller 127.0.0.1:29101,127.0.0.1:29102,127.0.0.1:29103 describe --status
```

- Identify:
  - `ClusterId`;
  - `LeaderId`;
  - `LeaderEpoch`;
  - `HighWatermark`;
  - voters;
  - max follower lag/time.
- Healthy proof: one leader, three voters, zero/small converging lag.

## Demo 4 — Controller follower loss

- Stop a controller that is not leader.
- Run `kraft-status`.
- Expected proof:
  - quorum retains leader and service;
  - stopped voter shows lag/unavailability;
  - brokers continue serving.
- Restart follower and prove it catches up.

## Demo 5 — Active controller loss

- Record current `LeaderId`.
- Stop that controller.
- Run `kraft-status` against remaining controller addresses.
- Expected proof:
  - leader ID and epoch change;
  - quorum remains available with two voters;
  - broker traffic continues.
- Restart and prove follower lag returns to zero.

## Migration teaching sequence

- Inventory deployed Kafka version and ZooKeeper health.
- Confirm the exact supported migration path for that version/vendor.
- Provision KRaft controllers with migration configuration.
- Perform documented staged controller and broker rolls.
- Observe migration/dual-write state.
- Validate clients, metadata, ACLs, configs, and operations.
- Finalize only after understanding the rollback boundary.
- Never describe migration as copying ZooKeeper files into KRaft directories.

## Participant challenge

- Predict the effect of stopping two of three controllers.
- Explain why running brokers do not replace missing controller quorum.
- Find the node IDs, voter endpoints, and log directories in each config.
- Prove a restarted follower catches the metadata high watermark.

## Must-know points

- A three-controller quorum tolerates one controller failure.
- Controller node IDs and broker node IDs must be unique cluster-wide.
- Cluster ID and formatted directory identity prevent cross-cluster mistakes.
- Combined mode is convenient for small labs; dedicated roles are clearer for production operations.
- Kafka 4.x is KRaft-only; ZooKeeper remains relevant for legacy administration and migration.

## Cleanup

- Stop brokers first, then controllers.

```bash
./lab.sh reset-kraft
```

## Original tools used by `lab.sh`

- Generate and format storage:

```bash
kafka-storage.sh random-uuid
kafka-storage.sh format --ignore-formatted --cluster-id CLUSTER_ID \
  --config workshop9/config/kraft/controller1.properties
kafka-storage.sh format --ignore-formatted --cluster-id CLUSTER_ID \
  --config workshop9/config/kraft/broker1.properties
```

- Controllers and brokers use the same original server launcher with different configs:

```bash
kafka-server-start.sh workshop9/config/kraft/controller1.properties
kafka-server-start.sh workshop9/config/kraft/broker1.properties
```

- Quorum and partition proof:

```bash
kafka-metadata-quorum.sh \
  --bootstrap-controller 127.0.0.1:29101,127.0.0.1:29102,127.0.0.1:29103 \
  describe --status
kafka-topics.sh \
  --bootstrap-server 127.0.0.1:29092,127.0.0.1:29093,127.0.0.1:29094 \
  --describe --under-replicated-partitions
```
