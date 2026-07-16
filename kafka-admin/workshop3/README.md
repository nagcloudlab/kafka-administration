# Workshop 03 — Topic Lifecycle, Retention, Compaction, and Quotas

## Learning outcomes

- Apply and roll back a dynamic topic configuration.
- Increase partitions and explain the key-ordering consequence.
- Demonstrate quota configuration.
- Distinguish delete retention, compaction, topic deletion, and backup.

## Demo 1 — Dynamic retention with rollback

```bash
./lab.sh create-topic admin-demo 3
./lab.sh topic-config admin-demo
./lab.sh set-retention admin-demo 3600000
./lab.sh topic-config admin-demo
./lab.sh clear-retention admin-demo
```

- Expected proof:
  - `retention.ms=3600000` appears as a topic override;
  - clearing removes the override and restores broker default inheritance.
- Real tool: `kafka-configs.sh --entity-type topics --entity-name admin-demo --alter`.
- Important: retention removes eligible closed segments asynchronously; it is not an exact record timer.

## Demo 2 — Partition increase

```bash
./lab.sh describe admin-demo
./lab.sh increase-partitions admin-demo 6
./lab.sh describe admin-demo
```

- Expected proof: partition count increases from three to six.
- Irreversible fact: Kafka cannot decrease a topic’s partition count.
- Application risk: changing partition count can remap the same key and affect ordering assumptions.

## Demo 3 — Producer/consumer quota

```bash
./lab.sh set-user-quota training-user 1000000 2000000
./lab.sh describe-user-quota training-user
./lab.sh clear-user-quota training-user
```

- Expected proof: producer and consumer byte-rate overrides appear and then disappear.
- Real tool: `kafka-configs.sh --entity-type users --add-config producer_byte_rate=...,consumer_byte_rate=...`.
- Teaching point: quota throttling is protective behavior, not automatically a broker fault.
- Lab limitation: plaintext clients have no authenticated username; Workshop 10 provides identities needed to prove per-user enforcement.

## Demo 4 — Destructive-operation gate

```bash
./lab.sh delete-topic admin-demo
```

- Expected proof: helper refuses without explicit confirmation.
- Execute only after showing the topic contains disposable data:

```bash
./lab.sh delete-topic admin-demo DELETE
```

## Compaction whiteboard/demo extension

- `cleanup.policy=delete`: removes segments by time/size.
- `cleanup.policy=compact`: retains the latest value per key over time.
- Tombstone: key with null value; eventually removes the key during compaction.
- `compact,delete`: combines key compaction with time/size bounds.
- Compaction is asynchronous and does not preserve only one physical record immediately.

## Participant challenge

- Create `audit-events` with 12 partitions.
- Apply a two-hour retention override.
- Prove the override.
- Remove it and prove inheritance returned.
- Explain whether increasing to 24 partitions is safe for keyed ordering.

## Must-know points

- Retention and replication are not backup.
- Message-size settings must align across producer, broker/topic, consumer, and replica fetch.
- Partition increases, topic deletion, and record deletion have different blast radii.
- Review compliance, replay window, growth rate, and disk headroom before retention changes.
- Treat topic configuration as version-controlled change with before/after evidence.

## Original tools used by `lab.sh`

- Topic override and rollback:

```bash
kafka-configs.sh --bootstrap-server BROKERS --entity-type topics --entity-name admin-demo \
  --alter --add-config retention.ms=3600000
kafka-configs.sh --bootstrap-server BROKERS --entity-type topics --entity-name admin-demo \
  --alter --delete-config retention.ms
```

- Partition increase:

```bash
kafka-topics.sh --bootstrap-server BROKERS --alter --topic admin-demo --partitions 6
```

- User quota:

```bash
kafka-configs.sh --bootstrap-server BROKERS --alter --entity-type users \
  --entity-name training-user --add-config producer_byte_rate=1000000,consumer_byte_rate=2000000
```

- Protected deletion ultimately runs:

```bash
kafka-topics.sh --bootstrap-server BROKERS --delete --topic admin-demo
```

- Full executable paths resolve under `../common/kafka_2.13-3.9.1/bin/`.
