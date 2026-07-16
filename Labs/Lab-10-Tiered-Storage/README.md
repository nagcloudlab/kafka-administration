# Lab 10 — Tiered Storage (KIP-405)

Store hot log segments on brokers, cold segments in cheap object storage. Same topic, same consumer semantics — the tier boundary is invisible.

## Scenarios

- [10.1 — Enable tiered storage and watch segments offload](Scenario-10.1-Tiered-Storage-Setup.md)

## Common prereqs

- Labs 1 and 5 done (you understand segments, retention, and compaction).
- Cluster up: `./cluster.sh start-monitoring` in `kafka-lab/`.
- **Kafka 3.9+** — tiered storage is Early Access in 3.6, evolving in 3.7-3.9, targeted GA in 4.0. This lab uses 3.9's shape.
- A **RemoteStorageManager plugin**. For a training lab we use the `LocalTieredStorage` plugin that ships in Kafka's test-jar (see § 10.1 for the exact classpath dance).

## Where the "remote" tier lives (this lab)

To keep the lab self-contained, "remote" storage is just another local directory on the same host: `/tmp/kafka-tiered-storage/`. In production it would be S3 / GCS / Azure Blob via a real plugin (Aiven, Confluent, or your own implementation).

```
/tmp/kafka-tiered-storage/         # the "remote tier" — grows as segments offload
  <topic-uuid>/
    <partition>/
      <segment-base-offset>.log
      <segment-base-offset>.index
      <segment-base-offset>.timeindex
```

## Full tiered-storage-off reset

```bash
./kafka-lab/cluster.sh stop
# Remove the tiered-storage lines from broker configs (they were appended in 10.1)
for id in 101 102 103; do
  sed -i '/^# ==== Tiered Storage/,/^$/d' kafka-lab/config/broker-$id.properties
done
rm -rf /tmp/kafka-tiered-storage
./kafka-lab/cluster.sh start
```

## Not covered in Lab 10

- **Real object-storage plugins (S3 / GCS / Azure)** — same broker config, different `remote.log.storage.manager.class.name`. Aiven's open-source plugin is a good starting point.
- **Metadata manager choices** — this lab uses the built-in `TopicBasedRemoteLogMetadataManager` (metadata itself stored in a Kafka topic). Custom implementations exist for very large deployments.
- **Snapshot/restore of the remote tier** — object-storage backup is your object-storage vendor's problem; out of scope.
- **Cost modelling** — the whole reason tiered storage exists ($/TB × time × read frequency); a spreadsheet exercise, not a lab.
