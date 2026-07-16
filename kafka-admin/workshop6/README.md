# Workshop 06 — Rolling Maintenance, Upgrades, Backup, and DR

## Learning outcomes

- Execute a production-style rolling broker and ZooKeeper maintenance procedure.
- Separate reversible binary upgrades from later feature finalization.
- Export a metadata inventory and explain why it is not record backup.
- Run a disaster-recovery tabletop with RPO/RTO decisions.

## Maintenance entry gate

```bash
./lab.sh health
./lab.sh groups
./lab.sh log-dirs
./lab.sh backup-metadata
```

- Proceed only when:
  - no offline/under-replicated partitions;
  - no active reassignment or unrelated incident;
  - consumer lag is understood;
  - disk and network have failure headroom;
  - clients use multiple bootstrap brokers;
  - rollback, monitoring silence, and approval are ready.

## Demo 1 — Rolling broker OS maintenance

- For Broker 1, then 2, then 3:
  - run `health`;
  - press `Ctrl-C` on exactly one broker;
  - wait for controlled shutdown;
  - explain “patch/reboot occurs here”;
  - restart that broker;
  - wait for full ISR;
  - continue only after `health` is clean.
- Finish:

```bash
./lab.sh preferred-leaders
./lab.sh health
```

- Real tool: `kafka-leader-election.sh --election-type PREFERRED --all-topic-partitions`.
- Preferred election is not unclean election; it uses an eligible assigned replica.

## Demo 2 — Rolling ZooKeeper

- Use `status` to identify leader/followers.
- Restart one follower.
- Prove it rejoins before continuing.
- Restart the second follower and prove again.
- Restart the leader last; show election.
- Never stop two members of a three-node ensemble.

## Demo 3 — Metadata inventory

```bash
./lab.sh backup-metadata
find ../common/backups -maxdepth 2 -type f -print
```

- Exported evidence:
  - topic descriptions;
  - topic configs;
  - broker configs;
  - ACL listing or security-disabled evidence;
  - consumer-group offsets.
- Explicit limitation: this inventory does not contain Kafka record data.

## Upgrade decision sequence

- Read release notes, Java/OS matrix, known issues, and compatibility rules.
- Inventory plugins, interceptors, configs, ACLs, clients, and monitoring.
- Rehearse exact upgrade and rollback in staging.
- Roll broker binaries one node at a time.
- Soak and validate traffic, ISR, lag, latency, and errors.
- Advance metadata/protocol feature levels only after rollback is no longer needed.
- Inspect current state:

```bash
./lab.sh metadata-version
```

## DR tabletop

- Scenario cards:
  - accidental topic deletion;
  - complete cluster loss;
  - site/region loss;
  - ZooKeeper metadata loss;
  - records survive but group offsets are lost;
  - CA/private credentials are lost.
- For each scenario decide:
  - detection and declaration authority;
  - restore/failover data source;
  - acceptable RPO and RTO;
  - duplicate, loss, and ordering consequences;
  - validation owner;
  - failback plan.

## Participant challenge

- Write the go/no-go criteria for patching Broker 2.
- Identify the rollback point for a binary-only roll.
- Explain why replicated deletion is not recoverable from replicas.

## Must-know points

- Replication improves availability; it is not protection from operator/application deletion.
- Backup is unproven until restore is tested.
- Avoid combining upgrade, rebalance, security rotation, and unrelated config changes.
- Stop brokers before ZooKeeper during full shutdown; start ZooKeeper before brokers.
- Feature finalization may have different rollback properties from binary deployment.

## Original tools used by `lab.sh`

- Preferred replica leadership:

```bash
kafka-leader-election.sh --bootstrap-server BROKERS \
  --election-type PREFERRED --all-topic-partitions
```

- Feature/metadata state:

```bash
kafka-features.sh --bootstrap-server BROKERS describe
```

- Metadata inventory commands:

```bash
kafka-topics.sh --bootstrap-server BROKERS --describe
kafka-configs.sh --bootstrap-server BROKERS --entity-type topics --all --describe
kafka-configs.sh --bootstrap-server BROKERS --entity-type brokers --all --describe
kafka-acls.sh --bootstrap-server BROKERS --list
kafka-consumer-groups.sh --bootstrap-server BROKERS --all-groups --describe
```

- The helper redirects these outputs into timestamped files under `../common/backups/`.
- These commands inventory metadata; none copies Kafka record logs.
