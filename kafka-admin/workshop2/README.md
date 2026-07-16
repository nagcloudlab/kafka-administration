# Workshop 02 — Consumer Groups, Lag, Rebalancing, and Replay

## Learning outcomes

- Prove partition ownership and group scaling.
- Interpret committed offset, log-end offset, and lag.
- Trigger a rebalance visibly.
- Preview and execute a safe offset reset on disposable data.

## Prerequisite

- Workshop 1 cluster is healthy.
- `orders` has six partitions.

```bash
./lab.sh health
./lab.sh produce-sequence orders 30
```

## Demo 1 — Create a committed group

```bash
./lab.sh consume-group-count training-group orders 10
./lab.sh groups
```

- Expected proof:
  - `CURRENT-OFFSET` advances;
  - `LOG-END-OFFSET` reflects produced records;
  - `LAG` is the difference per partition.
- Real tool: `kafka-consumer-groups.sh --all-groups --describe`.

## Demo 2 — Scale consumers and observe assignment

- Start two terminals with the same group:

```bash
./lab.sh consume-group training-group orders
./lab.sh consume-group training-group orders
```

- Inspect:

```bash
./lab.sh group-members training-group
```

- Expected proof:
  - each partition is owned by one group member;
  - six partitions cap useful active consumers at six;
  - extra consumers would be idle.
- Stop one consumer and immediately repeat `group-members`.
- Point out the temporary pause and reassignment during rebalance.

## Demo 3 — Offset reset safety workflow

- Stop every `training-group` consumer.
- Preview only:

```bash
./lab.sh reset-offsets training-group orders --to-earliest preview
```

- Underlying option: `kafka-consumer-groups.sh --reset-offsets --dry-run`.
- Ask participants to approve the old/new offsets.
- Apply to disposable training data:

```bash
./lab.sh reset-offsets training-group orders --to-earliest APPLY
./lab.sh consume-group-count training-group orders 10
```

- Expected proof: old records replay and offsets advance again.

## Demo 4 — Skip-forward warning

```bash
./lab.sh reset-offsets training-group orders --to-latest preview
```

- Do not apply until participants explain the consequence.
- Consequence: moving forward skips unprocessed records from the group’s perspective.
- `--shift-by N` is supported; negative shifts replay and positive shifts skip.

## Incident drill

- Symptom: lag continually grows.
- Evidence to collect:
  - group state and members;
  - lag by partition;
  - consumer error/rebalance logs;
  - broker fetch latency and throttling;
  - downstream dependency health.
- Avoid: resetting offsets as a generic lag fix.

## Participant challenge

- Produce 20 records.
- Consume 7 with `challenge-group`.
- Calculate expected lag before running `groups`.
- Preview a replay to earliest.
- Explain duplicate-processing requirements before applying it.

## Must-know points

- Offsets are per group, topic, and partition.
- Rebalancing is normal; continuous rebalancing is an incident symptom.
- Lag requires rate and time context, not only a static number.
- Backward reset can duplicate side effects; forward reset can lose processing.
- Snapshot offsets, stop the group, preview, obtain approval, execute, and verify.

## Original tools used by `lab.sh`

- Named consumer:

```bash
kafka-console-consumer.sh --bootstrap-server BROKERS --topic orders \
  --group training-group --consumer-property enable.auto.commit=true
```

- Group lag and membership:

```bash
kafka-consumer-groups.sh --bootstrap-server BROKERS --all-groups --describe
kafka-consumer-groups.sh --bootstrap-server BROKERS --group training-group --describe --members --verbose
```

- Safe preview and explicit execution:

```bash
kafka-consumer-groups.sh --bootstrap-server BROKERS --group training-group \
  --topic orders --reset-offsets --to-earliest --dry-run
kafka-consumer-groups.sh --bootstrap-server BROKERS --group training-group \
  --topic orders --reset-offsets --to-earliest --execute
```

- Other supported targets: `--to-latest` and `--shift-by N`.
- Full executable paths resolve under `../common/kafka_2.13-3.9.1/bin/`.
