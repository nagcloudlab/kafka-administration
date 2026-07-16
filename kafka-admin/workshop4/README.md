# Workshop 04 — Add Capacity and Reassign Partitions Safely

## Learning outcomes

- Add Broker 4 without pretending data automatically balances.
- Generate and review a real reassignment JSON plan.
- Execute with throttling, monitor ISR, verify completion, and remove throttles.
- Explain safe broker decommissioning.

## Pre-change evidence

```bash
./lab.sh health
./lab.sh describe orders
./lab.sh log-dirs
```

- Capture current leaders, replica sets, ISR, and disk placement.
- Ask: “What changes immediately when Broker 4 joins?”
- Correct answer: capacity and membership change; existing partitions do not automatically relocate.

## Demo 1 — Add Broker 4

```bash
./lab.sh start-broker broker4
```

```bash
./lab.sh status
./lab.sh health
./lab.sh log-dirs
```

- Expected proof:
  - Broker 4 responds to API discovery;
  - existing `orders` assignments remain unchanged;
  - Broker 4 initially stores no `orders` replicas.

## Demo 2 — Generate, never blindly execute

```bash
./lab.sh reassign-generate orders 1,2,3,4
```

- Real tool/options:

```text
kafka-reassign-partitions.sh --topics-to-move-json-file common/work/topics-to-move.json --broker-list 1,2,3,4 --generate
```

- The helper extracts only the printed **Proposed partition reassignment configuration** into `common/work/reassignment.json`.
- Open that generated file and review it; automatic extraction is not automatic approval.
- Review checklist:
  - every partition appears exactly once;
  - replica IDs are valid and unique per partition;
  - RF remains three;
  - Broker 4 receives replicas;
  - no broker/rack is overloaded;
  - target disks have headroom.

## Demo 3 — Throttled execution and proof

```bash
./lab.sh reassign-execute common/work/reassignment.json 5000000 APPLY
./lab.sh reassign-verify common/work/reassignment.json
./lab.sh health
./lab.sh log-dirs
```

- Expected proof:
  - partitions move to Broker 4;
  - ISR may change temporarily but no partition becomes unavailable;
  - verification reports completed partitions.
- Real execution: `--execute --throttle 5000000`.

## Demo 4 — Remove temporary throttles

```bash
./lab.sh reassign-clear-throttle common/work/reassignment.json
./lab.sh broker-configs
```

- Expected proof: no stale reassignment throttle remains.
- Important: the helper’s final `--verify` omits `--preserve-throttles`, allowing cleanup.

## Broker removal runbook

- Generate a reviewed plan with no replicas assigned to the retiring broker.
- Execute and verify the plan.
- Prove `kafka-log-dirs.sh --describe` shows no required replicas there.
- Restore preferred leaders if appropriate.
- Gracefully stop the broker.
- Remove its configuration only after observation and approval.

## Participant challenge

- Identify the most uneven broker by replica count and bytes.
- Propose a balanced placement without changing RF.
- State the expected network/disk impact.
- Execute with a throttle and prove completion.

## Must-know points

- Adding a broker and moving data are separate operations.
- Reassignment can saturate source and destination disks/network.
- Monitor ISR and client latency during movement.
- Production placement must include rack/zone awareness, not only numeric balance.
- Never decommission before every required replica has moved and caught up.

## Original tools used by `lab.sh`

- Generate the proposal:

```bash
kafka-reassign-partitions.sh --bootstrap-server BROKERS \
  --topics-to-move-json-file common/work/topics-to-move.json \
  --broker-list 1,2,3,4 --generate
```

- Execute with a five-megabyte-per-second replication throttle:

```bash
kafka-reassign-partitions.sh --bootstrap-server BROKERS \
  --reassignment-json-file common/work/reassignment.json \
  --execute --throttle 5000000
```

- Verify while preserving the throttle:

```bash
kafka-reassign-partitions.sh --bootstrap-server BROKERS \
  --reassignment-json-file common/work/reassignment.json \
  --verify --preserve-throttles
```

- Final verification removes temporary reassignment throttles:

```bash
kafka-reassign-partitions.sh --bootstrap-server BROKERS \
  --reassignment-json-file common/work/reassignment.json --verify
```

- Disk-placement proof uses `kafka-log-dirs.sh --bootstrap-server BROKERS --describe`.
