# Scenario 12.2 — Config precedence (broker default vs topic override vs client)

**Prereqs**
- Labs 1 and 3 done.
- Cluster on 9092–9094 running: `./cluster.sh start-monitoring` in `kafka-lab/`.

**Paste at the top of every new terminal:**

```bash
cd ~/kafka-administration/kafka-lab
export KAFKA=./kafka/bin
export BS=localhost:9092,localhost:9093,localhost:9094
export ZK=localhost:2181
```

---

## Problem

- **Every Kafka config value comes from one of four sources.** When something behaves unexpectedly, the first debugging question is: *"where did that value come from?"* If you don't know the precedence rules, you'll spend hours arguing over `broker-*.properties` while the answer is a topic-level override set by a previous admin six months ago.

- **The four sources** (highest precedence first):
  1. **Dynamic topic config** — set via `kafka-configs.sh --entity-type topics --alter --add-config X=Y`. Overrides everything else for that specific topic. Persists across broker restarts (stored in ZK or the KRaft metadata log).
  2. **Dynamic broker config** — set via `kafka-configs.sh --entity-type brokers --entity-name N --alter --add-config X=Y` (per-broker) OR `--entity-default` (cluster-wide). Persists.
  3. **Static broker config** — the values you set in `broker-N.properties` at process start. Requires broker restart to change.
  4. **Kafka's built-in default** — hard-coded in the code. What you get if none of the above applies.

- **Which client-side setting overrides what?** *None.* Producers and consumers have their own configs (like `acks`, `max.request.size`, `session.timeout.ms`) that live on the CLIENT — the broker doesn't override them. But some client settings interact with broker/topic limits:
  - Producer `max.request.size` (client, default 1 MB) → capped by broker `message.max.bytes` (default 1 MB) → capped by topic `max.message.bytes` (default = broker's). The **strictest** wins on the wire.
  - Consumer `fetch.max.bytes` (client) → interacts with broker `replica.fetch.max.bytes`.

- **The "hidden config drift" problem:**
  - Admin A creates a topic in prod with defaults.
  - Incident happens. Admin B applies a topic-level override to fix it ("set `retention.ms=1h` temporarily").
  - Incident resolved. Nobody reverts the override.
  - Six months later, Admin C changes the broker-level default. **The topic still uses the old override.** Admin C wastes an afternoon debugging.
  - Preventive practice: `kafka-configs.sh --describe` regularly, and audit topic overrides in every incident post-mortem.

- **Which properties are "dynamic"?** Kafka broker configs are marked `read-only`, `per-broker`, or `cluster-wide`. Only the last two can be changed via `kafka-configs.sh`. The [Kafka broker config docs](https://kafka.apache.org/documentation/#brokerconfigs) list this per property. Trying to dynamically change a `read-only` config gives an error.

- **Naming mismatch trap:**
  - Broker property: `log.retention.hours` (default 168 = 7 days)
  - Topic property: `retention.ms` (in milliseconds)
  - **They have different names for the same concept.** The topic property takes precedence but you can't grep for the exact string across both. See the [Kafka topic config docs](https://kafka.apache.org/documentation/#topicconfigs) for the full mapping table.

- **Common misconfigurations:**
  - Setting `retention.ms=0` on a topic thinking it disables retention → actually deletes everything immediately (min value is enforced but low).
  - Setting a broker default via `broker-*.properties`, forgetting to restart brokers → the file was updated but the running process still uses the old value.
  - Changing a `read-only` config dynamically → command silently fails, admin thinks it worked.
  - Removing a topic override intending to revert → falls back to the broker default, which may itself have been changed since.

## Symptom

- **"My topic's retention doesn't match broker.properties"** — check for a topic override.
- **"I set X=Y in server.properties but nothing changed"** — either the broker wasn't restarted, or a dynamic broker override wins.
- **"Producer says `RecordTooLargeException` but my broker has `message.max.bytes=10MB`"** — check the TOPIC's `max.message.bytes` and the producer's `max.request.size` — either one being smaller wins.
- **"kafka-configs shows no override, but retention isn't what I expect"** — you're probably looking at `--describe` output that omits inherited values; use `--all` where supported.

## Setup — 3 terminals

**Terminal 1 (control):**

```bash
# Create a plain topic — no overrides. Will inherit broker defaults.
$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --create --topic config-demo-inherited --partitions 3 --replication-factor 3

# Create a topic with an explicit override at creation time
$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --create --topic config-demo-override --partitions 3 --replication-factor 3 \
  --config retention.ms=3600000 \
  --config segment.ms=60000
```

**Terminal 2 (config compare — leave running):**

```bash
watch -n 3 '
echo "=== inherited topic ==="
'"$KAFKA"'/kafka-configs.sh --bootstrap-server '"$BS"' \
  --entity-type topics --entity-name config-demo-inherited --describe 2>/dev/null | \
  grep -oE "[a-z._]+=[^ ,]+" | head -10
echo
echo "=== override topic ==="
'"$KAFKA"'/kafka-configs.sh --bootstrap-server '"$BS"' \
  --entity-type topics --entity-name config-demo-override --describe 2>/dev/null | \
  grep -oE "[a-z._]+=[^ ,]+" | head -10
'
```

**Terminal 3 (broker log):**

```bash
tail -F logs/broker-101.log | grep --line-buffered -E 'DynamicConfigManager|IncrementalAlterConfigs|LogManager'
```

## Trigger — Step 1: prove the "inherited" topic uses no explicit overrides

**Terminal 1:**

```bash
# Basic describe — shows only OVERRIDES, not inherited values
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type topics --entity-name config-demo-inherited --describe
```

Expected: `Dynamic configs for topic config-demo-inherited are: (empty)` or a very short list. This topic inherits everything from broker defaults.

```bash
# Full describe — shows every effective config value AND its source
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type topics --entity-name config-demo-inherited --describe --all 2>&1 | \
  grep -E 'retention|segment|cleanup' | head -10
```

Expected: each line ends with something like `{DEFAULT_CONFIG}` (inherited from broker default) or `{STATIC_BROKER_CONFIG}` (explicitly set in broker's server.properties).

## Trigger — Step 2: compare with the "override" topic

```bash
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type topics --entity-name config-demo-override --describe
```

Expected:
```
Dynamic configs for topic config-demo-override are:
  retention.ms=3600000 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=3600000, ...}
  segment.ms=60000 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:segment.ms=60000, ...}
```

The `synonyms=` list is the **source chain**: which config layers set this value. `DYNAMIC_TOPIC_CONFIG` is the topic override; had there been a broker-level default it would appear too, further down.

## Trigger — Step 3: change the broker default DYNAMICALLY

Set a cluster-wide broker default for `log.retention.ms` (note: broker uses `log.` prefix; topic uses no prefix):

```bash
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type brokers --entity-default \
  --alter --add-config log.retention.ms=7200000

# Verify
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type brokers --entity-default --describe 2>&1 | grep retention
```

**T3 broker log** shows `Processing broker configs update` — the change is applied without restart.

## Observe — Step 4: watch how the two topics react

Re-run the describes:

```bash
# Inherited topic — no override, so it picks up the NEW broker default
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type topics --entity-name config-demo-inherited \
  --describe --all 2>&1 | grep -E '^\s+retention.ms'

# Override topic — its own retention.ms=3600000 wins, ignores the broker change
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type topics --entity-name config-demo-override \
  --describe --all 2>&1 | grep -E '^\s+retention.ms'
```

Expected:
- Inherited topic: `retention.ms=7200000  {DYNAMIC_DEFAULT_BROKER_CONFIG:log.retention.ms=7200000}` — now uses the new broker default.
- Override topic: `retention.ms=3600000  {DYNAMIC_TOPIC_CONFIG:retention.ms=3600000, DYNAMIC_DEFAULT_BROKER_CONFIG:log.retention.ms=7200000, ...}` — override wins; the broker default is listed as a lower-precedence synonym.

**This is the precedence rule in action.** Topic override > broker dynamic > broker static > code default.

## Trigger — Step 5: remove the override, watch inheritance resume

```bash
# Remove the topic-level retention.ms override
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type topics --entity-name config-demo-override \
  --alter --delete-config retention.ms

# Now it inherits the current broker default
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type topics --entity-name config-demo-override \
  --describe --all 2>&1 | grep -E '^\s+retention.ms'
```

Expected: `retention.ms=7200000  {DYNAMIC_DEFAULT_BROKER_CONFIG:log.retention.ms=7200000}` — the override is gone; the topic uses the broker default.

## Trigger — Step 6: prove that dynamic changes persist across broker restart

```bash
# Restart broker 101 gracefully
./cluster.sh stop-broker 101
sleep 3
./cluster.sh start-broker-monitoring 101
sleep 5

# The dynamic broker default we set should still be there
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type brokers --entity-default --describe 2>&1 | grep retention
```

Expected: `log.retention.ms=7200000` still there. Dynamic configs are persisted to ZK (or KRaft metadata log) — not just in-memory.

## Trigger — Step 7: audit — find every non-default topic override in the cluster

The production question every quarter:

```bash
# List every topic and any explicit dynamic overrides
$KAFKA/kafka-topics.sh --bootstrap-server $BS --list | \
  grep -v '^__' | \
  while read topic; do
    overrides=$($KAFKA/kafka-configs.sh --bootstrap-server $BS \
      --entity-type topics --entity-name "$topic" --describe 2>/dev/null | \
      grep -oE '[a-z._]+=[^ ,]+' | tr '\n' ',')
    if [[ -n "$overrides" ]]; then
      echo "$topic → $overrides"
    fi
  done
```

Expected: your two demo topics show up, plus any leftover overrides from earlier scenarios (a1-*, b1-*, c1-*, etc. if they still exist). Each line is a topic override that DIVERGES from broker default. **Every one is a candidate for a "why is this different?" review.**

## Solution

- **Always use `--describe --all`** when investigating. Basic `--describe` shows overrides only; `--all` shows every effective value AND its source. In production incidents, the source chain (`synonyms=`) is the debugging gold.

- **Precedence cheat sheet** (highest wins):
  ```
  DYNAMIC_TOPIC_CONFIG
    > DYNAMIC_BROKER_CONFIG (per-broker)
    > DYNAMIC_DEFAULT_BROKER_CONFIG (cluster-wide default)
    > STATIC_BROKER_CONFIG (server.properties)
    > DEFAULT_CONFIG (Kafka code default)
  ```

- **Change types**:
  | Change | Requires broker restart? | Persists? |
  |--------|-------------------------|-----------|
  | Edit `server.properties`, restart | Yes | Yes |
  | `kafka-configs --entity-type brokers --entity-name N` | No | Yes |
  | `kafka-configs --entity-type brokers --entity-default` | No | Yes |
  | `kafka-configs --entity-type topics` | No | Yes |
  | (Some configs marked `read-only`) | Yes | (via server.properties only) |

- **Prefer explicit topic overrides at creation** over changing broker defaults later. New topics inherit whatever the broker default *currently* is; if you change the default in six months, existing topics DON'T pick up the new value unless they had no override.

- **Change management practice**:
  1. Every topic override should have a JIRA / ticket reference in a runbook, not just a hallway conversation.
  2. Quarterly audit: run the Step 7 grep, review every diverging override with the topic owner. Delete stale ones.
  3. When responding to an incident, if you set a temporary override, add a TODO with an expiry date. Otherwise it becomes permanent.

- **Broker vs topic property name pairs** (the ones you'll hit most):
  | Broker property | Topic property |
  |-----------------|----------------|
  | `log.retention.hours` / `log.retention.ms` | `retention.ms` |
  | `log.retention.bytes` | `retention.bytes` |
  | `log.segment.bytes` | `segment.bytes` |
  | `log.segment.ms` | `segment.ms` |
  | `log.cleanup.policy` | `cleanup.policy` |
  | `min.insync.replicas` | `min.insync.replicas` (same name) |
  | `unclean.leader.election.enable` | `unclean.leader.election.enable` (same name) |
  | `message.max.bytes` | `max.message.bytes` |

- **Dynamic reload gotcha**: some configs take effect immediately (retention.ms, min.insync.replicas). Others take effect on next segment roll (segment.bytes). Others are only re-read on broker start (num.network.threads, etc.). Check the docs for each property; a config change that "does nothing" is usually a restart-required property that wasn't marked as such in your head.

## Verify

```bash
# 1. Both demo topics exist
$KAFKA/kafka-topics.sh --bootstrap-server $BS --list | grep config-demo

# 2. Broker-level default we set is still there
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type brokers --entity-default --describe 2>&1 | grep retention

# 3. --all shows source chain including the DYNAMIC_DEFAULT_BROKER_CONFIG synonym
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type topics --entity-name config-demo-inherited \
  --describe --all 2>&1 | grep -A2 'retention.ms' | head -5
```

## Takeaway

> **Every config has a source. Topic override wins over broker dynamic wins over broker static wins over code default. Use `--describe --all` when in doubt — the `synonyms=` list is the debugging story.**

## Instructor notes
- Poll before Step 4: *"If I set `log.retention.ms=7200000` at cluster level, do all my existing topics get 2h retention?"* Most say yes. The demo shows: only topics WITHOUT overrides pick up the change.
- The Step 7 audit script is a real-world artifact. Save it as a shell alias for the students — running it monthly in prod prevents config drift.
- Real-world story: at a large customer, a topic had `retention.ms=1000` (1 second) from an incident 8 months ago that nobody remembered. When ingestion volumes 10x'd during a launch, disk didn't fill (because everything was deleted immediately) — but auditors couldn't replay any of the day's transactions. The `synonyms=` output would have caught it in seconds.
- Bridge to 12.3: *"Now you know how to inspect and change configs. Next: the one topic setting you can't undo — partition count."*

## Teardown

```bash
# Delete demo topics
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic config-demo-inherited
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic config-demo-override

# Revert the cluster-wide broker default we set
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --entity-type brokers --entity-default \
  --alter --delete-config log.retention.ms
```
