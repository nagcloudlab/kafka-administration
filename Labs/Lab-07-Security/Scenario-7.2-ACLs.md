# Scenario 7.2 — ACLs (what may you do?)

**Prereqs**
- Sc 7.1 done — SASL is enabled and users `alice` + `bob` exist.
- Cluster up.

**Paste at the top of every new terminal:**

```bash
cd ~/kafka-administration/kafka-lab
export KAFKA=./kafka/bin
export BS=localhost:9092,localhost:9093,localhost:9094
export ZK=localhost:2181
```

---

## Problem

- **SASL answers "who are you?"** ACLs answer **"what may you do?"** — you need both for real isolation.
- **Kafka's authorization model:**
  - Enabled by setting `authorizer.class.name` on each broker.
  - **AclAuthorizer** (ZK mode, this lab) — ACL entries stored under `/kafka-acl/` in ZK.
  - **StandardAuthorizer** (KRaft mode) — ACLs stored in the metadata log.
  - Both implement the same conceptual model.

- **The four things an ACL binds together:**
  - **Principal** — who: `User:alice`, `Group:analytics`, or `User:*` (everyone).
  - **Operation** — verb: `Read`, `Write`, `Create`, `Delete`, `Alter`, `Describe`, `DescribeConfigs`, `AlterConfigs`, `ClusterAction`, `IdempotentWrite`, `All`.
  - **Resource** — noun: a topic name, a consumer group id, a transactional-id, or the whole cluster.
  - **Permission** — `Allow` or `Deny`. Deny wins over Allow when both match.

- **Default posture: deny-all.** With `allow.everyone.if.no.acl.found=false` (the safe default), a principal without any matching Allow is denied. Producing/consuming without an ACL fails with `TopicAuthorizationException`.

- **The `super.users` bypass:**
  - Principals in `super.users=User:X;User:Y` skip all ACL checks.
  - Almost always used for the internal admin user AND for inter-broker traffic if it lands as `User:ANONYMOUS` (i.e. when the inter-broker listener has no auth — as in this lab).
  - Semicolon-separated. Prefix is required (`User:`, not just `alice`).

- **What a producer/consumer actually needs:**
  - **Produce** to topic X → `Write` on `Topic:X` (+ `IdempotentWrite` on `Cluster` if `enable.idempotence=true`).
  - **Consume** from topic X in group G → `Read` on `Topic:X` **and** `Read` on `Group:G`.
  - **Transactions** → also need `Write` on `TransactionalId:<txn-id>`.
  - Missing any one → the operation fails with a clean, ACL-specific error message.

- **Common misconfigurations:**
  - Enabling the authorizer without adding `User:ANONYMOUS` to `super.users` while inter-broker uses PLAINTEXT → brokers can't replicate → the cluster falls over.
  - Granting `Write` on a topic but forgetting `Read` on the consumer group → consumer just hangs at `poll()` with no obvious error until you check DEBUG logs.
  - `--allow-principal 'alice'` (missing `User:` prefix) → the ACL is created but never matches any real principal.
  - Confusing prefix-ACL syntax: `--resource-pattern-type prefixed --topic payments-` matches `payments-*`; without `prefixed` it's a literal match.

## Symptom
- Client (unauthorised): `TopicAuthorizationException: Not authorized to access topics: [X]` — very specific, easy to grep.
- Consumer (missing group ACL): `GroupAuthorizationException: Not authorized to access group: G`.
- Broker log (denied action): `Principal = User:alice is Denied Operation = Write from host = 127.0.0.1 on resource = Topic:LITERAL:secure-payments`. Fingerprint for `grep`ing.
- Cluster catastrophically dead after enabling authorizer without super-user for inter-broker → `logs/broker-*.log` fills with `ClusterAuthorizationFailedException` from the replication fetcher.

## Setup — 3 terminals

**Terminal 1 (control):**

```bash
# Same alice-client.properties from Sc 7.1 — recreate if the file was cleaned up:
cat > /tmp/alice-client.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="alice" password="alice-secret";
EOF

cat > /tmp/bob-client.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="bob" password="bob-secret";
EOF
```

**Terminal 2 (live ACL view):**

```bash
watch -n 3 "$KAFKA/kafka-acls.sh --bootstrap-server $BS --list 2>/dev/null | head -30"
```

Baseline: no ACLs (empty).

**Terminal 3 (broker 101 authorization log tail):**

```bash
tail -F logs/broker-101.log | grep --line-buffered -E 'Denied|Allowed'
```

## Trigger — Step 1: enable the authorizer on all brokers

Paste in Terminal 1:

```bash
for id in 101 102 103; do
  cat >> config/broker-$id.properties <<EOF

# ==== Authorization (Sc 7.2) ====
authorizer.class.name=kafka.security.authorizer.AclAuthorizer
super.users=User:ANONYMOUS;User:admin
allow.everyone.if.no.acl.found=false
EOF
done

tail -5 config/broker-101.properties
```

- `User:ANONYMOUS` is a super-user because our inter-broker listener is PLAINTEXT — replication requests arrive without a SASL principal. In production, remove ANONYMOUS from super-users and set `inter.broker.listener.name=SASL_PLAINTEXT` with real broker credentials.

## Trigger — Step 2: rolling restart

```bash
for id in 101 102 103; do
  ./cluster.sh stop-broker              $id
  sleep 3
  ./cluster.sh start-broker-monitoring  $id
  sleep 5
done
```

## Observe — Part 1: default posture is DENY

Paste in Terminal 1:

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --create --topic secure-payments --partitions 3 --replication-factor 3

# Alice tries to produce — no ACL yet
echo "hello from alice" | \
  $KAFKA/kafka-console-producer.sh --bootstrap-server localhost:9192 --topic secure-payments \
  --producer.config /tmp/alice-client.properties 2>&1 | head -5
```

Expected: `TopicAuthorizationException: Not authorized to access topics: [secure-payments]`.

- **T3 (broker log):** `Principal = User:alice is Denied Operation = Write from host = 127.0.0.1 on resource = Topic:LITERAL:secure-payments`.

## Trigger — Step 3: grant alice minimal ACLs to produce

```bash
# Alice: write on the topic + idempotent write on the cluster
$KAFKA/kafka-acls.sh --bootstrap-server $BS --add \
  --allow-principal User:alice \
  --operation Write --operation Describe --operation Create \
  --topic secure-payments

$KAFKA/kafka-acls.sh --bootstrap-server $BS --add \
  --allow-principal User:alice \
  --operation IdempotentWrite \
  --cluster
```

- **T2 (ACL view):** now shows two Alice ACLs. Look for `Topic:LITERAL:secure-payments` and `Cluster:kafka-cluster`.

Retry the produce:

```bash
echo "hello from alice" | \
  $KAFKA/kafka-console-producer.sh --bootstrap-server localhost:9192 --topic secure-payments \
  --producer.config /tmp/alice-client.properties
```

Expected: silent success. **T3** shows `Allowed` lines now for `Principal = User:alice`.

## Trigger — Step 4: prove bob is still denied

```bash
echo "hello from bob" | \
  $KAFKA/kafka-console-producer.sh --bootstrap-server localhost:9192 --topic secure-payments \
  --producer.config /tmp/bob-client.properties 2>&1 | head -5
```

Expected: `TopicAuthorizationException` — bob doesn't have any Write ACL. **T3** shows `Denied` for `User:bob`.

## Trigger — Step 5: consume as alice — the group-ACL surprise

```bash
# First attempt — no Read ACL on the consumer group
$KAFKA/kafka-console-consumer.sh --bootstrap-server localhost:9192 --topic secure-payments \
  --from-beginning --timeout-ms 5000 \
  --consumer.config /tmp/alice-client.properties \
  --group alice-analytics 2>&1 | head -5
```

Expected: `GroupAuthorizationException: Not authorized to access group: alice-analytics`.

**Fix with the second half of the consume ACL:**

```bash
$KAFKA/kafka-acls.sh --bootstrap-server $BS --add \
  --allow-principal User:alice \
  --operation Read \
  --topic secure-payments \
  --group alice-analytics
```

Retry:

```bash
$KAFKA/kafka-console-consumer.sh --bootstrap-server localhost:9192 --topic secure-payments \
  --from-beginning --timeout-ms 3000 \
  --consumer.config /tmp/alice-client.properties \
  --group alice-analytics
```

Expected: `hello from alice` prints, then the consumer exits after the 3 s timeout.

## Solution

- **Grant patterns that match real workloads:**

  Producer-only service (fire-and-forget):
  ```bash
  $KAFKA/kafka-acls.sh --bootstrap-server $BS --add \
    --allow-principal User:orders-ingest \
    --operation Write --operation Describe \
    --topic 'orders-'    --resource-pattern-type prefixed
  $KAFKA/kafka-acls.sh --bootstrap-server $BS --add \
    --allow-principal User:orders-ingest \
    --operation IdempotentWrite --cluster
  ```

  Consumer-only service:
  ```bash
  $KAFKA/kafka-acls.sh --bootstrap-server $BS --add \
    --allow-principal User:orders-processor \
    --operation Read --operation Describe \
    --topic 'orders-'    --resource-pattern-type prefixed \
    --group 'orders-processor-*'  --resource-pattern-type prefixed
  ```

  Transactional producer (EOS):
  ```bash
  $KAFKA/kafka-acls.sh --bootstrap-server $BS --add \
    --allow-principal User:payments \
    --operation Write --operation Describe --topic payments \
    --operation Write --operation Describe --transactional-id 'payments-txn-' \
    --resource-pattern-type prefixed
  ```

- **Use prefixed ACLs to scale.** Never `--topic '*'` in production — one wildcard grant undoes the whole authorization posture.

- **Auditing:** the broker log lines `Principal = X is Allowed/Denied Operation = Y on resource = Z` are your audit trail. Ship them to your log aggregator with a filter on `is Denied` — every hit is either a misconfig or an attack.

- **List and remove:**
  ```bash
  # All ACLs
  $KAFKA/kafka-acls.sh --bootstrap-server $BS --list

  # By principal
  $KAFKA/kafka-acls.sh --bootstrap-server $BS --list --principal User:alice

  # Remove one
  $KAFKA/kafka-acls.sh --bootstrap-server $BS --remove \
    --allow-principal User:alice --operation Write --topic secure-payments --force
  ```

- **Migration tip:** to check the impact of turning on ACLs before you enforce them, run brokers with:
  ```properties
  authorizer.class.name=kafka.security.authorizer.AclAuthorizer
  allow.everyone.if.no.acl.found=true
  ```
  ...for a discovery period. Watch broker logs for `is Denied` lines — every one is a client you need an ACL for. Once the log is quiet, flip to `false` for enforcement.

## Verify

```bash
# 1. Authorizer is loaded on all brokers
grep -c '^authorizer.class.name' config/broker-*.properties
# Expected: 1 per broker

# 2. ACL count
$KAFKA/kafka-acls.sh --bootstrap-server $BS --list | grep -c 'Principal'

# 3. Positive path (alice → secure-payments) works
echo "verify-allowed" | $KAFKA/kafka-console-producer.sh \
  --bootstrap-server localhost:9192 --topic secure-payments \
  --producer.config /tmp/alice-client.properties

# 4. Negative path (bob → secure-payments) fails with the right exception
echo "verify-denied" | $KAFKA/kafka-console-producer.sh \
  --bootstrap-server localhost:9192 --topic secure-payments \
  --producer.config /tmp/bob-client.properties 2>&1 | grep -i authorization | head -1
# Expected: TopicAuthorizationException: Not authorized to access topics: [secure-payments]
```

## Takeaway

> **SASL says who. ACL says what. Deny-by-default, prefix-scoped, log every deny.**

## Instructor notes
- Poll before Step 3: *"What's the minimum ACL to produce with `acks=all, enable.idempotence=true`?"* Most say "Write on topic". Miss the `IdempotentWrite` on Cluster — the demo throws exactly that error if it's forgotten.
- The consumer-group ACL trap (Step 5) is the most common real-world "why is my consumer stuck" ticket. Point at the `GroupAuthorizationException` and read it out loud — the error is clear once you see it.
- Draw on the whiteboard the deny-by-default table:
  - `allow.everyone.if.no.acl.found=false` (default) → strict; any missing ACL = denied
  - `allow.everyone.if.no.acl.found=true` → migration mode; missing ACL = allowed but broker logs a hint
- `super.users=User:ANONYMOUS` is what makes this lab work with a PLAINTEXT inter-broker listener. **Never in production.** Mention it and show the safer path (dedicated broker credential + SASL inter-broker).
- Bridge to 7.3: *"7.1 was password-based auth. 7.2 was per-user authorization. 7.3 puts the whole conversation on TLS — plus adds a second identity mechanism (client cert)."*

## Teardown

```bash
# Remove ACLs for the demo topic + group
$KAFKA/kafka-acls.sh --bootstrap-server $BS --remove \
  --allow-principal User:alice --topic secure-payments --force
$KAFKA/kafka-acls.sh --bootstrap-server $BS --remove \
  --allow-principal User:alice --group alice-analytics --force
$KAFKA/kafka-acls.sh --bootstrap-server $BS --remove \
  --allow-principal User:alice --cluster --force

# Delete the topic
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic secure-payments

# Leave the authorizer enabled for Sc 7.3.
# To disable entirely, remove the "Authorization" block from broker-*.properties and rolling restart.
```
