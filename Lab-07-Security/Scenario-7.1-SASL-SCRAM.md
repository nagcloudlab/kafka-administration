# Scenario 7.1 — SASL/SCRAM authentication (who are you?)

**Prereqs**
- Labs 3 and 4 done.
- Cluster up: `./cluster.sh start-monitoring` in `kafka-lab/`.

**Paste at the top of every new terminal:**

```bash
cd ~/kafka-administration/kafka-lab
export KAFKA=./kafka/bin
export BS=localhost:9092,localhost:9093,localhost:9094
export ZK=localhost:2181
```

---

## Problem

- **The default cluster has no authentication.** Any TCP client that can reach broker ports can produce and consume. Every training cluster ships this way. Every corporate incident report begins the same way.
- **Kafka's built-in options:**
  - **SASL/PLAIN** — username + password sent in cleartext over the wire. Only safe over SSL.
  - **SASL/SCRAM-SHA-256 / SHA-512** — salted challenge-response. Passwords are hashed on the wire; a network sniff can't replay them. Users live in ZK (or the KRaft metadata log).
  - **SASL/GSSAPI (Kerberos)** — legacy, enterprise-heavy.
  - **SASL/OAUTHBEARER** — OAuth 2.0 tokens; needs an IdP.
  - **mTLS (SSL client certs)** — covered in 7.3.
- **SCRAM is the default recommendation for internal clusters:** stronger than PLAIN, no external dependencies (unlike Kerberos or OAuth), works out of the box.

- **Where SASL "lives" in the broker:**
  - A broker exposes one or more **listeners**, each with a name (e.g. `PLAINTEXT`, `SASL_PLAINTEXT`, `SSL`, `SASL_SSL`) and a port.
  - Each listener has a **security protocol** (via `listener.security.protocol.map`).
  - Inter-broker communication uses **one** of those listeners (chosen via `inter.broker.listener.name`).
  - You can safely add a SASL listener alongside an existing PLAINTEXT one — clients that hit the PLAINTEXT port still work; clients that hit the SASL port must authenticate.

- **How SCRAM users are stored:**
  - Under ZK: `/config/users/<username>` — a znode with SCRAM credentials JSON.
  - Managed by `kafka-configs.sh --alter --add-config 'SCRAM-SHA-256=[password=...]'`.
  - **Iteration count** (default 4096) controls the salt work factor. Bump to 8192+ for production; the load is one-off per user creation, not per-request.

- **Common misconfigurations:**
  - Only enabling SASL on one broker → clients hit round-robin, some connections fail. Enable on all brokers.
  - `SASL_PLAINTEXT` used across an untrusted network → passwords are hashed but the traffic itself isn't encrypted. Combine with TLS (`SASL_SSL`) for production.
  - Forgetting the JAAS config on the client → cryptic `SaslAuthenticationException: Authentication failed`.
  - Removing the old PLAINTEXT listener too early → any client that hasn't been rolled over stops working. Roll clients first, then remove PLAINTEXT.

## Symptom
- **Client without SASL config against the SASL listener** — client log: `Bootstrap broker localhost:9192 (id: -1 rack: null) disconnected` in a loop. Server-side broker log: `Failed authentication with /127.0.0.1 (Unexpected Kafka request of type METADATA during SASL handshake)`.
- **Client with wrong password** — `SaslAuthenticationException: Authentication failed: Invalid username or password`.
- **Broker after enabling SASL without a user existing yet** — nothing bad; the listener starts. But any auth attempt fails until users are created.

## Setup — 3 terminals

**Terminal 1 (control):**

```bash
# Back up broker configs before touching them
for id in 101 102 103; do
  cp config/broker-$id.properties config/broker-$id.properties.pre-security
done
```

**Terminal 2 (live listener view):**

```bash
watch -n 2 "ss -ltnp 2>/dev/null | grep -E ':(909[234]|919[234])\b'"
```

Baseline: only 9092/9093/9094 (PLAINTEXT).

**Terminal 3 (broker 101 log):**

```bash
tail -F logs/broker-101.log
```

## Trigger — Step 1: add SASL_PLAINTEXT listener to each broker

Paste in Terminal 1:

```bash
for id in 101 102 103; do
  sasl_port=$((id + 91))     # 101→192... wait: 101+91=192, so 9192 needs port math
  # Cleaner: SASL port = kafka port + 100
  kafka_port=$((9092 + id - 101))
  sasl_port=$((kafka_port + 100))    # 9192 / 9193 / 9194

  cat >> config/broker-$id.properties <<EOF

# ==== Security (Sc 7.1) ====
listeners=PLAINTEXT://:$kafka_port,SASL_PLAINTEXT://:$sasl_port
advertised.listeners=PLAINTEXT://localhost:$kafka_port,SASL_PLAINTEXT://localhost:$sasl_port
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT
inter.broker.listener.name=PLAINTEXT
sasl.enabled.mechanisms=SCRAM-SHA-256
EOF
done

# Sanity-check the additions
tail -8 config/broker-101.properties
```

- **Inter-broker stays on PLAINTEXT** — brokers-to-brokers don't need SASL creds. Simplifies the lab; in production you'd set `inter.broker.listener.name=SASL_PLAINTEXT` and provide broker JAAS creds.

## Trigger — Step 2: rolling restart with monitoring (from Sc 3.1)

```bash
for id in 101 102 103; do
  ./cluster.sh stop-broker              $id
  sleep 3
  ./cluster.sh start-broker-monitoring  $id
  sleep 5
done
```

## Observe
- **T2 (ports):** now also shows `*:9192`, `*:9193`, `*:9194` bound by java.
- **T3 (broker log):** for each listener, a line like:
  ```
  INFO [SocketServer listenerType=ZK_BROKER, nodeId=101] Started socket server acceptors and processors (kafka.network.SocketServer)
  ```
  followed by:
  ```
  INFO [KafkaServer id=101] started (kafka.server.KafkaServer)
  ```

- Existing PLAINTEXT clients keep working — the old listener is still bound.

## Trigger — Step 3: create SCRAM users

Paste in Terminal 1:

```bash
# Two demo users
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --alter --add-config 'SCRAM-SHA-256=[iterations=8192,password=alice-secret]' \
  --entity-type users --entity-name alice

$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --alter --add-config 'SCRAM-SHA-256=[iterations=8192,password=bob-secret]' \
  --entity-type users --entity-name bob

# Verify
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --describe --entity-type users
```

Expected: `alice` and `bob` listed with SCRAM-SHA-256 iterations.

You can also see them in ZK:

```bash
echo 'ls /config/users' | $KAFKA/zookeeper-shell.sh $ZK 2>/dev/null | tail -3
```

## Trigger — Step 4: build a client config file and try both success + failure

```bash
# Alice's client config
cat > /tmp/alice-client.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="alice" password="alice-secret";
EOF

# 1. SASL port WITHOUT auth config — expect failure
echo "hello anonymous" | \
  $KAFKA/kafka-console-producer.sh --bootstrap-server localhost:9192 --topic secure-test 2>&1 | head -6
```

Expected client output: repeated `disconnected while awaiting response` and eventually `TimeoutException`. Server side, `logs/broker-101.log` will show `Failed authentication`.

```bash
# 2. Create the topic (needs no auth — no ACLs yet)
$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --create --topic secure-test --partitions 3 --replication-factor 3

# 3. SASL port WITH auth config — expect success
echo "hello alice" | \
  $KAFKA/kafka-console-producer.sh --bootstrap-server localhost:9192 --topic secure-test \
  --producer.config /tmp/alice-client.properties

# 4. Consume it back — proves round-trip works
$KAFKA/kafka-console-consumer.sh --bootstrap-server localhost:9192 --topic secure-test \
  --from-beginning --timeout-ms 3000 \
  --consumer.config /tmp/alice-client.properties
```

## Observe
- Step 4.1 (no creds) — client hangs, log shows disconnects, eventual timeout.
- Step 4.3 (with creds) — produce succeeds silently.
- Step 4.4 (consume) — `hello alice` prints, then consumer exits after 3 s timeout.

## Solution

- **Two listeners is the safe migration pattern:**
  ```properties
  listeners=PLAINTEXT://:9092,SASL_PLAINTEXT://:9192
  advertised.listeners=PLAINTEXT://localhost:9092,SASL_PLAINTEXT://localhost:9192
  listener.security.protocol.map=PLAINTEXT:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT
  inter.broker.listener.name=PLAINTEXT       # or SASL_PLAINTEXT once you have broker creds
  sasl.enabled.mechanisms=SCRAM-SHA-256      # or SCRAM-SHA-512 for stronger hashing
  ```
  - Roll clients over to the SASL listener at their own pace.
  - Once every client is on SASL, remove the PLAINTEXT listener from broker configs and restart.

- **Production settings that this lab skipped:**
  - `SASL_SSL` (not `SASL_PLAINTEXT`) so passwords aren't just hashed but the whole conversation is encrypted.
  - `inter.broker.listener.name=SASL_SSL` with a broker-user (e.g. `admin`) and a JAAS config file passed via `KAFKA_OPTS=-Djava.security.auth.login.config=/path/broker_jaas.conf`.
  - Higher SCRAM iterations: `SCRAM-SHA-512=[iterations=16384,...]`.
  - Rotate user passwords on a schedule; there's no built-in expiry.

- **User management:**
  ```bash
  # Change alice's password
  $KAFKA/kafka-configs.sh --bootstrap-server $BS \
    --alter --add-config 'SCRAM-SHA-256=[password=new-secret]' \
    --entity-type users --entity-name alice

  # Delete alice entirely
  $KAFKA/kafka-configs.sh --bootstrap-server $BS \
    --alter --delete-config 'SCRAM-SHA-256' \
    --entity-type users --entity-name alice
  ```

- **Client library note:** SASL config in Java's `producer.properties`/`consumer.properties` uses `sasl.jaas.config` as a **single line**. Any linebreaks inside the value must be escaped. librdkafka (C/Python) uses a different shape: `sasl.username` and `sasl.password` as flat properties.

## Verify

```bash
# 1. SASL listener bound on all 3 brokers
ss -ltnp 2>/dev/null | grep -E ':919[234]\b' | wc -l
# Expected: 3

# 2. Users exist
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --describe --entity-type users | grep -E 'alice|bob'

# 3. Correct auth path works
echo "verify" | $KAFKA/kafka-console-producer.sh --bootstrap-server localhost:9192 \
  --topic secure-test --producer.config /tmp/alice-client.properties

# 4. Wrong-password path fails with a clean error
cat > /tmp/mallory.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="alice" password="WRONG";
EOF
echo "verify-wrong" | $KAFKA/kafka-console-producer.sh --bootstrap-server localhost:9192 \
  --topic secure-test --producer.config /tmp/mallory.properties 2>&1 | grep -i sasl | head -3
# Expected: SaslAuthenticationException: Authentication failed: ...
```

## Takeaway

> **Two listeners, one broker restart. SASL is add-on, not rip-and-replace — clients migrate at their own pace.**

## Instructor notes
- Poll before Step 4: *"If a broker has both a PLAINTEXT listener and a SASL listener, does a PLAINTEXT connection get rejected once we enable SASL?"* Most say yes. Demo shows no — the listeners are independent.
- The `Failed authentication` log line vs the client's confusing "disconnected" message is a great debugging story: server knows why, client only knows "connection dropped". Show them side-by-side.
- Iterations = work factor. Ask: *"Why would production want 16384 vs 4096?"* Answer: makes offline brute-force after a ZK snapshot leak harder.
- **kafka-clients library gotcha:** `sasl.jaas.config` must terminate with a semicolon `;`. Missing it → `Unable to parse the JAAS configuration`. Show the character explicitly.
- Bridge to 7.2: *"Alice can now log in — but can she do anything she wants? No ACL yet means she has full access. Next scenario locks that down."*

## Teardown

Leave the SASL config in place for Sc 7.2 — the next scenario needs it. Only clean the demo topic and users if you're stopping here:

```bash
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic secure-test

$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --alter --delete-config 'SCRAM-SHA-256' --entity-type users --entity-name alice
$KAFKA/kafka-configs.sh --bootstrap-server $BS \
  --alter --delete-config 'SCRAM-SHA-256' --entity-type users --entity-name bob

# Full revert (only if you're done with Lab 07 entirely):
# for id in 101 102 103; do
#   mv config/broker-$id.properties.pre-security config/broker-$id.properties
# done
# then rolling restart
```
