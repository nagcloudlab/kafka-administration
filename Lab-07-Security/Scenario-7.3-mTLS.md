# Scenario 7.3 — mTLS (encryption + cert-based auth on the wire)

**Prereqs**
- Sc 7.2 done — authorizer is enabled, ACL model understood.
- Cluster up.
- `openssl` and `keytool` (from JDK) available on PATH.

**Paste at the top of every new terminal:**

```bash
cd ~/kafka-administration/kafka-lab
export KAFKA=./kafka/bin
export BS=localhost:9092,localhost:9093,localhost:9094
export ZK=localhost:2181
```

---

## Problem

- **What TLS gives you:**
  - **Encryption on the wire** — passwords, message contents, all opaque to a sniffer. Required for anything crossing an untrusted network (WAN, VPC-peering, Kubernetes without service mesh).
  - **Server authentication** — client verifies it's talking to the *actual* broker, not a MITM. Done via cert chain to a trusted CA.
  - **Client authentication (the "m" in mTLS)** — broker verifies the client's identity via *client cert*, no password needed. Alternative or complement to SASL.

- **Kafka's TLS terminology (Java-centric):**
  - **Keystore** — a file (JKS or PKCS12) containing an entity's private key + its own cert (+ intermediate certs). Each broker has one; each client presenting a cert has one.
  - **Truststore** — a file containing the CAs (or specific certs) you trust to sign counterparty certs. Broker + all clients need one containing at least the CA that signed everyone.
  - **`ssl.client.auth`** — broker's expectation of the client cert:
    - `required` (mTLS) — client MUST present a cert; connection refused otherwise.
    - `requested` — client MAY present a cert; if present, verified.
    - `none` (default) — one-way TLS; only broker cert is validated.
  - **Principal from client cert:** by default, `User:<the-cert's-DN>` (e.g. `User:CN=alice,OU=training`). Customize with `ssl.principal.mapping.rules` if the DN is unwieldy.

- **Listener stacking (this lab's picture):**
  - `PLAINTEXT://:9092` — open, from Lab-01.
  - `SASL_PLAINTEXT://:9192` — SASL/SCRAM, from Sc 7.1.
  - `SSL://:9292` — mTLS, added here.
  - Later, remove the plaintext listeners entirely once every client has migrated.

- **Common misconfigurations / footguns:**
  - Cert's Common Name (CN) or SubjectAltName doesn't match the hostname the client uses → `SSLHandshakeException: unable to verify hostname`. Fix: match, or disable hostname verification client-side.
  - Broker keystore contains only the leaf cert (no intermediates) → client trust chain broken. Import CA into the broker keystore too.
  - Passwords in `.properties` files world-readable → `chmod 600` and consider `ssl.keystore.password.suppliers` in newer Kafka.
  - Cert expiry unnoticed → cluster silently starts refusing new connections at 00:00 on some future date. Alert on `days-until-expiry`.

## Symptom
- **Wrong CA in truststore:** `SSLHandshakeException: PKIX path building failed: unable to find valid certification path to requested target`.
- **Missing client cert with `ssl.client.auth=required`:** `SSLHandshakeException: Received fatal alert: bad_certificate`.
- **Hostname mismatch:** `SSLPeerUnverifiedException: Host name 'localhost' does not match the certificate subject`.
- **Expired cert:** `CertificateExpiredException`, brokers OR clients depending on which side expired.

## Setup — 3 terminals

**Terminal 1 (control):**

```bash
mkdir -p ./certs && cd ./certs
```

**Terminal 2 (broker log tail):**

```bash
tail -F logs/broker-101.log
```

**Terminal 3 (listener view):**

```bash
watch -n 2 "ss -ltnp 2>/dev/null | grep -E ':(909[234]|919[234]|929[234])\b'"
```

## Trigger — Step 1: generate a CA and per-broker + per-client certs

Big paste-and-run block. Runs in `./certs`. All passwords are `password` — training only, never do this in prod.

**Terminal 1 (in `./certs`):**

```bash
STOREPASS=password
VALIDITY=365

# ---------- 1. Root CA ----------
openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days $VALIDITY \
  -keyout ca.key -out ca.crt \
  -subj "/CN=kafka-lab-ca"

# ---------- 2. Truststore (contains CA — used by everyone) ----------
keytool -keystore truststore.jks -alias CARoot -import -file ca.crt \
  -storepass $STOREPASS -noprompt

# ---------- 3. Broker keystores ----------
for id in 101 102 103; do
  # 3a. Generate broker's own key + self-signed cert inside a keystore.
  #     Include SAN so hostname verification passes for localhost + broker-id name.
  keytool -keystore broker-$id.keystore.jks -alias broker-$id -validity $VALIDITY \
    -genkeypair -keyalg RSA -keysize 2048 \
    -dname "CN=broker-$id,OU=kafka-lab,O=training" \
    -ext "SAN=dns:localhost,dns:broker-$id,ip:127.0.0.1" \
    -storepass $STOREPASS -keypass $STOREPASS -noprompt

  # 3b. Export the broker's CSR
  keytool -keystore broker-$id.keystore.jks -alias broker-$id -certreq -file broker-$id.csr \
    -storepass $STOREPASS

  # 3c. Sign the CSR with the CA (preserve SAN via ext file)
  cat > broker-$id.ext <<EOF
subjectAltName=DNS:localhost,DNS:broker-$id,IP:127.0.0.1
EOF
  openssl x509 -req -in broker-$id.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out broker-$id.crt -days $VALIDITY -sha256 -extfile broker-$id.ext

  # 3d. Import CA + signed cert back into the broker keystore
  keytool -keystore broker-$id.keystore.jks -alias CARoot -import -file ca.crt \
    -storepass $STOREPASS -noprompt
  keytool -keystore broker-$id.keystore.jks -alias broker-$id -import -file broker-$id.crt \
    -storepass $STOREPASS -noprompt
done

# ---------- 4. Client keystore for alice ----------
#     Principal from this cert will be "User:CN=alice,OU=kafka-lab,O=training"
keytool -keystore alice.keystore.jks -alias alice -validity $VALIDITY \
  -genkeypair -keyalg RSA -keysize 2048 \
  -dname "CN=alice,OU=kafka-lab,O=training" \
  -storepass $STOREPASS -keypass $STOREPASS -noprompt

keytool -keystore alice.keystore.jks -alias alice -certreq -file alice.csr -storepass $STOREPASS
openssl x509 -req -in alice.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out alice.crt -days $VALIDITY -sha256
keytool -keystore alice.keystore.jks -alias CARoot -import -file ca.crt -storepass $STOREPASS -noprompt
keytool -keystore alice.keystore.jks -alias alice -import -file alice.crt -storepass $STOREPASS -noprompt

# ---------- 5. Sanity check ----------
ls -la *.jks
echo "--- broker-101 keystore contents ---"
keytool -list -keystore broker-101.keystore.jks -storepass $STOREPASS | head -15
```

- **What each file is for:**
  - `ca.crt / ca.key` — the training-only root CA. `ca.key` is the "if this leaks, redo everything" file.
  - `truststore.jks` — trust anchor for everyone (broker + client). One file, contains only the CA cert.
  - `broker-$id.keystore.jks` — broker's identity. Contains the broker's private key + signed cert + CA.
  - `alice.keystore.jks` — alice's identity. Contains alice's private key + signed cert + CA.

## Trigger — Step 2: add the SSL listener to each broker

Paste in Terminal 1 (back at `kafka-lab/`):

```bash
cd ..                 # back from ./certs to kafka-lab/
CERTS="$(pwd)/certs"

for id in 101 102 103; do
  kafka_port=$((9092 + id - 101))
  sasl_port=$((kafka_port + 100))
  ssl_port=$((kafka_port + 200))

  # Replace existing listeners= and add SSL bits
  # (Use a marker to make this idempotent — deletes any prior SSL block first)
  sed -i '/^# ==== TLS (Sc 7.3) ====$/,$d' config/broker-$id.properties

  cat >> config/broker-$id.properties <<EOF

# ==== TLS (Sc 7.3) ====
listeners=PLAINTEXT://:$kafka_port,SASL_PLAINTEXT://:$sasl_port,SSL://:$ssl_port
advertised.listeners=PLAINTEXT://localhost:$kafka_port,SASL_PLAINTEXT://localhost:$sasl_port,SSL://localhost:$ssl_port
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT,SSL:SSL

ssl.keystore.location=$CERTS/broker-$id.keystore.jks
ssl.keystore.password=password
ssl.key.password=password
ssl.truststore.location=$CERTS/truststore.jks
ssl.truststore.password=password
ssl.client.auth=required
EOF
done

tail -12 config/broker-101.properties
```

## Trigger — Step 3: rolling restart

```bash
for id in 101 102 103; do
  ./cluster.sh stop-broker              $id
  sleep 3
  ./cluster.sh start-broker-monitoring  $id
  sleep 5
done
```

## Observe
- **T3 (ports):** now also shows `*:9292`, `*:9293`, `*:9294` bound by java.
- **T2 (broker log):** listener-start line for SSL, plus:
  ```
  INFO SSL context initialized (org.apache.kafka.common.security.ssl.DefaultSslEngineFactory)
  ```

## Trigger — Step 4: connect an SSL client (alice)

Paste in Terminal 1:

```bash
cat > /tmp/alice-ssl.properties <<EOF
security.protocol=SSL
ssl.truststore.location=$(pwd)/certs/truststore.jks
ssl.truststore.password=password
ssl.keystore.location=$(pwd)/certs/alice.keystore.jks
ssl.keystore.password=password
ssl.key.password=password
# Hostname check off — our broker certs cover 'localhost' in SAN so this is optional,
# but disabling makes the training self-contained if a student uses a real IP.
ssl.endpoint.identification.algorithm=
EOF
```

## Trigger — Step 5: give alice an ACL (recall — deny-by-default from Sc 7.2)

Alice's principal via TLS is `User:CN=alice,OU=kafka-lab,O=training` — different from `User:alice` in Sc 7.2! Grant the same rights under the new principal:

```bash
ALICE="User:CN=alice,OU=kafka-lab,O=training"

$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --create --topic tls-payments --partitions 3 --replication-factor 3

$KAFKA/kafka-acls.sh --bootstrap-server $BS --add \
  --allow-principal "$ALICE" \
  --operation Write --operation Describe --operation Create \
  --topic tls-payments

$KAFKA/kafka-acls.sh --bootstrap-server $BS --add \
  --allow-principal "$ALICE" \
  --operation IdempotentWrite --cluster

$KAFKA/kafka-acls.sh --bootstrap-server $BS --add \
  --allow-principal "$ALICE" \
  --operation Read --topic tls-payments --group alice-tls-consumer
```

## Trigger — Step 6: end-to-end round trip over TLS

```bash
# Produce over SSL
echo "hello over TLS" | \
  $KAFKA/kafka-console-producer.sh --bootstrap-server localhost:9292 --topic tls-payments \
  --producer.config /tmp/alice-ssl.properties

# Consume over SSL
$KAFKA/kafka-console-consumer.sh --bootstrap-server localhost:9292 --topic tls-payments \
  --from-beginning --timeout-ms 3000 \
  --consumer.config /tmp/alice-ssl.properties \
  --group alice-tls-consumer
```

Expected: silent produce, `hello over TLS` on consume.

## Trigger — Step 7: prove client-cert requirement (mTLS)

Alice with cert works (Step 6). Try connecting with **only** the truststore, no keystore:

```bash
cat > /tmp/no-clientcert.properties <<EOF
security.protocol=SSL
ssl.truststore.location=$(pwd)/certs/truststore.jks
ssl.truststore.password=password
ssl.endpoint.identification.algorithm=
EOF

echo "no cert" | \
  $KAFKA/kafka-console-producer.sh --bootstrap-server localhost:9292 --topic tls-payments \
  --producer.config /tmp/no-clientcert.properties 2>&1 | head -6
```

Expected: `SSLHandshakeException: Received fatal alert: bad_certificate` — broker demands a cert (because `ssl.client.auth=required`), client didn't send one, TLS handshake fails.

Now try connecting to `localhost:9092` (PLAINTEXT) with an SSL config — protocol mismatch:

```bash
echo "wrong port" | \
  $KAFKA/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic tls-payments \
  --producer.config /tmp/alice-ssl.properties 2>&1 | head -6
```

Expected: `SocketException` / disconnect. The PLAINTEXT listener didn't speak TLS.

## Solution

- **Production wire-up (once TLS is working):**
  1. Change `inter.broker.listener.name` from `PLAINTEXT` to `SSL` and give brokers a client cert (the broker's own keystore can double as its client cert since it's signed by the same CA). Now inter-broker traffic is encrypted too.
  2. Remove the `PLAINTEXT://` listener entirely. Roll clients over first — the SASL and SSL listeners keep serving.
  3. Set `ssl.principal.mapping.rules=RULE:.*CN=([^,]+).*/$1/` to map `CN=alice,OU=...` down to just `User:alice`. Cleaner ACLs; matches what SASL uses.
  4. Move keystore/truststore passwords out of the properties file — Kafka supports `ssl.keystore.password.suppliers` for KMS-backed retrieval.

- **Cert rotation without downtime:**
  1. Generate the new cert signed by the same CA.
  2. Import it into the running broker's keystore (Java's `keytool` supports this).
  3. Call `kafka-configs.sh --entity-type brokers --entity-name N --alter --add-config 'ssl.keystore.type=<same>'` — this forces an SSL context refresh without restart. **The trigger is any dynamic ssl.* change on the broker config.**
  4. Old and new certs coexist under different aliases in the keystore until you remove the old.

- **Monitoring cert expiry:** JMX exposes `kafka.server:type=SslContextManager,name=<listener>-<version>-daysUntilExpiry`. Alert at 30 days out.

- **Choosing SASL vs TLS-client-auth:**
  - **SASL** (7.1): identity via username/password; easier user rotation; simple JAAS config.
  - **TLS mutual auth** (this scenario): identity via cert; no shared secret; harder rotation but more secure.
  - **Both at once** (`SASL_SSL` listener) — SASL identity is authoritative; TLS gives you encryption. This is the shape most production clusters end up in.

## Verify

```bash
# 1. SSL listener bound on all 3 brokers
ss -ltnp 2>/dev/null | grep -c -E ':929[234]\b'
# Expected: 3

# 2. Cert expiry (should be ~365 days out)
for id in 101 102 103; do
  echo -n "broker $id cert expiry: "
  keytool -list -v -keystore certs/broker-$id.keystore.jks -storepass password \
    -alias broker-$id | grep "Valid from" | head -1
done

# 3. mTLS round-trip
echo "verify-tls" | $KAFKA/kafka-console-producer.sh --bootstrap-server localhost:9292 \
  --topic tls-payments --producer.config /tmp/alice-ssl.properties

# 4. No-cert client is rejected
echo "no-cert-verify" | $KAFKA/kafka-console-producer.sh --bootstrap-server localhost:9292 \
  --topic tls-payments --producer.config /tmp/no-clientcert.properties 2>&1 | \
  grep -o 'bad_certificate\|SSLHandshake' | head -1
# Expected: SSLHandshake  (or bad_certificate)
```

## Takeaway

> **Encryption + identity in one protocol. Client cert is authoritative — TLS handshake fails before any Kafka request is sent.**

## Instructor notes
- Walk through Step 1 **one command at a time** — students who've never seen `keytool` need to see the file-by-file build-up (CA → keystore → CSR → signed cert → import back). Point out that the CA private key (`ca.key`) is the "keys to the kingdom" file.
- Draw the trust flow on the whiteboard: broker keystore ↔ CA ↔ client keystore, with the truststore acting as the "list of trusted signers" on both sides.
- Step 7's `bad_certificate` error is the entire scenario in one line — the broker refused the handshake, so no Kafka bytes were even exchanged. Contrast with 7.1 where SASL authentication happens *after* TCP connect.
- The **principal difference** between SASL and TLS (Step 5) trips everyone up. In production, `ssl.principal.mapping.rules` normalizes both to `User:alice`.
- **Never** ship this lab's certs to production — CA private key is on disk, passwords are literally `password`, and everything is signed for `localhost`. Say this out loud.
- Bridge to Lab 08: *"Everything so far ran on ZooKeeper. Lab 08 shows the modern replacement — KRaft — and how you migrate to it."*

## Teardown

```bash
# Delete the TLS demo topic + ACLs
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic tls-payments
ALICE="User:CN=alice,OU=kafka-lab,O=training"
$KAFKA/kafka-acls.sh --bootstrap-server $BS --remove \
  --allow-principal "$ALICE" --topic tls-payments --force
$KAFKA/kafka-acls.sh --bootstrap-server $BS --remove \
  --allow-principal "$ALICE" --group alice-tls-consumer --force
$KAFKA/kafka-acls.sh --bootstrap-server $BS --remove \
  --allow-principal "$ALICE" --cluster --force

# Optional: full revert of the security stack — see Lab-07-Security/README.md § "Full security-off reset"
```
