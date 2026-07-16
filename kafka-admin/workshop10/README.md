# Workshop 10 — TLS, SASL Authentication, ACL Authorization, and Rotation

## Learning outcomes

- Prove encryption, broker identity, user authentication, deny-by-default authorization, least-privilege ACLs, and recovery from denied access.
- Finish with certificate and credential rotation runbooks.
- Keep security last so participants understand the unsecured baseline first.

## Security model

- TLS: encrypts traffic and verifies server identity.
- SASL/PLAIN: authenticates username/password; safe here only because it runs inside TLS.
- ACLs: authorize the authenticated principal for resource operations.
- Authentication answers “who are you?”; authorization answers “what may you do?”

## Entry gate

- Stop normal brokers; keep the three ZooKeepers running.

```bash
./lab.sh preflight security
./lab.sh setup-security
./lab.sh cert-info
```

- Generated training assets:
  - local CA and private key;
  - one unique PKCS12 broker keystore per broker;
  - shared client truststore;
  - certificates with `localhost` and `127.0.0.1` SANs.
- Real tools:
  - `openssl req` and `openssl x509`;
  - `keytool -genkeypair`, `-certreq`, and `-importcert`.
- Expected certificate proof:
  - issuer is `Kafka Training CA`;
  - SAN matches the listener address;
  - validity dates are current.

## Demo 1 — Start secured brokers

```bash
./lab.sh start-secure-broker broker1
./lab.sh start-secure-broker broker2
./lab.sh start-secure-broker broker3
```

- Broker configuration proof:
  - listener is `SASL_SSL`;
  - inter-broker protocol is secured;
  - mechanism is PLAIN over TLS;
  - hostname verification remains enabled;
  - `AclAuthorizer` is active;
  - `allow.everyone.if.no.acl.found=false` denies by default;
  - only training admin/broker are super users.

## Demo 2 — Authentication succeeds, authorization fails

- Create the topic as admin:

```bash
./lab.sh secure-create-topic secure-orders
```

- Attempt producer access before ACL grant:

```bash
./lab.sh secure-produce secure-orders
```

- Expected proof: TLS and authentication succeed, then Kafka returns topic authorization failure.
- Teaching value: failure location distinguishes trust, authentication, and authorization problems.

## Demo 3 — Least-privilege grant

```bash
./lab.sh secure-grant secure-orders secure-training-group
./lab.sh secure-acls
```

- Producer receives only topic `Write` and `Describe`.
- Consumer receives topic `Read/Describe` plus group `Read`.
- Real tool:

```text
kafka-acls.sh --command-config workshop10/config/security/admin.properties --add --allow-principal User:producer --operation Write --operation Describe --topic secure-orders
```

## Demo 4 — Authorized data flow proof

- Terminal A:

```bash
./lab.sh secure-consume secure-orders secure-training-group
```

- Terminal B:

```bash
./lab.sh secure-produce secure-orders
```

- Enter numbered messages.
- Expected proof: authorized producer writes and authorized group reads.
- Negative proof: producer identity still cannot consume because no read/group ACL was granted.

## Demo 5 — TLS troubleshooting ladder

- Trust failure: wrong/missing CA truststore.
- Identity failure: certificate SAN does not match listener hostname/IP.
- Authentication failure: wrong SASL user/password.
- Authorization failure: valid identity lacks required resource operation.
- Diagnose in that order; do not disable hostname verification to hide SAN errors.

## Certificate rotation runbook

- Generate new CA/certificates before expiry.
- Add new CA trust while retaining old trust.
- Roll broker certificates one node at a time.
- Migrate clients and verify handshake success.
- Prove old CA is unused.
- Remove old trust in a later controlled change.

## Credential rotation runbook

- Introduce new credential while old remains valid when supported.
- Update clients through secret management.
- Measure adoption and authentication errors.
- Remove old credential after verification.
- Static JAAS in this lab requires broker rolls and is intentionally simplified.

## Participant challenge

- Explain why the pre-ACL producer failure proves authentication worked.
- Identify every permission needed by the consumer and why group ACL is separate.
- Inspect a certificate and verify SAN/issuer/expiry.
- Design a zero-downtime CA rotation sequence.

## Must-know points

- Never reuse these visible training passwords, keystores, or CA in production.
- Protect private keys and client configs with strict permissions and secret management.
- Avoid wildcard ACLs and unnecessary super users.
- Secure client, inter-broker, controller, and ZooKeeper paths as applicable.
- Monitor certificate expiry, handshake failures, authentication failures, and authorization denials.

## Original tools used by `lab.sh`

- The following shows the original tool sequence; generated file paths and training passwords are fully visible in `../common/native-lab.sh`.

- Training CA, signing request, certificate signing, and keystore imports:

```bash
openssl req -new -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout ca.key -out ca.crt -subj '/CN=Kafka Training CA'
keytool -genkeypair -alias broker -keyalg RSA -keysize 2048 \
  -keystore broker1.p12 -storetype PKCS12
keytool -certreq -alias broker -keystore broker1.p12 -file broker1.csr
openssl x509 -req -in broker1.csr -CA ca.crt -CAkey ca.key -out broker1.crt
keytool -importcert -alias broker -file broker1.crt -keystore broker1.p12
```

- Secured broker and admin commands:

```bash
kafka-server-start.sh workshop10/config/security/broker1.properties
kafka-topics.sh --bootstrap-server SECURE_BROKERS \
  --command-config workshop10/config/security/admin.properties \
  --create --topic secure-orders --partitions 6 --replication-factor 3
kafka-acls.sh --bootstrap-server SECURE_BROKERS \
  --command-config workshop10/config/security/admin.properties \
  --add --allow-principal User:producer --operation Write --operation Describe \
  --topic secure-orders
```

- Secured clients:

```bash
kafka-console-producer.sh --bootstrap-server SECURE_BROKERS --topic secure-orders \
  --producer.config workshop10/config/security/producer.properties --producer-property acks=all
kafka-console-consumer.sh --bootstrap-server SECURE_BROKERS --topic secure-orders \
  --group secure-training-group --consumer.config workshop10/config/security/consumer.properties \
  --from-beginning
```

- `SECURE_BROKERS` means ports `19092–19094` using `SASL_SSL` client properties.
