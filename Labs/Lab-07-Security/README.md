# Lab 07 — Security (Authentication, Authorization, Encryption)

Turns the open training cluster into a secured one — the three concepts an Ops admin will actually be asked to configure on day one.

## Scenarios (do in order — each builds on the previous)

- [7.1 — SASL/SCRAM authentication (who are you?)](Scenario-7.1-SASL-SCRAM.md)
- [7.2 — ACLs (what may you do?)](Scenario-7.2-ACLs.md)
- [7.3 — mTLS (encryption + cert-based auth on the wire)](Scenario-7.3-mTLS.md)

The scenarios stack — 7.2 assumes SASL is on (from 7.1); 7.3 assumes ACLs are in place (from 7.2). Skip them and half the commands won't do anything visible.

## Common prereqs

- Labs 3 and 4 complete (understanding of broker restart + `min.ISR` semantics).
- 3-broker ZK cluster up: `./cluster.sh start-monitoring` in `kafka-lab/`.

Paste at the top of every new terminal:

```bash
cd ~/kafka-administration/kafka-lab
export KAFKA=./kafka/bin
export BS=localhost:9092,localhost:9093,localhost:9094
export ZK=localhost:2181
```

## What each scenario changes on the cluster

| Sc | Files touched | Cluster state after |
|----|---------------|--------------------|
| 7.1 | `config/broker-*.properties`, ZK `/config/users/*` | Additional SASL_PLAINTEXT listener on 9192/9193/9194; SCRAM users `admin` + `alice` + `bob` |
| 7.2 | Broker config gets `authorizer.class.name`; ACLs stored in ZK | Only ACL-authorised principals can produce/consume |
| 7.3 | New SSL listener on 9292/9293/9294; broker keystore + truststore | Client cert or SASL required — anonymous plaintext rejected |

## Full security-off reset (revert everything)

```bash
./cluster.sh stop
# Restore the original broker properties (they were backed up per scenario)
for id in 101 102 103; do
  [[ -f config/broker-$id.properties.pre-security ]] && \
    mv config/broker-$id.properties.pre-security config/broker-$id.properties
done
rm -rf ./data/*
mkdir -p ./data/zookeeper ./data/broker-101 ./data/broker-102 ./data/broker-103
./cluster.sh start
```

## Not covered in Lab 07

- **OAuth2 / OIDC token auth** — SASL/OAUTHBEARER exists but needs an IdP; separate lab.
- **Kerberos (GSSAPI)** — real deployments use SCRAM or OAuth these days.
- **Per-user quotas** — briefly touched; the mechanics are the same as Lab 06 (client-id quotas) once you have authenticated users.
- **Automatic cert rotation** — production concern; out of scope for a first-time training.
