# Workshop 08 — JMX Exporter, Prometheus, Grafana, and Alerts

## Learning outcomes

- Explain the telemetry pipeline end to end.
- Prove raw JMX-derived metrics, successful Prometheus scrapes, PromQL results, dashboard provisioning, alert firing, and recovery.
- Separate “monitoring is up” from “Kafka is healthy.”

## Tool chain

- Kafka MBeans: metric source inside broker JVM.
- JMX Exporter Java agent: converts selected MBeans to Prometheus exposition.
- Prometheus: scrapes, stores, evaluates PromQL and alert rules.
- Grafana: queries Prometheus and renders the provisioned dashboard.

## Entry gate and install

```bash
./lab.sh preflight monitoring
./lab.sh setup-observability
```

- Installed native tools:
  - JMX Exporter 1.5.0;
  - Prometheus 3.13.0 LTS;
  - Grafana OSS 13.1.0.
- Prometheus/Grafana archives are SHA-256 verified.
- No Docker is used.

## Demo 1 — Instrument brokers at JVM startup

- Start the three regular ZooKeepers.
- Start brokers using:

```bash
./lab.sh start-monitored-broker broker1
./lab.sh start-monitored-broker broker2
./lab.sh start-monitored-broker broker3
```

- Real JVM pattern:

```text
-javaagent:jmx_prometheus_javaagent-1.5.0.jar=17071:workshop8/config/jmx/kafka.yml
```

- Exporter ports: `17071`, `17072`, `17073`.
- Raw proof:

```bash
curl -s http://127.0.0.1:17071/metrics | grep kafka_server_replicamanager | head
```

## Demo 2 — Start storage/query and visualization

```bash
./lab.sh start-prometheus
./lab.sh start-grafana
```

- Prometheus: <http://127.0.0.1:9090>
- Alerts: <http://127.0.0.1:9090/alerts>
- Grafana: <http://127.0.0.1:3000>
- Training login: `admin/admin`.
- Provisioned automatically:
  - Prometheus datasource;
  - “Kafka Administrator Overview” dashboard;
  - no manual UI setup required.

## Demo 3 — API proof before using the UI

```bash
./lab.sh monitoring-proof
```

- Expected proof:
  - Prometheus readiness succeeds;
  - three `up=1` series;
  - under-replicated partitions are zero;
  - Grafana database health is `ok`.
- Important: API proof makes the demo repeatable and avoids trusting a pretty dashboard alone.

## Demo 4 — Generate workload

```bash
./lab.sh create-topic orders 6
./lab.sh produce-sequence orders 100
```

- Show dashboard panels:
  - broker count;
  - message ingress rate;
  - JVM heap;
  - under-replicated partitions;
  - offline partitions.
- PromQL examples:

```promql
sum(up{job="kafka-brokers"})
rate(kafka_server_brokertopicmetrics_messagesinpersec_total[1m])
kafka_server_replicamanager_underreplicatedpartitions
jvm_memory_heap_used_bytes
```

## Demo 5 — Alert lifecycle proof

- Stop monitored Broker 2 with `Ctrl-C`.
- Wait longer than the configured 15-second `for` duration.
- Run:

```bash
./lab.sh monitoring-proof
./lab.sh health
```

- Expected proof:
  - Broker 2 exporter becomes `up=0`;
  - under-replicated partitions rise;
  - `KafkaBrokerDown` and under-replication alerts become pending/firing.
- Restart Broker 2.
- Wait for ISR recovery.
- Prove:
  - exporter returns to `up=1`;
  - ISR is complete;
  - alerts resolve.

## Participant challenge

- Find the exact alert expression and duration in `alerts.yml`.
- Change only the training duration to 30 seconds.
- Validate with `promtool` before restart.
- Explain why `up=1` alone cannot prove Kafka correctness.

## Must-know points

- Instrumentation, scraping, evaluation, visualization, and notification are separate layers.
- Monitor user impact and redundancy, not only processes.
- Consumer lag usually needs a lag exporter or application metric; broker JMX is insufficient.
- Secure exporters, Prometheus, Grafana, and credentials in production.
- Every alert needs severity, duration, owner, runbook, and tested resolution behavior.

## Original tools used by `lab.sh`

- Instrumented broker launch adds this JVM agent to the original Kafka server process:

```text
-javaagent:../common/observability/jmx_prometheus_javaagent-1.5.0.jar=17071:workshop8/config/jmx/kafka.yml
```

```bash
kafka-server-start.sh workshop1/config/broker1.properties
```

- Prometheus original binary and options:

```bash
prometheus \
  --config.file=workshop8/config/prometheus/prometheus.yml \
  --storage.tsdb.path=common/data/prometheus \
  --web.listen-address=127.0.0.1:9090
```

- Grafana original binary and important environment paths:

```bash
GF_PATHS_PROVISIONING=workshop8/config/grafana/provisioning \
GF_PATHS_DATA=common/data/grafana \
grafana server --homepath common/observability/grafana-13.1.0
```

- Configuration validation and API proof:

```bash
promtool check config workshop8/config/prometheus/prometheus.yml
curl -fsSG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=up{job="kafka-brokers"}'
curl -fsS http://127.0.0.1:3000/api/health
```
