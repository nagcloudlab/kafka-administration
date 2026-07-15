# Scenario 11.1 — Prometheus alerting + AlertManager

**Prereqs**
- Labs 1, 2, and 3 done. Prometheus + Grafana stack from Lab-02 is up.
- Cluster on 9092–9094 running with monitoring.

**Paste at the top of every new terminal:**

```bash
cd ~/kafka-administration
export KAFKA=./kafka-lab/kafka/bin
export BS=localhost:9092,localhost:9093,localhost:9094
```

---

## Problem

- **A dashboard tells you what's happening now. An alert tells you when to wake someone up.** Those are different jobs. Trainees who "watch the dashboard for issues" learn that within a week no one's watching.

- **Prometheus's alerting model:**
  - **Alerting rules** are Prometheus expressions (e.g. `sum(kafka_server_replica_manager_underminisrpartitioncount) > 0`) with a `for:` clause (`for: 5m` = must be true for 5 straight minutes).
  - When true for the `for:` duration, Prometheus fires an **alert instance** into its Alerting API.
  - **AlertManager** is a separate service that receives alerts from Prometheus, groups them, dedupes, routes to receivers (Slack, PagerDuty, webhook, email), and manages silences and inhibitions.

- **Rule + AlertManager separation is intentional:** rules are dumb math on metrics; routing is a policy decision that shouldn't require redeploying Prometheus.

- **The four alert-quality categories for a Kafka cluster:**
  - **Page-worthy** (SLO breach, data loss, cluster down) — humans, right now. Small in number, always actioned.
    - `UnderMinIsrPartitionCount > 0` — producer writes with `acks=all` are failing.
    - `OfflinePartitionsCount > 0` — controller can't elect; some partitions unavailable.
    - `up{job="kafka"} == 0 for 2m` — a broker is gone.
    - `UncleanLeaderElectionsPerSec > 0` — a data-loss event just occurred.
  - **Ticket-worthy** (degraded but stable) — file a ticket, look at business hours.
    - `UnderReplicatedPartitions > 0 for 15m` — sustained URP; investigate.
    - `RequestHandlerAvgIdlePercent < 20% for 10m` — CPU pressure.
    - `ActiveControllerCount != 1` — brain split or controller dead.
  - **FYI** (trend info, capacity) — display, don't page.
    - Log size growth rate.
    - Consumer lag on non-critical groups.
  - **Delete-immediately** — noisy, low-signal, page-fatigue producers.
    - `UnderReplicatedPartitions > 0` (no `for` clause) — flaps constantly.
    - `BytesInPerSec > threshold` — traffic changes constantly; use only for capacity plans.

- **Common misconfigurations:**
  - Alerts with `for: 0s` or missing `for:` — flap; every rebalance fires an alert. Always give a `for:` window matching the metric's normal noise.
  - Routing everything to one receiver → alert fatigue → real ones missed. Separate `severity=critical` from `severity=warning` at the routing layer.
  - No inhibition rules — when a broker goes down (critical), you also get 50 URP warnings (redundant); inhibit URP when the broker-down critical is firing.
  - No silences documented — someone silences URP for a maintenance, forgets, six months later a real URP goes unnoticed.

## Symptom

Setup scenario. Once alerts are wired:

- **Correctly firing:** a scenario from Lab 03 (e.g. `kill -9` on a broker) → within 2 min, an alert appears in AlertManager UI → within seconds, the configured receiver (a webhook, or a `docker logs kafka-lab-alertmanager` line in this lab) shows the notification.
- **Broken wiring:** rules defined but no alerts fire → Prometheus `/rules` UI shows the rule but state stays `inactive`. Usually a syntax error or wrong metric name.
- **Alerts fire but no notification:** Prometheus → AlertManager plumbing broken. Check `up{job="alertmanager"}` in Prometheus.

## Setup — 3 terminals

**Terminal 1 (control):**

```bash
cd ~/kafka-administration/Lab-02-Monitoring-Setup
```

**Terminal 2 (AlertManager notification tail — leave running):**

```bash
# Will show all alerts routed by AM
docker logs -f kafka-lab-alertmanager 2>&1 | grep --line-buffered -E 'alert|receiver|notify' &
```

**Terminal 3 (webhook-receiver logs — leave running):**

```bash
docker logs -f kafka-lab-webhook-echo 2>&1 &
```

## Trigger — Step 1: write the alerting rules

**Terminal 1:**

```bash
mkdir -p prometheus-rules alertmanager

cat > prometheus-rules/kafka-alerts.yml <<'EOF'
groups:
  # ---------- Page-worthy (severity=critical) ----------
  - name: kafka-critical
    interval: 15s
    rules:
      - alert: KafkaBrokerDown
        expr: up{job="kafka"} == 0
        for: 2m
        labels: { severity: critical, team: kafka }
        annotations:
          summary: "Kafka broker {{ $labels.broker }} unreachable for 2 min"
          runbook: "https://runbooks.example.com/kafka-broker-down"

      - alert: KafkaOfflinePartitions
        expr: sum(kafka_controller_offlinepartitionscount) > 0
        for: 1m
        labels: { severity: critical, team: kafka }
        annotations:
          summary: "{{ $value }} partitions offline — controller can't elect a leader"

      - alert: KafkaUnderMinISR
        expr: sum(kafka_server_replica_manager_underminisrpartitioncount) > 0
        for: 2m
        labels: { severity: critical, team: kafka }
        annotations:
          summary: "{{ $value }} partitions below min.insync.replicas — producers with acks=all failing"

      - alert: KafkaNoControllerOrSplit
        expr: sum(kafka_controller_activecontrollercount) != 1
        for: 1m
        labels: { severity: critical, team: kafka }
        annotations:
          summary: "Active controllers = {{ $value }} (expected 1)"

      - alert: KafkaUncleanElection
        expr: rate(kafka_controller_unclean_leader_elections_total[5m]) > 0
        for: 1m
        labels: { severity: critical, team: kafka }
        annotations:
          summary: "An unclean leader election happened — silent data loss occurred"

  # ---------- Ticket-worthy (severity=warning) ----------
  - name: kafka-warning
    interval: 30s
    rules:
      - alert: KafkaSustainedURP
        expr: max(kafka_server_replica_manager_underreplicatedpartitions) > 0
        for: 15m
        labels: { severity: warning, team: kafka }
        annotations:
          summary: "URP > 0 sustained for 15 min on broker {{ $labels.broker }}"

      - alert: KafkaHighRequestQueue
        expr: max(kafka_network_requestqueuesize) > 20
        for: 5m
        labels: { severity: warning, team: kafka }
        annotations:
          summary: "Request queue size high ({{ $value }}) on broker {{ $labels.broker }} for 5 min"

      - alert: KafkaLowRequestHandlerIdle
        expr: avg(kafka_broker_request_handler_avg_idle_percent) < 0.20
        for: 10m
        labels: { severity: warning, team: kafka }
        annotations:
          summary: "Request handler idle {{ $value | humanizePercentage }} (< 20%) — CPU pressure"

      - alert: KafkaHighUnderMinISRRisk
        # About to breach min.ISR: cluster has ISR count = min.ISR for any partition
        expr: sum(kafka_server_replica_manager_atminisrpartitioncount) > 0
        for: 10m
        labels: { severity: warning, team: kafka }
        annotations:
          summary: "{{ $value }} partitions at min.ISR — one more failure = producer errors"
EOF

# Sanity check
grep -c 'alert:' prometheus-rules/kafka-alerts.yml
# Expected: 8-9 alerts
```

## Trigger — Step 2: write AlertManager config

```bash
cat > alertmanager/alertmanager.yml <<'EOF'
route:
  receiver: default-webhook
  group_by:      [alertname, cluster]
  group_wait:    10s      # aggregate multiple related alerts for 10s before first send
  group_interval: 5m      # follow-ups for the same group
  repeat_interval: 1h     # resend if the alert stays firing this long

  routes:
    # Critical alerts get their own routing (in prod: PagerDuty)
    - match: { severity: critical }
      receiver: critical-webhook
      group_wait: 5s       # page faster
      repeat_interval: 15m # nag more aggressively

receivers:
  - name: default-webhook
    webhook_configs:
      - url: 'http://webhook-echo:9099/warning'
        send_resolved: true

  - name: critical-webhook
    webhook_configs:
      - url: 'http://webhook-echo:9099/critical'
        send_resolved: true

# Inhibition: when a broker is down (critical), silence URP warnings — they'd all be for that broker
inhibit_rules:
  - source_matchers: [ alertname="KafkaBrokerDown" ]
    target_matchers: [ severity="warning" ]
    equal:           [ broker ]
EOF
```

## Trigger — Step 3: extend Lab-02's docker-compose with AlertManager and a webhook echo

Add two services to the existing `docker-compose.yml` (idempotent — check first):

```bash
# Only append if not already there
grep -q 'alertmanager:' docker-compose.yml || cat >> docker-compose.yml <<'EOF'

  alertmanager:
    image: prom/alertmanager:v0.27.0
    container_name: kafka-lab-alertmanager
    restart: unless-stopped
    ports: ["9099:9093"]                                  # 9099 on host to avoid broker 102 collision
    volumes:
      - ./alertmanager/alertmanager.yml:/etc/alertmanager/alertmanager.yml:ro

  webhook-echo:
    image: mendhak/http-https-echo:34
    container_name: kafka-lab-webhook-echo
    restart: unless-stopped
    ports: ["9088:9099"]                                  # for us to poke at
    environment:
      HTTP_PORT: 9099
      LOG_WITHOUT_NEWLINE: "true"
EOF
```

## Trigger — Step 4: teach Prometheus about the rules + scrape AlertManager

Extend `prometheus.yml.template` (source of truth):

```bash
grep -q 'rule_files:' prometheus.yml.template || \
  sed -i '/^global:$/a rule_files:\n  - /etc/prometheus/rules/*.yml\n\nalerting:\n  alertmanagers:\n    - static_configs:\n        - targets: [alertmanager:9093]\n' prometheus.yml.template

# Show the top of the file to confirm
head -20 prometheus.yml.template
```

Also mount the rules folder into the Prometheus container. Add to the prometheus service in `docker-compose.yml`:

```bash
# Adds the rules volume mount — idempotent check
grep -q 'prometheus-rules' docker-compose.yml || \
  sed -i '/- .\/prometheus.yml:\/etc\/prometheus\/prometheus.yml:ro/a\      - ./prometheus-rules:/etc/prometheus/rules:ro' docker-compose.yml
```

## Trigger — Step 5: bring everything up

```bash
./down.sh
./up.sh
sleep 5
docker compose ps
```

Expected: five containers — `kafka-lab-prometheus`, `kafka-lab-grafana`, `kafka-lab-alertmanager`, `kafka-lab-webhook-echo`, all `Up`.

Open in browser:
- **Prometheus rules** — <http://localhost:9090/rules>. All 8 alerts should be listed with state `inactive` (green).
- **Prometheus alerts** — <http://localhost:9090/alerts>. Empty (no firing alerts yet).
- **AlertManager UI** — <http://localhost:9099>. Empty. Configuration tab shows the routes and inhibits we wrote.

## Trigger — Step 6: fire a critical alert on purpose

The fastest way to trigger `KafkaBrokerDown`: kill a broker JVM.

```bash
cd ~/kafka-administration/kafka-lab
kill -9 $(cat pids/broker-102.pid)
```

**Observe over 2-4 minutes:**
- **T-0:** broker 102 down.
- **T+30 s:** Prometheus scrape fails; `up{job="kafka",broker="102"}` goes to 0. Rule `KafkaBrokerDown` enters state `pending` on Prometheus /alerts.
- **T+2 min:** the `for: 2m` expires; alert transitions to `firing`. Prometheus posts it to AlertManager.
- **T+2 min 5 s:** AlertManager processes it after `group_wait: 5s` (critical path). Routes to `critical-webhook`.
- **T3 (webhook-echo logs)** should show a POST like:
  ```json
  { "receiver": "critical-webhook", "status": "firing",
    "alerts": [{ "labels": { "alertname": "KafkaBrokerDown", "broker": "102", ... },
                  "annotations": { "summary": "Kafka broker 102 unreachable for 2 min" } }] }
  ```

Recover the broker:

```bash
rm -f pids/broker-102.pid
./cluster.sh start-broker-monitoring 102
```

- **T+30 s after start:** scrape succeeds; `up==1`. Rule goes back to `inactive`.
- AlertManager: since `send_resolved: true`, another webhook POST arrives with `"status": "resolved"`.

## Trigger — Step 7: fire a warning (with inhibition) to prove routing

Trigger `KafkaSustainedURP` — the fastest way is to throttle a follower (Sc 3.3):

```bash
cd ~/kafka-administration/kafka-lab

$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --create --topic alerting-demo --partitions 6 --replication-factor 3

$KAFKA/kafka-configs.sh --bootstrap-server $BS --entity-type brokers --entity-name 102 \
  --alter --add-config 'follower.replication.throttled.rate=1024'
$KAFKA/kafka-configs.sh --bootstrap-server $BS --entity-type topics --entity-name alerting-demo \
  --alter --add-config 'follower.replication.throttled.replicas=*'

# Produce hard so URP grows and STAYS
$KAFKA/kafka-producer-perf-test.sh --topic alerting-demo \
  --num-records 5000000 --record-size 1000 --throughput -1 \
  --producer-props bootstrap.servers=$BS acks=all &
```

Wait 15+ minutes (the `for: 15m` on `KafkaSustainedURP`). In practice, for a demo, edit the rule to `for: 3m`, restart Prometheus (auto-detects rule change every 15 s), then wait 3 min.

Once firing: **T3** shows a warning webhook POST. Notice the inhibition wasn't triggered (broker 102 isn't `KafkaBrokerDown`, just slow).

Cleanup:

```bash
$KAFKA/kafka-configs.sh --bootstrap-server $BS --entity-type brokers --entity-name 102 \
  --alter --delete-config 'follower.replication.throttled.rate'
$KAFKA/kafka-configs.sh --bootstrap-server $BS --entity-type topics --entity-name alerting-demo \
  --alter --delete-config 'follower.replication.throttled.replicas'
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic alerting-demo
kill %1 2>/dev/null
```

## Solution

- **Alert on symptoms, not causes.** `KafkaUnderMinISR` is a symptom (produces are failing). `KafkaBrokerDown` is a cause. Alert on both — but the on-call runbook links to different playbooks.

- **Give every alert `for:`** — matches the metric's normal noise. Guides:
  - URP: 15 min (transient after every reassignment).
  - Broker down: 2 min (allow one Prometheus scrape miss).
  - Unclean election: 1 min (rare enough that even a single event pages).
  - Handler idle: 10 min (bursty).

- **Route by severity, not by metric.** Add `labels: { severity: critical|warning }` on every rule; AlertManager routes on that.

- **Runbook link in every annotation.** The on-call at 3 AM shouldn't have to open a wiki search. `annotations.runbook` → link that starts with the fix, not the theory.

- **Inhibition prevents alert storms.** When `KafkaBrokerDown` fires for broker 102, silence any warning on that same broker. Add more inhibitions as you learn the correlations.

- **Silences for maintenance.** During a planned rolling restart:
  ```bash
  amtool silence add alertname=KafkaSustainedURP --duration=2h \
    --comment="planned rolling restart" \
    --alertmanager.url=http://localhost:9099
  ```

- **Test alerts regularly.** Once a quarter, kill a broker in a maintenance window; confirm the page reaches on-call. Untested alerts are indistinguishable from missing alerts.

- **SLO / burn-rate alerting** (next-level, not shown here): instead of "URP > 0", alert on "burning through the URP error budget faster than the SLO allows". Uses `rate()` over multiple windows (1h, 6h, 24h) to distinguish transient blips from real trouble. See the SRE Workbook (chapter 5) for the pattern; a full recipe is a lab of its own.

## Verify

```bash
# 1. Rules loaded
curl -s http://localhost:9090/api/v1/rules | python3 -c "
import json, sys
d = json.load(sys.stdin)
for g in d['data']['groups']:
    for r in g['rules']:
        print(f\"  {r['name']:<32s} state={r.get('state','—')}\")
"

# 2. Prometheus reaches AlertManager
curl -s http://localhost:9090/api/v1/alertmanagers | python3 -m json.tool | head -12

# 3. AlertManager config valid
curl -s http://localhost:9099/api/v2/status | python3 -m json.tool | head -20

# 4. Webhook echo received recent alerts
docker logs --tail 30 kafka-lab-webhook-echo 2>&1 | grep -i alertname | tail -3
```

## Takeaway

> **Rules are dumb, routing is policy. Alert on symptoms, use `for:` clauses, inhibit correlated noise, link a runbook — otherwise you build alert fatigue instead of observability.**

## Instructor notes
- After Step 6, walk through the Prometheus /alerts and AlertManager UIs side by side. Show the alert lifecycle: `inactive → pending (during for:) → firing → resolved`.
- Ask before triggering: *"If I have 100 topics on a broker and that broker dies, how many alerts fire?"* Answer: 1 (the broker-down) plus 100 URP (unless inhibited). Show the inhibit config as the answer.
- Real-world story: a customer's URP alert had no `for:` clause. Every replica reassignment fired 50 pages in 30 seconds. On-call ignored the topic for 6 weeks, then missed a real failure. Show the `for: 15m` line as the fix.
- Bridge to 11.2: *"You now alert on cluster health. But consumers falling behind is invisible from broker JMX alone — you need a lag exporter."*

## Teardown

Leave the alerting stack up if you're moving on to 11.2 — 11.2 adds a lag exporter and a lag alert. Otherwise:

```bash
cd ~/kafka-administration/Lab-02-Monitoring-Setup
./down.sh
# Remove the extensions we added
git checkout docker-compose.yml prometheus.yml.template 2>/dev/null || \
  echo "(git tracked or hand-edit as needed)"
rm -rf prometheus-rules alertmanager
./up.sh   # back to Lab-02 baseline
```
