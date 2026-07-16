# Scenario 11.2 — Consumer group lag monitoring

**Prereqs**
- Lab-02 stack up (Prometheus + Grafana). Sc 11.1 recommended (alerting).
- Cluster on 9092–9094 running with monitoring.

**Paste at the top of every new terminal:**

```bash
cd ~/kafka-administration
export KAFKA=./kafka-lab/kafka/bin
export BS=localhost:9092,localhost:9093,localhost:9094
```

---

## Problem

- **Consumer lag is the metric that predicts angry downstream calls.** It's the difference between the latest offset written to a partition and the offset the consumer group has committed. Growing lag = the consumer is falling behind = downstream systems will notice.

- **Why the broker's JMX doesn't give you this directly:**
  - Brokers know the current log-end-offset per partition.
  - Brokers know the last-committed offset per consumer group (stored in `__consumer_offsets`).
  - But Kafka's shipped JMX beans expose these separately, per broker, in a form that's hard to correlate without a custom scraper.
  - The pragmatic answer: run a small sidecar that scrapes both, computes lag, and exports as Prometheus metrics.

- **The de-facto Prometheus exporter for Kafka:** [`kafka_exporter`](https://github.com/danielqsj/kafka_exporter) (community; also known as danielqsj/kafka_exporter). Runs as one container per cluster, reads `__consumer_offsets`, exposes:
  - `kafka_consumergroup_current_offset{consumergroup, topic, partition}` — where the group is
  - `kafka_consumergroup_lag{consumergroup, topic, partition}` — records behind (i.e., HWM − committed)
  - `kafka_consumergroup_lag_sum{consumergroup, topic}` — sum across partitions of one topic
  - `kafka_topic_partition_current_offset{topic, partition}` — the HWM
  - Plus broker/topic-level counts.

- **Alternatives:**
  - **`kafka-lag-exporter`** (Lightbend/Kafka-Streams-based, richer): tracks lag *in seconds* (how many seconds behind the head the consumer is), not just records. More accurate for capacity planning. Heavier.
  - **Confluent Metrics Reporter** — Confluent Platform only.
  - **Homegrown**: shell scripts polling `kafka-consumer-groups.sh --describe` and pushing to StatsD. Ugly but works.

- **What to alert on:**
  - `kafka_consumergroup_lag_sum > <threshold> for 10m` — group `X` is stuck.
  - `rate(kafka_consumergroup_current_offset[5m]) == 0 for 10m` — group is not committing (crashed or stalled).
  - `kafka_consumergroup_lag > <records> AND kafka_consumergroup_members == 0` — group has no live members and there's a backlog. Consumer service is down.

- **Common misconfigurations:**
  - Exporter running on the same host as brokers but without `--kafka.server=...` pointing at any live broker → useless.
  - Group filter (`--group.filter=`) excluding your production groups by accident → alerts silently miss them.
  - Alerting on absolute lag without accounting for throughput — a 1M-record lag on a firehose topic (100k/s) is 10 s behind; the same lag on a slow topic (100/s) is 10000 s behind. Alert on **time**-based lag (kafka-lag-exporter) if you can.

## Symptom

- **Working consumer, working exporter:** lag oscillates around a small number; sum-lag stays flat over hours.
- **Consumer stuck / crashed:** lag climbs monotonically; `current_offset` doesn't move.
- **No committed offset yet:** exporter shows the group but with `-1` current offset (or omits it) — group hasn't committed.
- **Exporter can't reach broker:** metrics missing entirely from the `/metrics` endpoint. `up{job="kafka-exporter"}` == 0 in Prometheus.

## Setup — 4 terminals

**Terminal 1 (control):**

```bash
cd ~/kafka-administration/Lab-02-Monitoring-Setup
```

**Terminal 2 (kafka-exporter log tail):**

```bash
# Filled in after Step 1
```

**Terminal 3 (consumer 1 — will run, then intentionally stall):**

Placeholder.

**Terminal 4 (Prometheus lag probe — leave running):**

```bash
watch -n 3 '
curl -sG http://localhost:9090/api/v1/query --data-urlencode "query=sum by (consumergroup) (kafka_consumergroup_lag)" | \
  python3 -c "
import json, sys
d = json.load(sys.stdin)
for r in d[\"data\"][\"result\"]:
    print(f\"  group {r[\\\"metric\\\"].get(\\\"consumergroup\\\",\\\"?\\\"):<25s} lag = {int(float(r[\\\"value\\\"][1]))}\")
if not d[\"data\"][\"result\"]:
    print(\"  no groups reporting yet\")"
'
```

## Trigger — Step 1: add kafka-exporter to the docker-compose stack

**Terminal 1:**

```bash
cd ~/kafka-administration/Lab-02-Monitoring-Setup

# The exporter needs to reach the brokers. On WSL2 Docker Desktop we use the WSL host IP
# (same trick Lab-02's up.sh uses). We already generated prometheus.yml with the IP;
# reuse it for exporter args.
HOST_IP=$(grep -m1 -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' prometheus.yml)
echo "using HOST_IP=$HOST_IP"

# Append the exporter service if not already present
grep -q 'kafka-exporter:' docker-compose.yml || cat >> docker-compose.yml <<EOF

  kafka-exporter:
    image: danielqsj/kafka-exporter:v1.9.0
    container_name: kafka-lab-kafka-exporter
    restart: unless-stopped
    ports: ["9308:9308"]
    command:
      - --kafka.server=$HOST_IP:9092
      - --kafka.server=$HOST_IP:9093
      - --kafka.server=$HOST_IP:9094
      - --topic.filter=.*
      - --group.filter=.*
      - --log.level=info
EOF
```

## Trigger — Step 2: scrape it from Prometheus

Edit `prometheus.yml.template` to add a scrape job:

```bash
grep -q 'kafka-exporter' prometheus.yml.template || \
  cat >> prometheus.yml.template <<'EOF'

  - job_name: kafka-exporter
    static_configs:
      - targets: ['kafka-exporter:9308']
EOF

tail -5 prometheus.yml.template
```

## Trigger — Step 3: bring up the extended stack

```bash
./down.sh
./up.sh
sleep 8
docker compose ps kafka-exporter
```

**Terminal 2:**

```bash
docker logs -f kafka-lab-kafka-exporter 2>&1 | grep --line-buffered -E 'INFO|Kafka|group'
```

Expected: startup lines showing it connected to `9092/9093/9094`, discovered brokers, and started scraping consumer groups.

Verify the /metrics endpoint:

```bash
curl -s http://localhost:9308/metrics | grep -E '^kafka_consumergroup_lag ' | head -5
curl -s http://localhost:9308/metrics | grep -E '^kafka_' | wc -l
# Expected: 20+ metrics
```

Verify Prometheus is scraping:

```bash
curl -s http://localhost:9090/api/v1/targets | python3 -c "
import json,sys
for t in json.load(sys.stdin)['data']['activeTargets']:
    if t['labels'].get('job') == 'kafka-exporter':
        print(f\"  scrape URL {t['scrapeUrl']}  ->  {t['health']}\")
"
```

## Trigger — Step 4: produce data + start a well-behaved consumer

**Terminal 1:**

```bash
cd ~/kafka-administration/kafka-lab

$KAFKA/kafka-topics.sh --bootstrap-server $BS \
  --create --topic lag-demo --partitions 6 --replication-factor 3

# Slow steady producer
$KAFKA/kafka-producer-perf-test.sh --topic lag-demo \
  --num-records 10000000 --record-size 200 --throughput 500 \
  --producer-props bootstrap.servers=$BS acks=all &
PROD_PID=$!
```

**Terminal 3 (healthy consumer):**

```bash
$KAFKA/kafka-console-consumer.sh --bootstrap-server $BS --topic lag-demo \
  --group lag-demo-app --property enable.auto.commit=true \
  --property auto.commit.interval.ms=1000 > /dev/null
```

**Terminal 4 (probe)** — within ~10 s shows `group lag-demo-app  lag = <small number>`. If consumer keeps up with 500 rec/s, lag oscillates near 0.

## Trigger — Step 5: fake a stall — pause the consumer

**Find and SIGSTOP the consumer (like Sc 6.1's rebalance trigger):**

```bash
# In a spare terminal
CPID=$(pgrep -f 'lag-demo-app' | head -1)
echo "consumer PID: $CPID"
kill -STOP $CPID
```

Producer keeps writing. Consumer is frozen.

**Observe over 1-2 min:**
- **T4 probe:** `lag` climbs — hundreds, thousands, tens of thousands.
- **Grafana Explore** — `sum by (consumergroup) (kafka_consumergroup_lag)` shows a rising line.
- **Prometheus** — you can build an alert:
  ```
  sum by (consumergroup) (kafka_consumergroup_lag) > 5000 for 5m
  ```
  (Add to `prometheus-rules/kafka-alerts.yml` from Sc 11.1 for a real page.)

**Recover:**

```bash
kill -CONT $CPID
# Watch T4 — lag drops rapidly as the consumer catches up.
```

## Trigger — Step 6: consumer completely dies (worst case)

```bash
# Kill the consumer entirely
kill $CPID
```

**Observe:**
- Lag keeps growing (producer still writing, no consumer at all).
- `kafka_consumergroup_members` for `lag-demo-app` drops to 0 within `session.timeout.ms`.
- Alert candidate:
  ```
  (sum by (consumergroup) (kafka_consumergroup_lag) > 1000)
    and
  (max by (consumergroup) (kafka_consumergroup_members) == 0)
  for: 5m
  ```
  → "consumer group has lag AND no members — the app is down."

## Trigger — Step 7: import a lag dashboard into Grafana

Add a minimal dashboard so trainees don't fight Explore syntax:

```bash
cd ~/kafka-administration/Lab-02-Monitoring-Setup

cat > grafana-provisioning/dashboards/kafka-consumer-lag.json <<'EOF'
{
  "id": null, "uid": "kafka-consumer-lag", "title": "Kafka - Consumer Lag",
  "tags": ["kafka","training","lag"], "timezone": "browser", "schemaVersion": 39, "version": 1,
  "refresh": "5s", "time": { "from": "now-15m", "to": "now" }, "editable": true, "graphTooltip": 1,
  "annotations": {"list": []}, "templating": {"list": []},
  "panels": [
    { "id": 1, "type": "stat", "title": "Total records lag (cluster)",
      "gridPos": {"h":4,"w":8,"x":0,"y":0},
      "datasource": {"type":"prometheus","uid":"prometheus"},
      "targets": [{"refId":"A","expr":"sum(kafka_consumergroup_lag)",
        "datasource":{"type":"prometheus","uid":"prometheus"}}],
      "fieldConfig": {"defaults":{"unit":"short","color":{"mode":"thresholds"},
        "thresholds":{"mode":"absolute","steps":[
          {"value":null,"color":"green"},{"value":10000,"color":"yellow"},{"value":100000,"color":"red"}]}}},
      "options": {"colorMode":"background","graphMode":"none","textMode":"auto",
        "reduceOptions":{"calcs":["lastNotNull"],"fields":"","values":false}}
    },
    { "id": 2, "type": "stat", "title": "Groups with members = 0 (dead)",
      "gridPos": {"h":4,"w":8,"x":8,"y":0},
      "datasource": {"type":"prometheus","uid":"prometheus"},
      "targets": [{"refId":"A","expr":"count(kafka_consumergroup_members == 0)",
        "datasource":{"type":"prometheus","uid":"prometheus"}}],
      "fieldConfig": {"defaults":{"unit":"short","color":{"mode":"thresholds"},
        "thresholds":{"mode":"absolute","steps":[{"value":null,"color":"green"},{"value":1,"color":"red"}]}}},
      "options": {"colorMode":"background","graphMode":"none","textMode":"auto",
        "reduceOptions":{"calcs":["lastNotNull"],"fields":"","values":false}}
    },
    { "id": 3, "type": "stat", "title": "Active groups",
      "gridPos": {"h":4,"w":8,"x":16,"y":0},
      "datasource": {"type":"prometheus","uid":"prometheus"},
      "targets": [{"refId":"A","expr":"count(count by (consumergroup)(kafka_consumergroup_lag))",
        "datasource":{"type":"prometheus","uid":"prometheus"}}],
      "fieldConfig": {"defaults":{"unit":"short"}},
      "options": {"colorMode":"value","graphMode":"none","textMode":"auto",
        "reduceOptions":{"calcs":["lastNotNull"],"fields":"","values":false}}
    },
    { "id": 4, "type": "timeseries", "title": "Lag per group",
      "gridPos": {"h":10,"w":24,"x":0,"y":4},
      "datasource": {"type":"prometheus","uid":"prometheus"},
      "targets": [{"refId":"A","expr":"sum by (consumergroup) (kafka_consumergroup_lag)",
        "legendFormat":"{{consumergroup}}",
        "datasource":{"type":"prometheus","uid":"prometheus"}}],
      "fieldConfig": {"defaults":{"unit":"short","color":{"mode":"palette-classic"},
        "custom":{"drawStyle":"line","lineInterpolation":"smooth","lineWidth":2,"fillOpacity":15}}},
      "options": {"legend":{"displayMode":"table","placement":"right","showLegend":true,
          "calcs":["last","max"]},"tooltip":{"mode":"multi","sort":"desc"}}
    },
    { "id": 5, "type": "timeseries", "title": "Lag per (group, topic, partition)",
      "gridPos": {"h":10,"w":24,"x":0,"y":14},
      "datasource": {"type":"prometheus","uid":"prometheus"},
      "targets": [{"refId":"A","expr":"topk(20, kafka_consumergroup_lag)",
        "legendFormat":"{{consumergroup}} / {{topic}} / p{{partition}}",
        "datasource":{"type":"prometheus","uid":"prometheus"}}],
      "fieldConfig": {"defaults":{"unit":"short","color":{"mode":"palette-classic"},
        "custom":{"drawStyle":"line","lineInterpolation":"smooth","lineWidth":1,"fillOpacity":10}}},
      "options": {"legend":{"displayMode":"list","placement":"bottom","showLegend":true},
        "tooltip":{"mode":"multi","sort":"desc"}}
    }
  ]
}
EOF
```

Grafana's file watcher picks it up within 30 s. Open at:

<http://localhost:3000/d/kafka-consumer-lag/kafka-consumer-lag>

## Solution

- **Deploy pattern:** one exporter per Kafka cluster (per environment). Cheap (~50 MB RAM, negligible CPU). Point Prometheus at it as another scrape job.

- **Alert rules that survive prod:**
  ```yaml
  - alert: KafkaConsumerGroupHighLag
    expr: sum by (consumergroup) (kafka_consumergroup_lag) > 10000
    for: 10m
    labels: { severity: warning, team: kafka }
    annotations:
      summary: "Group {{ $labels.consumergroup }} lag = {{ $value }} for 10m"

  - alert: KafkaConsumerGroupNoMembersButLag
    expr: (sum by (consumergroup) (kafka_consumergroup_lag) > 100)
        and
          (max by (consumergroup) (kafka_consumergroup_members) == 0)
    for: 5m
    labels: { severity: critical, team: kafka }
    annotations:
      summary: "Group {{ $labels.consumergroup }} has lag but no members — consumer app is down"

  - alert: KafkaConsumerStalled
    expr: rate(kafka_consumergroup_current_offset[10m]) == 0
        and
          sum by (consumergroup, topic) (kafka_topic_partition_current_offset > 0)
    for: 15m
    labels: { severity: warning, team: kafka }
    annotations:
      summary: "Group {{ $labels.consumergroup }} not committing on topic {{ $labels.topic }} for 15m"
  ```

- **Time-based lag** (records/sec × lag records ≈ seconds behind) — a quick derived metric:
  ```
  # PromQL — approximate seconds behind, per group
  sum by (consumergroup) (kafka_consumergroup_lag)
    /
  sum by (consumergroup) (rate(kafka_topic_partition_current_offset[5m]))
  ```
  Alert on that in units the business understands: "orders-processor is 90 seconds behind".

- **Consumer team ownership** — put a `team=` label on each group in the exporter config (or via relabel_config in Prometheus). Now the alert route from Sc 11.1 can go to the RIGHT team, not just "kafka".

- **Retention of lag metrics** — historic lag helps root-cause after the fact (was the group already lagging before the deploy?). Bump `--storage.tsdb.retention.time=7d` in Prometheus if disk allows.

## Verify

```bash
# 1. Exporter running
curl -sf http://localhost:9308/metrics > /dev/null && echo "exporter OK" || echo "DOWN"

# 2. Prometheus is scraping it
curl -s http://localhost:9090/api/v1/query --data-urlencode 'query=up{job="kafka-exporter"}' | \
  python3 -c "import json,sys; d=json.load(sys.stdin); print('scrape up =', d['data']['result'][0]['value'][1] if d['data']['result'] else 'no data')"

# 3. Groups are being reported
curl -sG http://localhost:9090/api/v1/query --data-urlencode 'query=count(count by (consumergroup)(kafka_consumergroup_lag))' | \
  python3 -c "import json,sys; d=json.load(sys.stdin); print('groups seen =', d['data']['result'][0]['value'][1] if d['data']['result'] else 0)"

# 4. Grafana knows about the dashboard
curl -s -u admin:admin "http://localhost:3000/api/search?query=Consumer%20Lag" | \
  python3 -m json.tool
```

## Takeaway

> **Consumer lag is the SLO consumers actually care about. A single exporter, a single dashboard, two alerts — and you'll spot 90% of downstream incidents before anyone calls.**

## Instructor notes
- Point at Terminal 4 during Step 5's SIGSTOP — the lag climb is instant and dramatic. That's the "why we monitor this" moment.
- Ask: *"Which alert is more useful — lag > 10000, or lag_rate > 100 rec/s?"* Both are — lag is the sustained-degraded signal; rate is the "getting-worse" leading indicator. Alert on both, with different `for:` windows.
- Discussion — what should the lag threshold be? Answer: it depends on the consumer's throughput. A firehose consumer at 100k rec/s can absorb 100k lag in a second (fine). A slow batch consumer at 10 rec/s takes 3 hours for the same 100k. Time-based lag is more meaningful; kafka-lag-exporter provides it out of the box if the community exporter's records-lag isn't enough.
- Real-world: half of "Kafka is slow" tickets are actually "consumer is slow, but no one's looking at the lag graph". Once you have this dashboard on-screen for the team, 30 min of ticket-triage per week disappears.
- Bridge to future labs: *"Alerting + lag closes the observability loop for the ops team. Next steps beyond this course: distributed tracing across producers/consumers (OpenTelemetry), and cost-focused dashboards (bytes/day/user)."*

## Teardown

```bash
cd ~/kafka-administration/kafka-lab

# Stop the producer + consumer
kill $PROD_PID 2>/dev/null
pkill -f 'lag-demo-app' 2>/dev/null

# Delete the topic + group
$KAFKA/kafka-topics.sh --bootstrap-server $BS --delete --topic lag-demo
$KAFKA/kafka-consumer-groups.sh --bootstrap-server $BS --delete --group lag-demo-app 2>/dev/null

# Leave the exporter + dashboard in place — they're useful for every subsequent scenario.
# To fully revert:
# cd ~/kafka-administration/Lab-02-Monitoring-Setup
# ./down.sh
# git checkout docker-compose.yml prometheus.yml.template 2>/dev/null   # or hand-edit
# rm grafana-provisioning/dashboards/kafka-consumer-lag.json
# ./up.sh
```
