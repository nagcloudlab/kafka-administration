# Lab 11 — Observability at Scale (Alerting + Consumer Lag)

Lab 02 gave you dashboards. This lab gives you the two things you actually get paged on: **alert rules** that fire before disaster, and **consumer lag** — the single metric that predicts angry downstream calls.

## Scenarios (do in order)

- [11.1 — Prometheus alerting + AlertManager](Scenario-11.1-Alerting.md)
- [11.2 — Consumer group lag monitoring](Scenario-11.2-Consumer-Lag.md)

## Common prereqs

- Labs 1, 2, and 3 done. Prometheus + Grafana are up from Lab 02.
- Cluster on 9092–9094 running with monitoring: `./cluster.sh start-monitoring` in `kafka-lab/`.
- Ports free: **9093 (AlertManager) → wait — conflicts with broker 102!**. AlertManager port in this lab is remapped to **9099**. Ports **9308** for kafka-exporter's own web listener.

## Where things live

Lab 11 **extends** Lab-02's stack — adds two containers:

```
Lab-02-Monitoring-Setup/                 (unchanged from Lab-02, still your entrypoint)
├── docker-compose.yml                   ← EXTENDED in Sc 11.1 + 11.2 (alertmanager, kafka-exporter)
├── prometheus.yml.template              ← EXTENDED to scrape alertmanager + kafka-exporter
├── alertmanager/alertmanager.yml        ← NEW (Sc 11.1)
├── prometheus-rules/kafka-alerts.yml    ← NEW (Sc 11.1)
└── grafana-provisioning/dashboards/
    ├── kafka-consumer-lag.json          ← NEW (Sc 11.2)
    └── ...existing dashboards...
```

We keep everything under `Lab-02-Monitoring-Setup/` so the same `./up.sh` continues to bring up the full stack.

## Full reset

Same as Lab 02 (`./down.sh -v` then `./up.sh` to regenerate). Any config we add in this lab lives in files under `Lab-02-Monitoring-Setup/`, so they come back with the next `./up.sh`.

## Not covered in Lab 11

- **SLO / burn-rate multi-window alerting** — real production practice; needs more math than fits a training scenario. Referenced in 11.1's Solution as the next step.
- **Log aggregation (Loki / Elasticsearch)** — same shape as metrics; separate lab.
- **Distributed tracing across producers/consumers** — OpenTelemetry SDK integration on the client side; not an admin lab.
- **PagerDuty / Opsgenie routing** — commodity AlertManager config; use their receiver docs.
