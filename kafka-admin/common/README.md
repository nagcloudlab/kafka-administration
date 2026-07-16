# Shared Lab Runtime

- `native-lab.sh`: shared command engine used by every workshop wrapper.
- `kafka_2.13-3.9.1/`: downloaded Apache Kafka distribution.
- `observability/`: downloaded JMX Exporter, Prometheus, and Grafana binaries.
- `data/`: disposable runtime state.
- `work/`: generated reassignment plans and cluster IDs.
- `backups/`: metadata inventory exports; not record-data backups.

- Run `../workshop1/lab.sh help` for the command catalog.
- Participants should use the workshop-local wrapper and that workshop's README.
- Trainers may inspect this directory while explaining the real tools and generated evidence.
