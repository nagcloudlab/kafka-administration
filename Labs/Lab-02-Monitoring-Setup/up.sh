#!/usr/bin/env bash
#
# Bring up Prometheus + Grafana with the correct WSL/host IP baked into
# prometheus.yml. This is the recommended entrypoint — running
# `docker compose up -d` directly WILL fail because prometheus.yml is
# regenerated here from prometheus.yml.template.
#
# Usage:
#   ./up.sh                          # auto-detect host IP
#   HOST_IP=10.0.0.42 ./up.sh        # override detection

set -euo pipefail

cd "$(dirname "$0")"

# ---- 1. Resolve host IP that containers can reach ------------------------

if [[ -z "${HOST_IP:-}" ]]; then
    HOST_IP=$(ip -4 addr show eth0 2>/dev/null | grep -oP 'inet \K[\d.]+' | head -1 || true)
fi
if [[ -z "${HOST_IP:-}" ]]; then
    HOST_IP=$(hostname -I 2>/dev/null | awk '{print $1}' || true)
fi
if [[ -z "${HOST_IP:-}" ]]; then
    echo "ERROR: could not detect host IP." >&2
    echo "Set it explicitly: HOST_IP=<addr> ./up.sh" >&2
    exit 1
fi
echo "host IP for scrape targets: $HOST_IP"

# ---- 2. Regenerate prometheus.yml from template --------------------------

if [[ ! -f prometheus.yml.template ]]; then
    echo "ERROR: prometheus.yml.template missing" >&2
    exit 1
fi
sed "s/__HOST_IP__/$HOST_IP/g" prometheus.yml.template > prometheus.yml
echo "wrote prometheus.yml"

# ---- 3. Sanity-check JMX exporters are reachable -------------------------

echo -n "checking JMX exporters... "
missing=0
for port in 7071 7072 7073; do
    if ! curl -sf --max-time 2 "http://$HOST_IP:$port/metrics" > /dev/null; then
        echo
        echo "  WARN: $HOST_IP:$port not responding — is the broker up with monitoring?"
        echo "        (fix: cd ../kafka-lab && ./cluster.sh start-monitoring)"
        missing=1
    fi
done
[[ $missing -eq 0 ]] && echo "all 3 reachable"

# ---- 4. Bring up Prometheus + Grafana ------------------------------------

docker compose up -d
echo

# ---- 5. Report URLs ------------------------------------------------------

cat <<EOF
Grafana    http://localhost:3000                          (admin / admin)
  → Dashboard  http://localhost:3000/d/kafka-admin-training
Prometheus http://localhost:9090
  → Targets    http://localhost:9090/targets
EOF
