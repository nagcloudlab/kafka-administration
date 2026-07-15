#!/usr/bin/env bash
#
# Tear down Prometheus + Grafana.
#   ./down.sh          # stop containers, keep volumes
#   ./down.sh -v       # stop containers AND wipe volumes (fresh slate)

set -euo pipefail
cd "$(dirname "$0")"
docker compose down "$@"
