#!/usr/bin/env bash
#
# Lab helper — manage a local 1 x ZooKeeper + 3 x Kafka broker cluster.
#
# Layout it assumes (already present in this repo):
#   kafka-lab/kafka/              -- extracted Kafka distribution
#   kafka-lab/config/             -- zookeeper.properties + broker-{101,102,103}.properties
#
# Runtime state written by this script:
#   kafka-lab/logs/               -- <service>.log per daemon (nohup output)
#   kafka-lab/pids/               -- <service>.pid per daemon
#
# Usage:
#   ./cluster.sh start                       # zookeeper + all 3 brokers
#   ./cluster.sh stop                        # all brokers + zookeeper
#   ./cluster.sh restart                     # stop then start
#   ./cluster.sh status                      # show pids and running state
#   ./cluster.sh start-zk | stop-zk | restart-zk
#   ./cluster.sh start-broker   <101|102|103>
#   ./cluster.sh stop-broker    <101|102|103>
#   ./cluster.sh restart-broker <101|102|103>
#
# Monitoring (JMX Prometheus exporter — see Lab-02-Monitoring-Setup):
#   ./cluster.sh start-monitoring             # zookeeper + brokers with JMX agent on 7071-7073
#   ./cluster.sh restart-monitoring           # stop, then start-monitoring
#   ./cluster.sh start-broker-monitoring   <101|102|103>
#   ./cluster.sh restart-broker-monitoring <101|102|103>
#
# Monitoring defaults (override with env vars if needed):
#   JMX_EXPORTER_JAR     ../Lab-02-Monitoring-Setup/jmx_prometheus_javaagent.jar
#   JMX_EXPORTER_CONFIG  ../Lab-02-Monitoring-Setup/kafka-jmx-config.yaml
# JMX exporter listens on 7070+(broker.id-100)  =>  7071/7072/7073

set -euo pipefail

LAB_DIR="$(cd "$(dirname "$0")" && pwd)"
KAFKA_HOME="$LAB_DIR/kafka"
CONFIG_DIR="$LAB_DIR/config"
LOG_DIR="$LAB_DIR/logs"
PID_DIR="$LAB_DIR/pids"

BROKER_IDS=(101 102 103)

mkdir -p "$LOG_DIR" "$PID_DIR"

# -------------------------------------------------------------- helpers

port_for_broker() {
    case "$1" in
        101) echo 9092 ;;
        102) echo 9093 ;;
        103) echo 9094 ;;
        *)   return 1 ;;
    esac
}

is_running() {
    local pid_file="$1"
    [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null
}

port_in_use() {
    ss -ltn 2>/dev/null | awk '{print $4}' | grep -qE "[:.]$1\$"
}

wait_port() {
    local port="$1" timeout="${2:-30}" i
    for ((i = 0; i < timeout; i++)); do
        port_in_use "$port" && return 0
        sleep 1
    done
    return 1
}

stop_by_pidfile() {
    local pid_file="$1" name="$2" timeout="${3:-30}" i
    if ! is_running "$pid_file"; then
        echo "$name not running"
        rm -f "$pid_file"
        return 0
    fi
    local pid; pid="$(cat "$pid_file")"
    echo "stopping $name (pid $pid)..."
    kill "$pid"
    for ((i = 0; i < timeout; i++)); do
        is_running "$pid_file" || break
        sleep 1
    done
    if is_running "$pid_file"; then
        echo "$name did not exit within ${timeout}s — sending SIGKILL"
        kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$pid_file"
    echo "$name stopped"
}

# -------------------------------------------------------------- zookeeper

start_zk() {
    local pid_file="$PID_DIR/zookeeper.pid"
    if is_running "$pid_file"; then
        echo "zookeeper already running (pid $(cat "$pid_file"))"
        return 0
    fi
    if port_in_use 2181; then
        echo "port 2181 already in use — refusing to start zookeeper"
        return 1
    fi
    echo "starting zookeeper..."
    nohup "$KAFKA_HOME/bin/zookeeper-server-start.sh" \
        "$CONFIG_DIR/zookeeper.properties" > "$LOG_DIR/zookeeper.log" 2>&1 &
    echo $! > "$pid_file"
    if wait_port 2181 20; then
        echo "zookeeper up (pid $(cat "$pid_file"))"
    else
        echo "zookeeper did not bind 2181 in time — check $LOG_DIR/zookeeper.log"
        return 1
    fi
}

stop_zk()    { stop_by_pidfile "$PID_DIR/zookeeper.pid" "zookeeper" 15; }
restart_zk() { stop_zk; start_zk; }

# -------------------------------------------------------------- brokers

start_broker() {
    local id="$1" port pid_file jmx_port jar cfg
    port="$(port_for_broker "$id")" || { echo "unknown broker id: $id"; return 1; }
    pid_file="$PID_DIR/broker-$id.pid"

    if [[ ! -f "$CONFIG_DIR/broker-$id.properties" ]]; then
        echo "missing config: $CONFIG_DIR/broker-$id.properties"
        return 1
    fi
    if is_running "$pid_file"; then
        echo "broker $id already running (pid $(cat "$pid_file"))"
        return 0
    fi
    if port_in_use "$port"; then
        echo "broker $id port $port already in use — refusing to start"
        return 1
    fi

    # Monitoring: when MONITORING=1, attach the JMX Prometheus exporter agent
    # on port 7070 + (id-100). Config resolved from env vars with lab defaults.
    if [[ "${MONITORING:-0}" == "1" ]]; then
        jar="${JMX_EXPORTER_JAR:-$LAB_DIR/../Lab-02-Monitoring-Setup/jmx_prometheus_javaagent.jar}"
        cfg="${JMX_EXPORTER_CONFIG:-$LAB_DIR/../Lab-02-Monitoring-Setup/kafka-jmx-config.yaml}"
        if [[ ! -f "$jar" ]]; then
            echo "monitoring on but JMX jar missing: $jar"
            echo "  hint: see Lab-02-Monitoring-Setup/README.md § 4 (download step)"
            return 1
        fi
        if [[ ! -f "$cfg" ]]; then
            echo "monitoring on but JMX config missing: $cfg"
            return 1
        fi
        jmx_port=$((7070 + id - 100))
        export KAFKA_OPTS="-javaagent:$jar=$jmx_port:$cfg"
        echo "starting broker $id (port $port, JMX exporter on $jmx_port)..."
    else
        echo "starting broker $id (port $port)..."
    fi

    nohup "$KAFKA_HOME/bin/kafka-server-start.sh" \
        "$CONFIG_DIR/broker-$id.properties" > "$LOG_DIR/broker-$id.log" 2>&1 &
    echo $! > "$pid_file"

    if wait_port "$port" 40; then
        echo "broker $id up (pid $(cat "$pid_file"))"
    else
        echo "broker $id did not bind $port in time — check $LOG_DIR/broker-$id.log"
        return 1
    fi
}

stop_broker()    { stop_by_pidfile "$PID_DIR/broker-$1.pid" "broker $1" 30; }
restart_broker() { stop_broker "$1"; start_broker "$1"; }

# -------------------------------------------------------------- whole cluster

start_all() {
    start_zk
    for id in "${BROKER_IDS[@]}"; do start_broker "$id"; done
}

stop_all() {
    for id in "${BROKER_IDS[@]}"; do stop_broker "$id"; done
    stop_zk
}

restart_all() { stop_all; start_all; }

status() {
    printf "%-14s %-10s %-10s %s\n" "service" "pid" "state" "port"
    printf "%-14s %-10s %-10s %s\n" "-------" "---" "-----" "----"
    local pid_file pid state
    pid_file="$PID_DIR/zookeeper.pid"
    if is_running "$pid_file"; then
        printf "%-14s %-10s %-10s %s\n" "zookeeper" "$(cat "$pid_file")" "RUNNING" "2181"
    else
        printf "%-14s %-10s %-10s %s\n" "zookeeper" "-" "stopped" "2181"
    fi
    for id in "${BROKER_IDS[@]}"; do
        pid_file="$PID_DIR/broker-$id.pid"
        if is_running "$pid_file"; then
            printf "%-14s %-10s %-10s %s\n" "broker-$id" "$(cat "$pid_file")" "RUNNING" "$(port_for_broker "$id")"
        else
            printf "%-14s %-10s %-10s %s\n" "broker-$id" "-" "stopped" "$(port_for_broker "$id")"
        fi
    done
}

# -------------------------------------------------------------- dispatch

usage() {
    # Print the top comment block (skip the shebang, stop at the first non-# line).
    awk 'NR==1 {next} /^#/ {sub(/^# ?/, ""); print; next} {exit}' "$0"
}

cmd="${1:-}"
case "$cmd" in
    start)          start_all ;;
    stop)           stop_all ;;
    restart)        restart_all ;;
    status)         status ;;
    start-zk)       start_zk ;;
    stop-zk)        stop_zk ;;
    restart-zk)     restart_zk ;;
    start-broker)   start_broker   "${2:?broker id required (101|102|103)}" ;;
    stop-broker)    stop_broker    "${2:?broker id required (101|102|103)}" ;;
    restart-broker) restart_broker "${2:?broker id required (101|102|103)}" ;;

    # ---- monitoring variants (set MONITORING=1, then delegate) ----
    start-monitoring)
        export MONITORING=1; start_all ;;
    restart-monitoring)
        export MONITORING=1; restart_all ;;
    start-broker-monitoring)
        export MONITORING=1; start_broker   "${2:?broker id required (101|102|103)}" ;;
    restart-broker-monitoring)
        export MONITORING=1; restart_broker "${2:?broker id required (101|102|103)}" ;;

    ""|help|-h|--help) usage ;;
    *) echo "unknown command: $cmd"; echo; usage; exit 1 ;;
esac
