#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION=3.9.1
SCALA_VERSION=2.13
DEFAULT_HOME="$ROOT/common/kafka_${SCALA_VERSION}-${VERSION}"
KAFKA_HOME="${KAFKA_HOME:-$DEFAULT_HOME}"
BOOTSTRAP=127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094,127.0.0.1:19095
SECURE_BOOTSTRAP=127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094
TARGET_BOOTSTRAP=127.0.0.1:39092,127.0.0.1:39093,127.0.0.1:39094
cd "$ROOT"

die() { echo "ERROR: $*" >&2; exit 1; }
need_java() { command -v java >/dev/null || die "Java 11 or 17 is required"; }
need_kafka() { [[ -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]] || die "Kafka not found at $KAFKA_HOME. Run: $0 setup"; }
port_open() { (echo >/dev/tcp/127.0.0.1/"$1") >/dev/null 2>&1; }
zk_cmd() { local port=$1 cmd=$2; (exec 3<>/dev/tcp/127.0.0.1/"$port"; printf '%s\n' "$cmd" >&3; timeout 2 cat <&3) 2>/dev/null || true; }
assert_ports_free() {
  local port busy=0
  for port in 12181 12182 12183 12881 12882 12883 13881 13882 13883 19092 19093 19094 19095 19981 19982 19983 19991 19992 19993 19994; do
    if port_open "$port"; then echo "Port $port is already in use" >&2; busy=1; fi
  done
  (( busy == 0 )) || die "stop the conflicting processes before starting the lab"
}
check_port_set() {
  local label=$1; shift; local port busy=0
  for port in "$@"; do if port_open "$port"; then echo "$label port $port is already in use" >&2; busy=1; fi; done
  (( busy == 0 )) || die "stop conflicting processes before this workshop"
  echo "$label ports are free."
}

case "${1:-help}" in
  setup)
    need_java
    command -v curl >/dev/null || die "curl is required for the one-time download"
    command -v tar >/dev/null || die "tar is required"
    mkdir -p common/data/{zk1,zk2,zk3,broker1,broker2,broker3,broker4} common/work
    printf '1\n' > common/data/zk1/myid
    printf '2\n' > common/data/zk2/myid
    printf '3\n' > common/data/zk3/myid
    if [[ ! -x "$DEFAULT_HOME/bin/kafka-server-start.sh" ]]; then
      archive="kafka_${SCALA_VERSION}-${VERSION}.tgz"
      url="https://archive.apache.org/dist/kafka/${VERSION}/${archive}"
      echo "Downloading $url"
      curl -fL --retry 3 "$url" -o "common/$archive"
      tar -xzf "common/$archive" -C common
      rm "common/$archive"
    fi
    echo "Ready. Kafka home: $DEFAULT_HOME" ;;
  preflight)
    need_java; need_kafka; scope=${2:-base}
    echo "Java: $(java -version 2>&1 | head -1)"
    echo "Kafka: $KAFKA_HOME"
    case "$scope" in
      base) assert_ports_free; echo "Base cluster ports are free." ;;
      mm) check_port_set MirrorMaker 39092 39093 39094 39101 39102 39103 49991 49992 49993 ;;
      monitoring) check_port_set Monitoring 17071 17072 17073 9090 3000 ;;
      kraft) check_port_set KRaft 29092 29093 29094 29101 29102 29103 29901 29902 29903 29911 29912 29913 ;;
      security) check_port_set Security 19092 19093 19094 39991 39992 39993 ;;
      all) assert_ports_free; check_port_set Additional 17071 17072 17073 29092 29093 29094 29101 29102 29103 29901 29902 29903 29911 29912 29913 3000 39092 39093 39094 39101 39102 39103 39991 39992 39993 49991 49992 49993 9090 ;;
      *) die "use: $0 preflight base|mm|monitoring|kraft|security|all" ;;
    esac ;;
  validate-files)
    need_java; need_kafka
    bash -n common/native-lab.sh
    for f in workshop{1,2,3,4,5,6,7,8,9,10}/lab.sh; do bash -n "$f"; [[ -x $f ]] || die "$f is not executable"; done
    for f in workshop1/config/{zk1,zk2,zk3,broker1,broker2,broker3}.properties workshop4/config/broker4.properties workshop7/config/{target1,target2,target3,mm2}.properties workshop9/config/kraft/{controller1,controller2,controller3,broker1,broker2,broker3}.properties workshop10/config/security/{broker1,broker2,broker3,admin,producer,consumer}.properties; do [[ -s $f ]] || die "$f missing or empty"; done
    [[ $(<common/data/zk1/myid) == 1 && $(<common/data/zk2/myid) == 2 && $(<common/data/zk3/myid) == 3 ]] || die "ZooKeeper myid files are incorrect"
    if [[ -x common/observability/prometheus-3.13.0.linux-amd64/promtool ]]; then common/observability/prometheus-3.13.0.linux-amd64/promtool check config workshop8/config/prometheus/prometheus.yml; fi
    [[ ! -f workshop10/security/generated/broker1.crt ]] || openssl x509 -checkend 86400 -noout -in workshop10/security/generated/broker1.crt
    echo "All scripts, required configs, node IDs, monitoring rules, and available certificates validated." ;;
  start-zk)
    need_kafka; node=${2:-}; [[ $node =~ ^zk[123]$ ]] || die "use: $0 start-zk zk1|zk2|zk3"
    echo "Starting $node in foreground; press Ctrl-C for a graceful stop"
    export JMX_PORT=$((19980 + ${node#zk}))
    exec "$KAFKA_HOME/bin/zookeeper-server-start.sh" "workshop1/config/$node.properties" ;;
  start-broker)
    need_kafka; node=${2:-}; [[ $node =~ ^broker[1234]$ ]] || die "use: $0 start-broker broker1|broker2|broker3|broker4"
    echo "Starting $node in foreground; press Ctrl-C for controlled shutdown"
    export JMX_PORT=$((19990 + ${node#broker}))
    cfg="workshop1/config/$node.properties"; [[ $node == broker4 ]] && cfg="workshop4/config/broker4.properties"
    exec "$KAFKA_HOME/bin/kafka-server-start.sh" "$cfg" ;;
  status)
    need_kafka
    echo "ZooKeeper ensemble:"
    for n in 1 2 3; do
      if port_open "1218$n"; then printf '  zk%s: ' "$n"; zk_cmd "1218$n" srvr | grep -E 'Mode:|Node count:' | tr '\n' ' '; echo; else echo "  zk$n: DOWN"; fi
    done
    echo "Kafka listeners:"
    for n in 2 3 4 5; do port="1909$n"; if port_open "$port"; then echo "  broker$((n-1)): UP ($port)"; else echo "  broker$((n-1)): DOWN ($port)"; fi; done
    if port_open 19092; then
      echo "Under-replicated partitions (empty is healthy):"
      "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe --under-replicated-partitions
    fi ;;
  health)
    need_kafka; port_open 19092 || die "broker1 is not reachable on 19092"
    echo "Registered broker endpoints:"
    "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server "$BOOTSTRAP" 2>/dev/null | grep -E '^[^[:space:]]+ \(id:'
    echo "Offline partitions (empty is healthy):"
    "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe --unavailable-partitions
    echo "Under-replicated partitions (empty is healthy):"
    "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe --under-replicated-partitions ;;
  topics) need_kafka; "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list ;;
  topic-config)
    need_kafka; topic=${2:-orders}
    "$KAFKA_HOME/bin/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" --entity-type topics --entity-name "$topic" --describe ;;
  set-retention)
    need_kafka; topic=${2:-}; retention=${3:-}
    [[ -n $topic && $retention =~ ^[0-9]+$ ]] || die "use: $0 set-retention TOPIC MILLISECONDS"
    "$KAFKA_HOME/bin/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" --entity-type topics --entity-name "$topic" --alter --add-config "retention.ms=$retention"
    "$0" topic-config "$topic" ;;
  clear-retention)
    need_kafka; topic=${2:-}; [[ -n $topic ]] || die "use: $0 clear-retention TOPIC"
    "$KAFKA_HOME/bin/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" --entity-type topics --entity-name "$topic" --alter --delete-config retention.ms
    "$0" topic-config "$topic" ;;
  increase-partitions)
    need_kafka; topic=${2:-}; count=${3:-}
    [[ -n $topic && $count =~ ^[0-9]+$ ]] || die "use: $0 increase-partitions TOPIC NEW_TOTAL"
    "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --alter --topic "$topic" --partitions "$count" ;;
  log-dirs) need_kafka; "$KAFKA_HOME/bin/kafka-log-dirs.sh" --bootstrap-server "$BOOTSTRAP" --describe ;;
  create-topic)
    need_kafka; topic=${2:-orders}; partitions=${3:-6}
    "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic "$topic" --partitions "$partitions" --replication-factor 3 ;;
  delete-topic)
    need_kafka; topic=${2:-}; confirm=${3:-}; [[ -n $topic && $confirm == DELETE ]] || die "use: $0 delete-topic TOPIC DELETE"
    "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$topic" ;;
  describe)
    need_kafka
    if [[ -n ${2:-} ]]; then
      "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe --topic "$2"
    else
      "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe
    fi ;;
  produce)
    need_kafka; echo "Type messages; Ctrl-D finishes"
    exec "$KAFKA_HOME/bin/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "${2:-orders}" --producer-property acks=all ;;
  produce-sequence)
    need_kafka; topic=${2:-orders}; count=${3:-10}; [[ $count =~ ^[1-9][0-9]*$ ]] || die "COUNT must be positive"
    for ((i=1; i<=count; i++)); do printf 'proof-%03d\n' "$i"; done | "$KAFKA_HOME/bin/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$topic" --producer-property acks=all
    echo "Produced $count numbered proof records to $topic." ;;
  consume-count)
    need_kafka; topic=${2:-orders}; count=${3:-10}; [[ $count =~ ^[1-9][0-9]*$ ]] || die "COUNT must be positive"
    "$KAFKA_HOME/bin/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$topic" --from-beginning --max-messages "$count" --timeout-ms 15000 ;;
  cluster-proof)
    need_kafka; topic=${2:-trainer-proof}; count=${3:-10}
    "$0" health
    "$0" create-topic "$topic" 6
    "$0" produce-sequence "$topic" "$count"
    "$0" consume-count "$topic" "$count"
    echo "PROOF PASSED: cluster is healthy and $count acknowledged records were read from $topic." ;;
  consume)
    need_kafka
    exec "$KAFKA_HOME/bin/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" --topic "${2:-orders}" --from-beginning ;;
  consume-group)
    need_kafka; group=${2:-training-group}; topic=${3:-orders}
    exec "$KAFKA_HOME/bin/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$topic" --group "$group" --consumer-property enable.auto.commit=true ;;
  consume-group-count)
    need_kafka; group=${2:-training-group}; topic=${3:-orders}; count=${4:-10}; [[ $count =~ ^[1-9][0-9]*$ ]] || die "COUNT must be positive"
    "$KAFKA_HOME/bin/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$topic" --group "$group" --max-messages "$count" --timeout-ms 15000 --consumer-property enable.auto.commit=true ;;
  offsets)
    need_kafka; cluster=${2:-source}; topic=${3:-orders}; bs=$BOOTSTRAP; [[ $cluster == target ]] && bs=$TARGET_BOOTSTRAP
    [[ $cluster == source || $cluster == target ]] || die "use: $0 offsets source|target TOPIC"
    "$KAFKA_HOME/bin/kafka-get-offsets.sh" --bootstrap-server "$bs" --topic "$topic" ;;
  groups) need_kafka; "$KAFKA_HOME/bin/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" --all-groups --describe ;;
  group-members)
    need_kafka; group=${2:-training-group}
    "$KAFKA_HOME/bin/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" --group "$group" --describe --members --verbose ;;
  reset-offsets)
    need_kafka; group=${2:-}; topic=${3:-}; target=${4:---to-earliest}; mode=${5:-preview}
    [[ -n $group && -n $topic ]] || die "use: $0 reset-offsets GROUP TOPIC (--to-earliest|--to-latest|--shift-by N) [preview|APPLY]"
    args=(--bootstrap-server "$BOOTSTRAP" --group "$group" --topic "$topic" --reset-offsets)
    case "$target" in
      --to-earliest|--to-latest) args+=("$target") ;;
      --shift-by) [[ ${5:-} =~ ^-?[0-9]+$ ]] || die "--shift-by requires a number"; args+=(--shift-by "$5"); mode=${6:-preview} ;;
      *) die "target must be --to-earliest, --to-latest, or --shift-by" ;;
    esac
    [[ $mode == APPLY ]] && args+=(--execute) || args+=(--dry-run)
    "$KAFKA_HOME/bin/kafka-consumer-groups.sh" "${args[@]}" ;;
  set-user-quota)
    need_kafka; user=${2:-}; producer=${3:-}; consumer=${4:-}
    [[ -n $user && $producer =~ ^[0-9]+$ && $consumer =~ ^[0-9]+$ ]] || die "use: $0 set-user-quota USER PRODUCER_BYTES_PER_SEC CONSUMER_BYTES_PER_SEC"
    "$KAFKA_HOME/bin/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" --alter --entity-type users --entity-name "$user" --add-config "producer_byte_rate=$producer,consumer_byte_rate=$consumer" ;;
  describe-user-quota)
    need_kafka
    if [[ -n ${2:-} ]]; then
      "$KAFKA_HOME/bin/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" --describe --entity-type users --entity-name "$2"
    else
      "$KAFKA_HOME/bin/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" --describe --entity-type users
    fi ;;
  clear-user-quota)
    need_kafka; user=${2:-}; [[ -n $user ]] || die "use: $0 clear-user-quota USER"
    "$KAFKA_HOME/bin/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" --alter --entity-type users --entity-name "$user" --delete-config producer_byte_rate,consumer_byte_rate ;;
  reassign-generate)
    need_kafka; topic=${2:-orders}; brokers=${3:-1,2,3,4}; mkdir -p common/work
    printf '{"topics":[{"topic":"%s"}],"version":1}\n' "$topic" > common/work/topics-to-move.json
    "$KAFKA_HOME/bin/kafka-reassign-partitions.sh" --bootstrap-server "$BOOTSTRAP" --topics-to-move-json-file common/work/topics-to-move.json --broker-list "$brokers" --generate | tee common/work/reassignment-proposal.txt
    awk '/Proposed partition reassignment configuration/{getline; print; exit}' common/work/reassignment-proposal.txt > common/work/reassignment.json
    [[ -s common/work/reassignment.json ]] || die "could not extract proposed reassignment JSON"
    echo "Proposed JSON saved to common/work/reassignment.json. Review it before execution." ;;
  reassign-execute)
    need_kafka; file=${2:-common/work/reassignment.json}; throttle=${3:-5000000}; confirm=${4:-}
    [[ -f $file ]] || die "$file not found; generate and review a plan first"
    [[ $throttle =~ ^[0-9]+$ && $confirm == APPLY ]] || die "use: $0 reassign-execute FILE BYTES_PER_SEC APPLY"
    "$KAFKA_HOME/bin/kafka-reassign-partitions.sh" --bootstrap-server "$BOOTSTRAP" --reassignment-json-file "$file" --execute --throttle "$throttle" ;;
  reassign-verify)
    need_kafka; file=${2:-common/work/reassignment.json}; [[ -f $file ]] || die "$file not found"
    "$KAFKA_HOME/bin/kafka-reassign-partitions.sh" --bootstrap-server "$BOOTSTRAP" --reassignment-json-file "$file" --verify --preserve-throttles ;;
  reassign-clear-throttle)
    need_kafka; file=${2:-common/work/reassignment.json}; [[ -f $file ]] || die "$file not found"
    "$KAFKA_HOME/bin/kafka-reassign-partitions.sh" --bootstrap-server "$BOOTSTRAP" --reassignment-json-file "$file" --verify ;;
  broker-configs) need_kafka; "$KAFKA_HOME/bin/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" --entity-type brokers --all --describe ;;
  metadata-version) need_kafka; "$KAFKA_HOME/bin/kafka-features.sh" --bootstrap-server "$BOOTSTRAP" describe ;;
  backup-metadata)
    need_kafka; stamp=$(date -u +%Y%m%dT%H%M%SZ); dir="common/backups/$stamp"; mkdir -p "$dir"
    "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe > "$dir/topics.txt"
    "$KAFKA_HOME/bin/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" --entity-type topics --all --describe > "$dir/topic-configs.txt"
    "$KAFKA_HOME/bin/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" --entity-type brokers --all --describe > "$dir/broker-configs.txt"
    "$KAFKA_HOME/bin/kafka-acls.sh" --bootstrap-server "$BOOTSTRAP" --list > "$dir/acls.txt" 2>&1 || true
    "$KAFKA_HOME/bin/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" --all-groups --describe > "$dir/consumer-groups.txt" || true
    echo "Metadata inventory written to $dir (this is not a record-data backup)." ;;
  setup-mm-target)
    need_kafka
    for p in 39092 39093 39094 39101 39102 39103; do port_open "$p" && die "port $p is active; stop the target cluster first"; done
    mkdir -p common/work
    id=$([[ -f common/work/mm-target-cluster-id ]] && cat common/work/mm-target-cluster-id || "$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)
    printf '%s\n' "$id" > common/work/mm-target-cluster-id
    for cfg in workshop7/config/target{1..3}.properties; do "$KAFKA_HOME/bin/kafka-storage.sh" format --ignore-formatted --cluster-id "$id" --config "$cfg"; done
    echo "MirrorMaker target formatted with cluster ID $id" ;;
  start-mm-target)
    need_kafka; node=${2:-}; [[ $node =~ ^target[123]$ ]] || die "use: $0 start-mm-target target1|target2|target3"
    export JMX_PORT=$((49990 + ${node#target})); exec "$KAFKA_HOME/bin/kafka-server-start.sh" "workshop7/config/$node.properties" ;;
  start-mm2)
    need_kafka; echo "Starting MirrorMaker 2 in foreground; Ctrl-C stops replication"
    exec "$KAFKA_HOME/bin/connect-mirror-maker.sh" workshop7/config/mm2.properties ;;
  mm-status)
    need_kafka
    echo "Target topics:"; "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$TARGET_BOOTSTRAP" --list
    echo "Target orders:"; "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$TARGET_BOOTSTRAP" --describe --topic orders 2>/dev/null || true
    echo "Target consumer group:"; "$KAFKA_HOME/bin/kafka-consumer-groups.sh" --bootstrap-server "$TARGET_BOOTSTRAP" --group training-group --describe 2>/dev/null || true ;;
  mm-target-consume)
    need_kafka; exec "$KAFKA_HOME/bin/kafka-console-consumer.sh" --bootstrap-server "$TARGET_BOOTSTRAP" --topic "${2:-orders}" --from-beginning --timeout-ms "${3:-10000}" ;;
  reset-mm-target)
    for p in 39092 39093 39094 39101 39102 39103; do port_open "$p" && die "port $p is active; stop target and MirrorMaker first"; done
    rm -rf common/data/mm-target* common/work/mm-target-cluster-id; echo "MirrorMaker target data deleted." ;;
  setup-observability)
    need_java; command -v curl >/dev/null || die "curl is required"; mkdir -p common/observability common/data/{prometheus,grafana}
    jmx=common/observability/jmx_prometheus_javaagent-1.5.0.jar
    [[ -f $jmx ]] || curl -fL --retry 3 "https://github.com/prometheus/jmx_exporter/releases/download/1.5.0/jmx_prometheus_javaagent-1.5.0.jar" -o "$jmx"
    if [[ ! -x common/observability/prometheus-3.13.0.linux-amd64/prometheus ]]; then
      file=prometheus-3.13.0.linux-amd64.tar.gz; curl -fL --retry 3 "https://github.com/prometheus/prometheus/releases/download/v3.13.0/$file" -o "common/observability/$file"
      echo '744d93324cc024d82089921737bd797474d7f1e5dbbfd1c6b387bad258538cb9  common/observability/prometheus-3.13.0.linux-amd64.tar.gz' | sha256sum -c -
      tar -xzf "common/observability/$file" -C common/observability; rm "common/observability/$file"
    fi
    if [[ ! -x common/observability/grafana-13.1.0/bin/grafana ]]; then
      file=grafana_13.1.0_28013217238_linux_amd64.tar.gz; curl -fL --retry 3 "https://dl.grafana.com/grafana/release/13.1.0/$file" -o "common/observability/$file"
      echo '4f562bb224b8bb758b47789381babb284cb41687da8d714f2ff0e118e945e775  common/observability/grafana_13.1.0_28013217238_linux_amd64.tar.gz' | sha256sum -c -
      tar -xzf "common/observability/$file" -C common/observability; rm "common/observability/$file"
    fi
    echo "JMX Exporter, Prometheus 3.13.0 LTS, and Grafana 13.1.0 are ready." ;;
  start-monitored-broker)
    need_kafka; node=${2:-}; [[ $node =~ ^broker[123]$ ]] || die "use: $0 start-monitored-broker broker1|broker2|broker3"
    [[ -f common/observability/jmx_prometheus_javaagent-1.5.0.jar ]] || die "run setup-observability first"
    port=$((17070 + ${node#broker})); export JMX_PORT=$((19990 + ${node#broker}))
    export KAFKA_OPTS="${KAFKA_OPTS:-} -javaagent:$ROOT/common/observability/jmx_prometheus_javaagent-1.5.0.jar=$port:$ROOT/workshop8/config/jmx/kafka.yml"
    echo "Starting $node with Prometheus exporter on $port"
    exec "$KAFKA_HOME/bin/kafka-server-start.sh" "workshop1/config/$node.properties" ;;
  start-prometheus)
    bin=common/observability/prometheus-3.13.0.linux-amd64/prometheus; [[ -x $bin ]] || die "run setup-observability first"
    exec "$bin" --config.file=workshop8/config/prometheus/prometheus.yml --storage.tsdb.path=common/data/prometheus --web.listen-address=127.0.0.1:9090 ;;
  start-grafana)
    home=common/observability/grafana-13.1.0; [[ -x $home/bin/grafana ]] || die "run setup-observability first"
    export KAFKA_TRAINING_DASHBOARDS="$ROOT/workshop8/config/grafana/dashboards" GF_PATHS_PROVISIONING="$ROOT/workshop8/config/grafana/provisioning" GF_PATHS_DATA="$ROOT/common/data/grafana" GF_PATHS_LOGS="$ROOT/common/data/grafana/logs" GF_PATHS_PLUGINS="$ROOT/common/data/grafana/plugins" GF_SERVER_HTTP_ADDR=127.0.0.1 GF_SERVER_HTTP_PORT=3000 GF_SECURITY_ADMIN_USER=admin GF_SECURITY_ADMIN_PASSWORD=admin
    exec "$home/bin/grafana" server --homepath "$ROOT/$home" ;;
  monitoring-proof)
    command -v curl >/dev/null || die "curl is required"
    echo "Prometheus readiness:"; curl -fsS http://127.0.0.1:9090/-/ready; echo
    echo "Scrape target health:"; curl -fsSG http://127.0.0.1:9090/api/v1/query --data-urlencode 'query=up{job="kafka-brokers"}' ; echo
    echo "Under-replicated partitions:"; curl -fsSG http://127.0.0.1:9090/api/v1/query --data-urlencode 'query=kafka_server_replicamanager_underreplicatedpartitions'; echo
    echo "Grafana health:"; curl -fsS http://127.0.0.1:3000/api/health; echo ;;
  setup-kraft)
    need_kafka
    for p in 29101 29102 29103 29092 29093 29094; do port_open "$p" && die "port $p is active; stop the KRaft lab first"; done
    mkdir -p common/work common/data
    cluster_id=$([[ -f common/work/kraft-cluster-id ]] && cat common/work/kraft-cluster-id || "$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)
    printf '%s\n' "$cluster_id" > common/work/kraft-cluster-id
    for cfg in workshop9/config/kraft/controller{1..3}.properties workshop9/config/kraft/broker{1..3}.properties; do
      "$KAFKA_HOME/bin/kafka-storage.sh" format --ignore-formatted --cluster-id "$cluster_id" --config "$cfg"
    done
    echo "KRaft storage formatted with cluster ID $cluster_id" ;;
  start-controller)
    need_kafka; node=${2:-}; [[ $node =~ ^controller[123]$ ]] || die "use: $0 start-controller controller1|controller2|controller3"
    export JMX_PORT=$((29900 + ${node#controller})); echo "Starting KRaft $node; press Ctrl-C to stop"
    exec "$KAFKA_HOME/bin/kafka-server-start.sh" "workshop9/config/kraft/$node.properties" ;;
  start-kraft-broker)
    need_kafka; node=${2:-}; [[ $node =~ ^broker[123]$ ]] || die "use: $0 start-kraft-broker broker1|broker2|broker3"
    export JMX_PORT=$((29910 + ${node#broker})); echo "Starting KRaft $node; press Ctrl-C to stop"
    exec "$KAFKA_HOME/bin/kafka-server-start.sh" "workshop9/config/kraft/$node.properties" ;;
  kraft-status)
    need_kafka
    "$KAFKA_HOME/bin/kafka-metadata-quorum.sh" --bootstrap-controller 127.0.0.1:29101,127.0.0.1:29102,127.0.0.1:29103 describe --status
    "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server 127.0.0.1:29092,127.0.0.1:29093,127.0.0.1:29094 --describe --under-replicated-partitions ;;
  reset-kraft)
    for p in 29101 29102 29103 29092 29093 29094; do port_open "$p" && die "port $p is active; stop all KRaft processes first"; done
    rm -rf common/data/kraft-* common/work/kraft-cluster-id; echo "KRaft data deleted. Run setup-kraft before the next start." ;;
  setup-security)
    need_kafka; command -v openssl >/dev/null || die "openssl is required"; command -v keytool >/dev/null || die "keytool is required"
    rm -rf workshop10/security/generated; mkdir -p workshop10/security/generated common/data/{security-broker1,security-broker2,security-broker3}
    openssl req -new -x509 -nodes -days 365 -newkey rsa:2048 -keyout workshop10/security/generated/ca.key -out workshop10/security/generated/ca.crt -subj '/CN=Kafka Training CA'
    keytool -importcert -noprompt -alias training-ca -file workshop10/security/generated/ca.crt -keystore workshop10/security/generated/truststore.p12 -storetype PKCS12 -storepass changeit
    for n in 1 2 3; do
      keytool -genkeypair -alias broker -keyalg RSA -keysize 2048 -validity 365 -dname 'CN=localhost' -ext 'SAN=dns:localhost,ip:127.0.0.1' -keystore "workshop10/security/generated/broker$n.p12" -storetype PKCS12 -storepass changeit -keypass changeit
      keytool -certreq -alias broker -keystore "workshop10/security/generated/broker$n.p12" -storepass changeit -file "workshop10/security/generated/broker$n.csr" -ext 'SAN=dns:localhost,ip:127.0.0.1'
      openssl x509 -req -days 365 -in "workshop10/security/generated/broker$n.csr" -CA workshop10/security/generated/ca.crt -CAkey workshop10/security/generated/ca.key -CAcreateserial -out "workshop10/security/generated/broker$n.crt" -extfile <(printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\n')
      keytool -importcert -noprompt -alias training-ca -file workshop10/security/generated/ca.crt -keystore "workshop10/security/generated/broker$n.p12" -storepass changeit
      keytool -importcert -noprompt -alias broker -file "workshop10/security/generated/broker$n.crt" -keystore "workshop10/security/generated/broker$n.p12" -storepass changeit
    done
    echo "Training CA, broker certificates, and truststore created under workshop10/security/generated." ;;
  start-secure-broker)
    need_kafka; node=${2:-}; [[ $node =~ ^broker[123]$ ]] || die "use: $0 start-secure-broker broker1|broker2|broker3"
    [[ -f workshop10/security/generated/truststore.p12 ]] || die "run setup-security first"
    export JMX_PORT=$((39990 + ${node#broker})); echo "Starting TLS/SASL/ACL $node; press Ctrl-C to stop"
    exec "$KAFKA_HOME/bin/kafka-server-start.sh" "workshop10/config/security/$node.properties" ;;
  secure-create-topic)
    need_kafka; topic=${2:-secure-orders}
    "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$SECURE_BOOTSTRAP" --command-config workshop10/config/security/admin.properties --create --if-not-exists --topic "$topic" --partitions 6 --replication-factor 3 ;;
  secure-grant)
    need_kafka; topic=${2:-secure-orders}; group=${3:-secure-training-group}
    "$KAFKA_HOME/bin/kafka-acls.sh" --bootstrap-server "$SECURE_BOOTSTRAP" --command-config workshop10/config/security/admin.properties --add --allow-principal User:producer --operation Write --operation Describe --topic "$topic"
    "$KAFKA_HOME/bin/kafka-acls.sh" --bootstrap-server "$SECURE_BOOTSTRAP" --command-config workshop10/config/security/admin.properties --add --allow-principal User:consumer --operation Read --operation Describe --topic "$topic"
    "$KAFKA_HOME/bin/kafka-acls.sh" --bootstrap-server "$SECURE_BOOTSTRAP" --command-config workshop10/config/security/admin.properties --add --allow-principal User:consumer --operation Read --group "$group" ;;
  secure-acls) need_kafka; "$KAFKA_HOME/bin/kafka-acls.sh" --bootstrap-server "$SECURE_BOOTSTRAP" --command-config workshop10/config/security/admin.properties --list ;;
  secure-produce)
    need_kafka; exec "$KAFKA_HOME/bin/kafka-console-producer.sh" --bootstrap-server "$SECURE_BOOTSTRAP" --topic "${2:-secure-orders}" --producer.config workshop10/config/security/producer.properties --producer-property acks=all ;;
  secure-consume)
    need_kafka; exec "$KAFKA_HOME/bin/kafka-console-consumer.sh" --bootstrap-server "$SECURE_BOOTSTRAP" --topic "${2:-secure-orders}" --group "${3:-secure-training-group}" --consumer.config workshop10/config/security/consumer.properties --from-beginning ;;
  cert-info)
    [[ -f workshop10/security/generated/broker1.crt ]] || die "run setup-security first"
    openssl x509 -in workshop10/security/generated/broker1.crt -noout -subject -issuer -dates -ext subjectAltName ;;
  preferred-leaders) need_kafka; "$KAFKA_HOME/bin/kafka-leader-election.sh" --bootstrap-server "$BOOTSTRAP" --election-type PREFERRED --all-topic-partitions ;;
  reset)
    for p in 12181 12182 12183 12881 12882 12883 13881 13882 13883 19092 19093 19094 19095; do port_open "$p" && die "port $p is active; stop all terminals first"; done
    rm -rf common/data
    mkdir -p common/data/{zk1,zk2,zk3,broker1,broker2,broker3,broker4}
    printf '1\n' > common/data/zk1/myid; printf '2\n' > common/data/zk2/myid; printf '3\n' > common/data/zk3/myid
    echo "All native lab data deleted and node IDs recreated." ;;
  *)
    cat <<'HELP'
Usage: ./lab.sh COMMAND   # from any workshop directory
  setup | preflight [base|mm|monitoring|kraft|security|all] | validate-files
  start-zk zk1|zk2|zk3             (foreground)
  start-broker broker1|broker2|broker3|broker4 (foreground)
  status | health | topics | create-topic [name] [partitions] | delete-topic TOPIC DELETE | describe [topic]
  topic-config [topic] | set-retention TOPIC MS | clear-retention TOPIC
  increase-partitions TOPIC NEW_TOTAL | log-dirs
  produce [topic] | produce-sequence [topic] [count] | consume [topic]
  consume-count [topic] [count] | cluster-proof [topic] [count]
  consume-group [group] [topic]
  consume-group-count [group] [topic] [count] | offsets source|target [topic]
  groups | group-members [group] | reset-offsets GROUP TOPIC TARGET [preview|APPLY]
  set-user-quota USER PRODUCER_BPS CONSUMER_BPS | describe-user-quota [USER] | clear-user-quota USER
  reassign-generate [TOPIC] [BROKER_IDS] | reassign-execute FILE BPS APPLY
  reassign-verify [FILE] | reassign-clear-throttle [FILE]
  broker-configs | metadata-version | backup-metadata | preferred-leaders | reset
  setup-mm-target | start-mm-target target1-3 | start-mm2 | mm-status
  mm-target-consume [TOPIC] [TIMEOUT_MS] | reset-mm-target
  setup-observability | start-monitored-broker broker1-3
  start-prometheus | start-grafana | monitoring-proof
  setup-kraft | start-controller controller1-3 | start-kraft-broker broker1-3
  kraft-status | reset-kraft
  setup-security | start-secure-broker broker1-3 | secure-create-topic [TOPIC]
  secure-grant [TOPIC] [GROUP] | secure-acls | secure-produce [TOPIC]
  secure-consume [TOPIC] [GROUP] | cert-info
HELP
    ;;
esac
