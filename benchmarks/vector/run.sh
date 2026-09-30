#!/usr/bin/env bash
#
# Runs the vector benchmark against one target end to end: load, recall, readers, filtered and
# under-insert, one result JSON per run.
#
# The script is endpoint-driven. It probes the target's endpoint first. It starts a container from
# this folder's compose file only when the caller gave no --url and the target's default localhost
# endpoint does not answer. Every other path completes without Docker on the client, and a caller
# endpoint is used as given.
#
# The external RavenDB development server is never started by this script. The containerized
# targets (ravendb-7, pgvector) are started from the compose file when they are needed and are
# stopped again only when this script started them. Elasticsearch is the exception to reuse: without
# --url the script always starts a fresh cluster in its own compose project, because the trial licence
# is per cluster, and removes it with its data on exit.
#
# --constrained runs load and recall alone with the database container's memory limited. The harness
# limits and restarts the container, so the script only allows it for a container it started itself.
#
# Every option other than --target is forwarded to the vector command unchanged. A caller-supplied
# --scenario, --url, --database or --output-prefix replaces the script default rather than being
# appended twice.

set -euo pipefail

# The physical path: MSBuild must see one path per project, and a path through a symbolic link gives it two.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SCENARIO_DEFAULT="$SCRIPT_DIR/scenario.json"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
RESULTS_DIR="$SCRIPT_DIR/results"

# How long the script waits for a target to answer, and how often it probes, in seconds.
READY_TIMEOUT="${VECTOR_READY_TIMEOUT:-120}"
READY_POLL_SECONDS=2

usage() {
  cat <<'EOF'
Usage: run.sh --target <ravendb|ravendb-7|pgvector|elasticsearch> [--constrained] [options]

Runs load, recall, readers, filtered and under-insert, and writes one result JSON per run under
benchmarks/vector/results/. Every other option is forwarded to the vector command unchanged.

The script starts a target from benchmarks/vector/docker-compose.yml only when no --url was given
and the target's default localhost endpoint does not answer. A caller-supplied --url is used as
given and starts nothing.

Examples:
  ./benchmarks/vector/run.sh --target ravendb
  ./benchmarks/vector/run.sh --target pgvector --seed 7 --dataset sphere-100k
  ./benchmarks/vector/run.sh --target elasticsearch --elasticsearch-index-kind bbq_disk
  ./benchmarks/vector/run.sh --target elasticsearch --constrained
  ./benchmarks/vector/run.sh --target pgvector --url postgresql://bench:bench@db-host:5432/bench --node-exporter-url http://db-host:9100/metrics
EOF
}

TARGET=""
PASSTHROUGH=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)
      TARGET="${2:-}"
      shift 2
      ;;
    --target=*)
      TARGET="${1#*=}"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      PASSTHROUGH+=("$1")
      shift
      ;;
  esac
done

if [[ -z "$TARGET" ]]; then
  echo "error: --target is required. Valid targets: ravendb, ravendb-7, pgvector, elasticsearch." >&2
  exit 2
fi

case "$TARGET" in
  ravendb|ravendb-7|pgvector|elasticsearch) ;;
  *)
    echo "error: unknown target '$TARGET'. Valid targets: ravendb, ravendb-7, pgvector, elasticsearch." >&2
    exit 2
    ;;
esac

# The host port of the containerized RavenDB service is overridable, so a database host can move it
# and a caller can point the script's default endpoint away from a port already in use.
RAVENDB7_PORT="${RAVENDB7_PORT:-8087}"
PGVECTOR_PORT="${PGVECTOR_PORT:-5432}"
ELASTICSEARCH_PORT="${ELASTICSEARCH_PORT:-9200}"
export ELASTICSEARCH_PORT
COMPOSE_PROJECT=(-f "$COMPOSE_FILE")

case "$TARGET" in
  ravendb)
    DEFAULT_URL="http://localhost:8081"
    DEFAULT_PORT=8081
    COMPOSE_SERVICE=""
    ;;
  ravendb-7)
    DEFAULT_URL="http://localhost:$RAVENDB7_PORT"
    DEFAULT_PORT="$RAVENDB7_PORT"
    PORT_VARIABLE=RAVENDB7_PORT
    COMPOSE_SERVICE="ravendb-7"
    ;;
  pgvector)
    DEFAULT_URL="postgresql://bench:bench@localhost:$PGVECTOR_PORT/bench"
    DEFAULT_PORT="$PGVECTOR_PORT"
    PORT_VARIABLE=PGVECTOR_PORT
    COMPOSE_SERVICE="pgvector"
    ;;
  elasticsearch)
    DEFAULT_URL="http://localhost:$ELASTICSEARCH_PORT"
    DEFAULT_PORT="$ELASTICSEARCH_PORT"
    PORT_VARIABLE=ELASTICSEARCH_PORT
    COMPOSE_SERVICE="elasticsearch"
    COMPOSE_PROJECT=(-f "$COMPOSE_FILE" -p "ravenbench-vector-es-$$")
    ;;
esac

has_option() {
  local name="$1"
  shift
  local argument
  for argument in "$@"; do
    if [[ "$argument" == "$name" || "$argument" == "$name="* ]]; then
      return 0
    fi
  done
  return 1
}

value_of() {
  local name="$1"
  shift
  local expect_value=0
  local argument
  for argument in "$@"; do
    if [[ $expect_value -eq 1 ]]; then
      echo "$argument"
      return 0
    fi
    if [[ "$argument" == "$name" ]]; then
      expect_value=1
      continue
    fi
    if [[ "$argument" == "$name="* ]]; then
      echo "${argument#*=}"
      return 0
    fi
  done
  return 1
}

host_of_url() {
  local host_port="${1#*://}"
  host_port="${host_port%%/*}"
  host_port="${host_port##*@}"
  echo "${host_port%%:*}"
}

port_of_url() {
  local host_port="${1#*://}"
  host_port="${host_port%%/*}"
  host_port="${host_port##*@}"
  if [[ "$host_port" == *:* ]]; then
    echo "${host_port##*:}"
  else
    echo "$2"
  fi
}

probe_endpoint() {
  local host="$1" port="$2"
  if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then
    return 0
  fi
  return 1
}

# A Docker-published port accepts a TCP connection before the server inside is ready to answer
# HTTP, so an HTTP target is ready only when its ready path returns 200: /build/version for RavenDB,
# /_license for Elasticsearch, which answers 404 until the cluster has installed its licence.
probe_http_ready() {
  local host="$1" port="$2" path="$3"
  (
    exec 3<>"/dev/tcp/$host/$port" 2>/dev/null || exit 1
    printf 'GET %s HTTP/1.0\r\nHost: %s\r\nConnection: close\r\n\r\n' "$path" "$host" >&3 2>/dev/null || exit 1
    local line
    IFS= read -t 3 -r line <&3 2>/dev/null || exit 1
    [[ "$line" == *" 200 "* ]]
  )
}

is_ravendb_target() {
  [[ "$TARGET" == "ravendb" || "$TARGET" == "ravendb-7" ]]
}

probe_ready() {
  local host="$1" port="$2"
  if is_ravendb_target; then
    probe_http_ready "$host" "$port" /build/version
  elif [[ "$TARGET" == "elasticsearch" ]]; then
    probe_http_ready "$host" "$port" /_license
  else
    probe_endpoint "$host" "$port"
  fi
}

wait_for_endpoint() {
  local host="$1" port="$2"
  local deadline=$((SECONDS + READY_TIMEOUT))
  while (( SECONDS < deadline )); do
    if probe_ready "$host" "$port"; then
      return 0
    fi
    sleep "$READY_POLL_SECONDS"
  done
  return 1
}

docker_cli_available() {
  command -v docker >/dev/null 2>&1
}

docker_daemon_available() {
  docker info >/dev/null 2>&1
}

# The two Docker failures have different fixes, so they never share a message. The check runs
# before the harness build, so a user does not wait for a compile to learn Docker is unusable.
require_docker_to_start() {
  if ! docker_cli_available; then
    echo "error: Docker is not installed and the script must start the '$TARGET' container." >&2
    echo "Install Docker, or start the target on another host and rerun with --url:" >&2
    echo "  docker compose -f benchmarks/vector/docker-compose.yml up -d $COMPOSE_SERVICE" >&2
    echo "  ./benchmarks/vector/run.sh --target $TARGET --url <endpoint>" >&2
    exit 1
  fi

  if ! docker_daemon_available; then
    echo "error: Docker is installed but the daemon is not reachable, and the script must start the '$TARGET' container." >&2
    echo "Join the 'docker' group or run with sudo, or start the target elsewhere and rerun with --url <endpoint>." >&2
    exit 1
  fi
}

STARTED_COMPOSE=0
ES_DATA_RUN_DIR=""
stop_started_container() {
  local status=$?
  if [[ "$STARTED_COMPOSE" -eq 1 ]]; then
    echo "Stopping the $TARGET container the script started."
    if [[ "$TARGET" == "elasticsearch" ]]; then
      # The project is this run's own, so its volume is this run's cluster and goes with it.
      docker compose "${COMPOSE_PROJECT[@]}" down -v >/dev/null 2>&1 || true
    else
      docker compose "${COMPOSE_PROJECT[@]}" rm -s -f "$COMPOSE_SERVICE" >/dev/null 2>&1 || true
    fi
  fi
  if [[ -n "$ES_DATA_RUN_DIR" ]]; then
    rm -rf "$ES_DATA_RUN_DIR"
  fi
  exit "$status"
}
trap stop_started_container EXIT

URL="$DEFAULT_URL"
URL_WAS_GIVEN=0
if has_option --url "${PASSTHROUGH[@]}"; then
  URL="$(value_of --url "${PASSTHROUGH[@]}")"
  URL_WAS_GIVEN=1
fi
HOST="$(host_of_url "$URL")"
PORT="$(port_of_url "$URL" "$DEFAULT_PORT")"

CONSTRAINED=0
if has_option --constrained "${PASSTHROUGH[@]}"; then
  CONSTRAINED=1
  if [[ "$URL_WAS_GIVEN" -eq 1 || -z "$COMPOSE_SERVICE" ]]; then
    echo "error: --constrained limits and restarts the $TARGET container, so it runs only against a container this script starts; it refuses the endpoint $HOST:$PORT." >&2
    exit 2
  fi
fi

# Elasticsearch without --url always gets a fresh cluster, so a port that already answers is not reused.
FRESH_CLUSTER=0
if [[ "$TARGET" == "elasticsearch" && "$URL_WAS_GIVEN" -eq 0 ]]; then
  FRESH_CLUSTER=1
fi

if probe_endpoint "$HOST" "$PORT" && [[ "$FRESH_CLUSTER" -eq 1 || "$CONSTRAINED" -eq 1 ]]; then
  echo "error: port $HOST:$PORT already answers, and this run needs a fresh $TARGET container of its own. Set $PORT_VARIABLE to a free port, or pass --url to use the running server (not with --constrained)." >&2
  exit 1
elif probe_endpoint "$HOST" "$PORT"; then
  echo "Using the $TARGET endpoint that already answers at $HOST:$PORT."
else
  if [[ -z "$COMPOSE_SERVICE" ]]; then
    echo "error: the RavenDB server at $URL did not answer. Start it before running the benchmark." >&2
    exit 1
  fi
  if [[ "$URL_WAS_GIVEN" -eq 1 ]]; then
    # A caller endpoint is respected as given: a dead one fails the run instead of starting a
    # local container for an endpoint the caller did not ask for.
    echo "error: the $TARGET endpoint at $HOST:$PORT did not answer; the script does not start a local container for a caller-supplied --url." >&2
    exit 1
  fi
  require_docker_to_start
  if [[ "$TARGET" == "elasticsearch" && -n "${ELASTICSEARCH_DATA_DIR:-}" ]]; then
    # A fresh data directory per run under the caller's host path; the exit trap removes it.
    ES_DATA_RUN_DIR="$(mktemp -d "$ELASTICSEARCH_DATA_DIR/vector-es-XXXXXX")"
    chmod 777 "$ES_DATA_RUN_DIR"
    export ELASTICSEARCH_DATA_DIR="$ES_DATA_RUN_DIR"
  fi
  echo "Starting the $TARGET container from $COMPOSE_FILE; it will answer at $HOST:$PORT."
  # STARTED_COMPOSE is set before the start, so the exit trap stops a partially started container.
  STARTED_COMPOSE=1
  if ! docker compose "${COMPOSE_PROJECT[@]}" up -d --wait --wait-timeout "$READY_TIMEOUT" "$COMPOSE_SERVICE"; then
    echo "error: the $TARGET container did not become ready within ${READY_TIMEOUT}s." >&2
    exit 1
  fi
fi

if ! wait_for_endpoint "$HOST" "$PORT"; then
  echo "error: the $TARGET endpoint at $HOST:$PORT did not become ready within ${READY_TIMEOUT}s." >&2
  exit 1
fi

RUN_ARGS=(vector --target "$TARGET")

if ! has_option --url "${PASSTHROUGH[@]}"; then
  RUN_ARGS+=(--url "$DEFAULT_URL")
fi

if ! has_option --database "${PASSTHROUGH[@]}"; then
  # A fresh database per invocation; the harness creates it through the target's own connection.
  # Lowercase, because Elasticsearch index names must be.
  RUN_ARGS+=(--database "vector_${TARGET//-/_}_$(date -u +%Y%m%dt%H%M%S)_$$")
fi

if ! has_option --scenario "${PASSTHROUGH[@]}"; then
  RUN_ARGS+=(--scenario "$SCENARIO_DEFAULT")
fi

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
if has_option --output-prefix "${PASSTHROUGH[@]}"; then
  RESULT_PREFIX="$(value_of --output-prefix "${PASSTHROUGH[@]}")"
else
  mkdir -p "$RESULTS_DIR"
  RESULT_PREFIX="$RESULTS_DIR/${TARGET}-${RUN_ID}"
  RUN_ARGS+=(--output-prefix "$RESULT_PREFIX")
fi

RUN_ARGS+=("${PASSTHROUGH[@]}")

echo "Running the vector runs against $TARGET."
VECTOR_STATUS=0
PATH="$HOME/.dotnet:$PATH" dotnet run --project "$REPO_ROOT/src/RavenBench/RavenBench.csproj" -c Release -- "${RUN_ARGS[@]}" || VECTOR_STATUS=$?
if [[ "$VECTOR_STATUS" -ne 0 ]]; then
  # 137 is SIGKILL, which the kernel OOM killer sends.
  echo "error: the vector command against $TARGET exited with status $VECTOR_STATUS before writing every result." >&2
  exit "$VECTOR_STATUS"
fi

if [[ "$CONSTRAINED" -eq 1 ]]; then
  echo "Results: ${RESULT_PREFIX}-constrained.json"
else
  echo "Results: ${RESULT_PREFIX}-<load|recall|readers|filtered|under-insert>.json"
fi
