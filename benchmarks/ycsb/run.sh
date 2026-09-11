#!/usr/bin/env bash
#
# Runs the ycsb benchmark against one target end to end: load, C, A, B and insert-stream.
#
# The script starts the target from this folder's compose file when its endpoint does not already
# answer, waits until the endpoint answers, runs the ycsb command on this folder's scenario, and
# stops the container it started. RavenDB is an external server: the script uses the reachable
# server and starts no container for it. The script addresses only the endpoints this file names.
#
# Every option other than --target is forwarded to the ycsb command unchanged. A caller-supplied
# --scenario, --url, --database or --output-prefix replaces the script default rather than being
# appended twice.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SCENARIO_DEFAULT="$SCRIPT_DIR/scenario.json"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
RESULTS_DIR="$SCRIPT_DIR/results"

# How long the script waits for a target to answer, and how often it probes, in seconds.
READY_TIMEOUT="${YCSB_READY_TIMEOUT:-120}"
READY_POLL_SECONDS=2

usage() {
  cat <<'EOF'
Usage: run.sh --target <ravendb|postgresql|mongodb|documentdb> [options]

Runs the ycsb load, C, A, B and insert-stream runs and writes one result JSON per run under
benchmarks/ycsb/results/. Every other option is forwarded to the ycsb command unchanged.

Examples:
  ./benchmarks/ycsb/run.sh --target ravendb
  ./benchmarks/ycsb/run.sh --target mongodb --seed 7 --doc-count 1000
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
  echo "error: --target is required. Valid targets: ravendb, postgresql, mongodb, documentdb." >&2
  exit 2
fi

case "$TARGET" in
  ravendb|postgresql|mongodb|documentdb) ;;
  *)
    echo "error: unknown target '$TARGET'. Valid targets: ravendb, postgresql, mongodb, documentdb." >&2
    exit 2
    ;;
esac

case "$TARGET" in
  ravendb)
    DEFAULT_URL="http://localhost:8081"
    DEFAULT_PORT=8081
    COMPOSE_SERVICE=""
    ;;
  postgresql)
    DEFAULT_URL="postgresql://bench:bench@localhost:5432/bench"
    DEFAULT_PORT=5432
    COMPOSE_SERVICE="postgresql"
    ;;
  mongodb)
    DEFAULT_URL="mongodb://localhost:27017"
    DEFAULT_PORT=27017
    COMPOSE_SERVICE="mongodb"
    ;;
  documentdb)
    DEFAULT_URL="mongodb://bench:bench@localhost:10260/?tls=true&tlsAllowInvalidCertificates=true"
    DEFAULT_PORT=10260
    COMPOSE_SERVICE="documentdb"
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

wait_for_endpoint() {
  local host="$1" port="$2"
  local deadline=$((SECONDS + READY_TIMEOUT))
  while (( SECONDS < deadline )); do
    if probe_endpoint "$host" "$port"; then
      return 0
    fi
    sleep "$READY_POLL_SECONDS"
  done
  return 1
}

# PostgreSQL has no lazy database creation: the transport connects to the run's database and then
# creates its table, so a fresh run database must exist first. The local container's psql does that.
# MongoDB and DocumentDB create the database on first write, and the RavenDB transport creates it.
ensure_postgres_database() {
  local database="$1"
  local container
  container="$(docker ps --filter "publish=$PORT" --format '{{.ID}}' | head -n 1)"
  if [[ -z "$container" ]]; then
    echo "error: no local PostgreSQL container publishes port $PORT, so the script cannot create database '$database'. Pass --database for an endpoint you manage." >&2
    exit 1
  fi

  local exists
  exists="$(docker exec "$container" psql -U bench -d bench -tAc "SELECT 1 FROM pg_database WHERE datname = '$database'")"
  if [[ "$exists" != "1" ]]; then
    docker exec "$container" psql -U bench -d bench -c "CREATE DATABASE \"$database\""
  fi
}

STARTED_COMPOSE=0
stop_started_container() {
  if [[ "$STARTED_COMPOSE" -eq 1 ]]; then
    echo "Stopping the $TARGET container the script started."
    docker compose -f "$COMPOSE_FILE" rm -s -f "$COMPOSE_SERVICE" >/dev/null 2>&1 || true
  fi
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

if probe_endpoint "$HOST" "$PORT"; then
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
  echo "Starting the $TARGET container from $COMPOSE_FILE."
  docker compose -f "$COMPOSE_FILE" up -d "$COMPOSE_SERVICE"
  STARTED_COMPOSE=1
fi

if ! wait_for_endpoint "$HOST" "$PORT"; then
  echo "error: the $TARGET endpoint at $HOST:$PORT did not become ready within ${READY_TIMEOUT}s." >&2
  exit 1
fi

RUN_ARGS=(ycsb --target "$TARGET")

if ! has_option --url "${PASSTHROUGH[@]}"; then
  RUN_ARGS+=(--url "$DEFAULT_URL")
fi

RUN_DATABASE=""
if ! has_option --database "${PASSTHROUGH[@]}"; then
  # A fresh database per invocation, so a rerun never loads into a keyspace that already holds
  # the bench/ ids and never needs a product-specific cleanup.
  RUN_DATABASE="ycsb_${TARGET}_$(date -u +%Y%m%dT%H%M%S)_$$"
  RUN_ARGS+=(--database "$RUN_DATABASE")
fi

if [[ "$TARGET" == "postgresql" && -n "$RUN_DATABASE" ]]; then
  ensure_postgres_database "$RUN_DATABASE"
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

echo "Running the ycsb sequence against $TARGET."
PATH="$HOME/.dotnet:$PATH" dotnet run --project "$REPO_ROOT/src/RavenBench/RavenBench.csproj" -c Release -- "${RUN_ARGS[@]}"

echo "Results: ${RESULT_PREFIX}-{load,C,A,B,insert-stream}.json"
