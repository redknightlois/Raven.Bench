#!/usr/bin/env bash
#
# Runs the ycsb benchmark against one target end to end: the load run, then the whole set the
# scenario names (the closed-loop ramp, every fixed rate, every distribution, every repetition).
#
# The script is endpoint-driven. It probes the target's endpoint first. It starts a container from
# this folder's compose file only when the caller gave no --url and the target's default localhost
# endpoint does not answer. Every other path completes without Docker on the client, and a caller
# endpoint is used as given.
#
# The external RavenDB development server is never started by this script. The five containerized
# targets (ravendb-6, ravendb-7, postgresql, mongodb, documentdb) are started from the compose file
# when they are needed and are stopped again only when this script started them.
#
# Every option other than --target is forwarded to the ycsb command unchanged. A caller-supplied
# --scenario, --url, --database or --output-prefix replaces the script default rather than being
# appended twice.

set -euo pipefail

# The physical path: MSBuild must see one path per project, and a path through a symbolic link gives it two.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SCENARIO_DEFAULT="$SCRIPT_DIR/scenario.json"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
RESULTS_DIR="$SCRIPT_DIR/results"

BENCH=ycsb
# How long the script waits for a target to answer, in seconds.
READY_TIMEOUT="${YCSB_READY_TIMEOUT:-120}"

usage() {
  cat <<'EOF'
Usage: run.sh --target <ravendb|ravendb-6|ravendb-7|postgresql|mongodb|documentdb> [options]

Runs the ycsb load run and every workload run the scenario names, and writes one result JSON per run under
benchmarks/ycsb/results/. Every other option is forwarded to the ycsb command unchanged.

The script starts a target from benchmarks/ycsb/docker-compose.yml only when no --url was given
and the target's default localhost endpoint does not answer. A caller-supplied --url is used as
given and starts nothing.

Examples:
  ./benchmarks/ycsb/run.sh --target ravendb
  ./benchmarks/ycsb/run.sh --target mongodb --seed 7 --doc-count 1000
  ./benchmarks/ycsb/run.sh --target postgresql --url postgresql://bench:bench@db-host:5432/bench
  ./benchmarks/ycsb/run.sh --target postgresql --transport client
  ./benchmarks/ycsb/run.sh --target postgresql --cross-check

--cross-check runs workload C through --transport raw and --transport client on the postgresql
target over one loaded keyspace, and compares the rows against the run-to-run noise.
EOF
}

source "$SCRIPT_DIR/../run-common.sh"
parse_run_options "$@"
require_target ravendb ravendb-6 ravendb-7 postgresql mongodb documentdb

COMMAND="ycsb"
if has_option --cross-check "${PASSTHROUGH[@]}"; then
  COMMAND="ycsb-crosscheck"
  FORWARDED=()
  for argument in "${PASSTHROUGH[@]}"; do
    [[ "$argument" == "--cross-check" ]] || FORWARDED+=("$argument")
  done
  PASSTHROUGH=("${FORWARDED[@]}")
fi

if [[ "$COMMAND" == "ycsb-crosscheck" && "$TARGET" != "postgresql" ]]; then
  echo "error: --cross-check compares the PostgreSQL transports; target '$TARGET' is not 'postgresql'." >&2
  exit 2
fi

# The host ports of the two containerized RavenDB services are overridable, so a database host can
# move them and a caller can point the script's default endpoint away from a port already in use.
RAVENDB6_PORT="${RAVENDB6_PORT:-8086}"
RAVENDB7_PORT="${RAVENDB7_PORT:-8087}"

case "$TARGET" in
  ravendb)
    DEFAULT_URL="http://localhost:8081"
    DEFAULT_PORT=8081
    COMPOSE_SERVICE=""
    READY_PATH=/build/version
    ;;
  ravendb-6)
    DEFAULT_URL="http://localhost:$RAVENDB6_PORT"
    DEFAULT_PORT="$RAVENDB6_PORT"
    COMPOSE_SERVICE="ravendb-6"
    READY_PATH=/build/version
    ;;
  ravendb-7)
    DEFAULT_URL="http://localhost:$RAVENDB7_PORT"
    DEFAULT_PORT="$RAVENDB7_PORT"
    COMPOSE_SERVICE="ravendb-7"
    READY_PATH=/build/version
    ;;
  postgresql)
    DEFAULT_URL="postgresql://bench:bench@localhost:5432/bench"
    DEFAULT_PORT=5432
    COMPOSE_SERVICE="postgresql"
    READY_PATH=""
    ;;
  mongodb)
    DEFAULT_URL="mongodb://localhost:27017"
    DEFAULT_PORT=27017
    COMPOSE_SERVICE="mongodb"
    READY_PATH=""
    ;;
  documentdb)
    DEFAULT_URL="mongodb://bench:bench@localhost:10260/?tls=true&tlsAllowInvalidCertificates=true"
    DEFAULT_PORT=10260
    COMPOSE_SERVICE="documentdb"
    READY_PATH=""
    ;;
esac

# PostgreSQL has no lazy database creation: the transport connects to the run's database and then
# creates its table, so a fresh run database must exist first. The client's psql creates it when
# present; otherwise, for a local endpoint only, the local container that publishes the endpoint's
# port does; otherwise the run fails and tells the user to create the database on the database host.
ensure_postgres_database() {
  local database="$1"

  if command -v psql >/dev/null 2>&1; then
    create_postgres_database_with_psql "$database"
    return 0
  fi

  if is_local_host "$HOST" && docker_cli_available && docker_daemon_available; then
    if create_postgres_database_with_container "$database"; then
      return 0
    fi
  fi

  echo "error: the run database '$database' does not exist, and this client has neither 'psql' nor, for a local endpoint, a local container that publishes port $PORT to create it." >&2
  echo "Create the database on the database host, then rerun with --database '$database'." >&2
  exit 1
}

create_postgres_database_with_psql() {
  local database="$1"
  local exists
  exists="$(psql "$URL" -tAc "SELECT 1 FROM pg_database WHERE datname = '$database'")"
  if [[ "$exists" != "1" ]]; then
    psql "$URL" -v ON_ERROR_STOP=1 -c "CREATE DATABASE \"$database\""
  fi
}

create_postgres_database_with_container() {
  local database="$1"
  local container
  container="$(docker ps --filter "publish=$PORT" --format '{{.ID}}' | head -n 1)"
  if [[ -z "$container" ]]; then
    return 1
  fi

  local exists
  exists="$(docker exec "$container" psql -U bench -d bench -tAc "SELECT 1 FROM pg_database WHERE datname = '$database'")"
  if [[ "$exists" != "1" ]]; then
    docker exec "$container" psql -U bench -d bench -c "CREATE DATABASE \"$database\""
  fi
}

# A target without a client mode refuses it before any probe or container start.
if [[ "$TARGET" == "mongodb" || "$TARGET" == "documentdb" ]] && has_option --transport "${PASSTHROUGH[@]}"; then
  TRANSPORT="$(value_of --transport "${PASSTHROUGH[@]}")"
  if [[ "$TRANSPORT" != "raw" ]]; then
    echo "error: target '$TARGET' has no '--transport $TRANSPORT' mode; it runs only '--transport raw'." >&2
    exit 2
  fi
fi

resolve_endpoint
start_endpoint_if_needed

RUN_ARGS=("$COMMAND" --target "$TARGET")

if ! has_option --url "${PASSTHROUGH[@]}"; then
  RUN_ARGS+=(--url "$DEFAULT_URL")
fi

if ! has_option --database "${PASSTHROUGH[@]}"; then
  # A fresh database per invocation, so a rerun never loads into a keyspace that already holds
  # the bench/ ids and never needs a product-specific cleanup.
  RUN_DATABASE="$(run_database_name)"
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

if [[ "$COMMAND" == "ycsb-crosscheck" ]]; then
  echo "Results: ${RESULT_PREFIX}-<raw|client>-<run>-closed-<distribution>-rep<n>.json and ${RESULT_PREFIX}-crosscheck.json"
else
  echo "Results: ${RESULT_PREFIX}-<run>-<closed|rate<rate>>-<distribution>-rep<n>.json"
fi
