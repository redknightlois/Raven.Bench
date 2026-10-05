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

BENCH=vector
# How long the script waits for a target to answer, in seconds.
READY_TIMEOUT="${VECTOR_READY_TIMEOUT:-120}"

usage() {
  cat <<'EOF'
Usage: run.sh --target <ravendb|ravendb-7|ravendb-7-default-index|pgvector|elasticsearch> [--constrained] [options]

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

source "$SCRIPT_DIR/../run-common.sh"
parse_run_options "$@"
require_target ravendb ravendb-7 ravendb-7-default-index pgvector elasticsearch

# The host port of the containerized RavenDB service is overridable, so a database host can move it
# and a caller can point the script's default endpoint away from a port already in use.
RAVENDB7_PORT="${RAVENDB7_PORT:-8087}"
PGVECTOR_PORT="${PGVECTOR_PORT:-5432}"
ELASTICSEARCH_PORT="${ELASTICSEARCH_PORT:-9200}"
export ELASTICSEARCH_PORT

case "$TARGET" in
  ravendb)
    DEFAULT_URL="http://localhost:8081"
    DEFAULT_PORT=8081
    COMPOSE_SERVICE=""
    READY_PATH=/build/version
    ;;
  ravendb-7|ravendb-7-default-index)
    DEFAULT_URL="http://localhost:$RAVENDB7_PORT"
    DEFAULT_PORT="$RAVENDB7_PORT"
    PORT_VARIABLE=RAVENDB7_PORT
    COMPOSE_SERVICE="ravendb-7"
    READY_PATH=/build/version
    ;;
  pgvector)
    DEFAULT_URL="postgresql://bench:bench@localhost:$PGVECTOR_PORT/bench"
    DEFAULT_PORT="$PGVECTOR_PORT"
    PORT_VARIABLE=PGVECTOR_PORT
    COMPOSE_SERVICE="pgvector"
    READY_PATH=""
    ;;
  elasticsearch)
    DEFAULT_URL="http://localhost:$ELASTICSEARCH_PORT"
    DEFAULT_PORT="$ELASTICSEARCH_PORT"
    PORT_VARIABLE=ELASTICSEARCH_PORT
    COMPOSE_SERVICE="elasticsearch"
    # Elasticsearch answers /_license with 404 until the cluster has installed its licence.
    READY_PATH=/_license
    COMPOSE_PROJECT=(-f "$COMPOSE_FILE" -p "ravenbench-vector-es-$$")
    # The project is this run's own, so its volume is this run's cluster and goes with it.
    OWN_COMPOSE_PROJECT=1
    ;;
esac

resolve_endpoint

CONSTRAINED=0
if has_option --constrained "${PASSTHROUGH[@]}"; then
  CONSTRAINED=1
  if [[ "$URL_WAS_GIVEN" -eq 1 || -z "$COMPOSE_SERVICE" ]]; then
    echo "error: --constrained limits and restarts the $TARGET container, so it runs only against a container this script starts; it refuses the endpoint $HOST:$PORT." >&2
    exit 2
  fi
fi

# Elasticsearch without --url always gets a fresh cluster, so a port that already answers is not reused.
FRESH_CONTAINER="$CONSTRAINED"
if [[ "$TARGET" == "elasticsearch" && "$URL_WAS_GIVEN" -eq 0 ]]; then
  FRESH_CONTAINER=1
fi

before_compose_start() {
  if [[ "$TARGET" == "elasticsearch" && -n "${ELASTICSEARCH_DATA_DIR:-}" ]]; then
    # A fresh data directory per run under the caller's host path; the exit trap removes it.
    ES_DATA_RUN_DIR="$(mktemp -d "$ELASTICSEARCH_DATA_DIR/vector-es-XXXXXX")"
    RUN_DIRS+=("$ES_DATA_RUN_DIR")
    chmod 777 "$ES_DATA_RUN_DIR"
    export ELASTICSEARCH_DATA_DIR="$ES_DATA_RUN_DIR"
  fi
}

start_endpoint_if_needed

RUN_ARGS=(vector --target "$TARGET")

if ! has_option --url "${PASSTHROUGH[@]}"; then
  RUN_ARGS+=(--url "$DEFAULT_URL")
fi

if ! has_option --database "${PASSTHROUGH[@]}"; then
  # A fresh database per invocation; the harness creates it through the target's own connection.
  RUN_DATABASE="$(run_database_name)"
  RUN_ARGS+=(--database "$RUN_DATABASE")
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
