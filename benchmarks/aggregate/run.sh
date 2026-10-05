#!/usr/bin/env bash
#
# Runs the aggregate benchmark against one target end to end: build, count-by-category,
# sum-by-region, filtered-group and under-write, one result JSON per run.
#
# The script is endpoint-driven. It probes the target's endpoint first. It starts a container from
# this folder's compose file only when the caller gave no --url and the target's default localhost
# endpoint does not answer. Every other path completes without Docker on the client, and a caller
# endpoint is used as given.
#
# The external RavenDB development server is never started by this script. The two containerized
# targets (ravendb-7, and mongodb for both MongoDB targets) are started from the compose file when they are needed and are
# stopped again only when this script started them.
#
# Every option other than --target is forwarded to the aggregate command unchanged. A caller-supplied
# --scenario, --url, --database or --output-prefix replaces the script default rather than being
# appended twice.

set -euo pipefail

# The physical path: MSBuild must see one path per project, and a path through a symbolic link gives it two.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SCENARIO_DEFAULT="$SCRIPT_DIR/scenario.json"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
RESULTS_DIR="$SCRIPT_DIR/results"

BENCH=aggregate
# How long the script waits for a target to answer, in seconds.
READY_TIMEOUT="${AGGREGATE_READY_TIMEOUT:-120}"

usage() {
  cat <<'EOF'
Usage: run.sh --target <ravendb|ravendb-7|mongodb|mongodb-indexed> [options]

Runs build, count-by-category, sum-by-region, filtered-group and under-write, and writes one result
JSON per run under benchmarks/aggregate/results/. Every other option is forwarded to the aggregate command unchanged.

The script starts a target from benchmarks/aggregate/docker-compose.yml only when no --url was given
and the target's default localhost endpoint does not answer. A caller-supplied --url is used as
given and starts nothing.

Examples:
  ./benchmarks/aggregate/run.sh --target ravendb
  ./benchmarks/aggregate/run.sh --target mongodb-indexed --documents 20000 --write-rate 200
  ./benchmarks/aggregate/run.sh --target mongodb --url mongodb://db-host:27017 --node-exporter-url http://db-host:9100/metrics
EOF
}

source "$SCRIPT_DIR/../run-common.sh"
parse_run_options "$@"
require_target ravendb ravendb-7 mongodb mongodb-indexed

# The host port of the containerized RavenDB service is overridable, so a database host can move it
# and a caller can point the script's default endpoint away from a port already in use.
RAVENDB7_PORT="${RAVENDB7_PORT:-8087}"
MONGODB_PORT="${MONGODB_PORT:-27017}"

case "$TARGET" in
  ravendb)
    DEFAULT_URL="http://localhost:8081"
    DEFAULT_PORT=8081
    COMPOSE_SERVICE=""
    READY_PATH=/build/version
    ;;
  ravendb-7)
    DEFAULT_URL="http://localhost:$RAVENDB7_PORT"
    DEFAULT_PORT="$RAVENDB7_PORT"
    COMPOSE_SERVICE="ravendb-7"
    READY_PATH=/build/version
    ;;
  mongodb|mongodb-indexed)
    # Both MongoDB targets run on one server; only the indexes the harness creates differ.
    DEFAULT_URL="mongodb://localhost:$MONGODB_PORT"
    DEFAULT_PORT="$MONGODB_PORT"
    COMPOSE_SERVICE="mongodb"
    READY_PATH=""
    ;;
esac

resolve_endpoint
start_endpoint_if_needed

RUN_ARGS=(aggregate --target "$TARGET")

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

echo "Running the aggregate runs against $TARGET."
AGGREGATE_STATUS=0
PATH="$HOME/.dotnet:$PATH" dotnet run --project "$REPO_ROOT/src/RavenBench/RavenBench.csproj" -c Release -- "${RUN_ARGS[@]}" || AGGREGATE_STATUS=$?
if [[ "$AGGREGATE_STATUS" -ne 0 ]]; then
  # 137 is SIGKILL, which the kernel OOM killer sends.
  echo "error: the aggregate command against $TARGET exited with status $AGGREGATE_STATUS before writing every result." >&2
  exit "$AGGREGATE_STATUS"
fi

echo "Results: ${RESULT_PREFIX}-<build|count-by-category|sum-by-region|filtered-group|under-write>.json"
