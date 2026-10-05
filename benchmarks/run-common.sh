# Helpers shared by benchmarks/*/run.sh. Sourced, never run.
#
# The sourcing script sets BENCH (its folder name), COMPOSE_FILE and READY_TIMEOUT, and defines usage.
# After parse_run_options and the target case, it sets DEFAULT_URL, DEFAULT_PORT, COMPOSE_SERVICE and
# READY_PATH (the HTTP path that returns 200 once the server is ready; empty for a TCP probe).

READY_POLL_SECONDS=2
COMPOSE_PROJECT=(-f "$COMPOSE_FILE")
STARTED_COMPOSE=0
# A compose project created for this run alone; teardown removes its volumes with it.
OWN_COMPOSE_PROJECT=0
RUN_DATABASE=""
# Directories the run created; the exit trap removes them.
RUN_DIRS=()

# Sets TARGET and PASSTHROUGH; every option other than --target and --help is forwarded.
parse_run_options() {
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
}

# Exits 2 unless TARGET is one of the arguments.
require_target() {
  local valid="$1" name
  for name in "${@:2}"; do
    valid+=", $name"
  done
  if [[ -z "$TARGET" ]]; then
    echo "error: --target is required. Valid targets: $valid." >&2
    exit 2
  fi
  local candidate
  for candidate in "$@"; do
    [[ "$candidate" == "$TARGET" ]] && return 0
  done
  echo "error: unknown target '$TARGET'. Valid targets: $valid." >&2
  exit 2
}

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

# A host is local when it is a loopback name or address, this machine's host name, or an address of a
# local interface. The interface list comes from `hostname -I`; where that is missing (macOS), only the
# first two rules apply and an interface address counts as remote.
is_local_host() {
  case "$1" in
    localhost|127.*|::1|"[::1]") return 0 ;;
  esac
  [[ "$1" == "${HOSTNAME:-}" ]] && return 0
  local address
  for address in $(hostname -I 2>/dev/null); do
    [[ "$1" == "$address" ]] && return 0
  done
  return 1
}

probe_endpoint() {
  local host="$1" port="$2"
  (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null
}

# A Docker-published port accepts a TCP connection before the server inside is ready, so an HTTP target
# is ready only when READY_PATH returns 200; a target with no READY_PATH is ready when its port accepts.
probe_ready() {
  local host="$1" port="$2"
  if [[ -z "${READY_PATH:-}" ]]; then
    probe_endpoint "$host" "$port"
    return
  fi
  (
    exec 3<>"/dev/tcp/$host/$port" 2>/dev/null || exit 1
    printf 'GET %s HTTP/1.0\r\nHost: %s\r\nConnection: close\r\n\r\n' "$READY_PATH" "$host" >&3 2>/dev/null || exit 1
    line=""
    IFS= read -t 3 -r line <&3 2>/dev/null || exit 1
    [[ "$line" == *" 200 "* ]]
  )
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
    echo "  docker compose -f benchmarks/$BENCH/docker-compose.yml up -d $COMPOSE_SERVICE" >&2
    echo "  ./benchmarks/$BENCH/run.sh --target $TARGET --url <endpoint>" >&2
    exit 1
  fi

  if ! docker_daemon_available; then
    echo "error: Docker is installed but the daemon is not reachable, and the script must start the '$TARGET' container." >&2
    echo "Join the 'docker' group or run with sudo, or start the target elsewhere and rerun with --url <endpoint>." >&2
    exit 1
  fi
}

# A fresh database name per invocation, lowercase with underscores, so every product accepts it
# (Elasticsearch index names must be lowercase).
run_database_name() {
  echo "${BENCH}_${TARGET//-/_}_$(date -u +%Y%m%dt%H%M%S)_$$"
}

# The one teardown rule: a container the script started is stopped. A compose project of the run's
# own goes with its volumes; a shared project keeps its named volume, and the output says so.
stop_started_container() {
  if [[ "$STARTED_COMPOSE" -ne 1 ]]; then
    return 0
  fi
  echo "Stopping the $TARGET container the script started."
  if [[ "$OWN_COMPOSE_PROJECT" -eq 1 ]]; then
    docker compose "${COMPOSE_PROJECT[@]}" down -v >/dev/null 2>&1 || true
    return 0
  fi
  docker compose "${COMPOSE_PROJECT[@]}" rm -s -f "$COMPOSE_SERVICE" >/dev/null 2>&1 || true
  echo "The named volume of the $COMPOSE_SERVICE service keeps the data this run loaded${RUN_DATABASE:+ (database '$RUN_DATABASE')}. Remove the volumes of benchmarks/$BENCH/docker-compose.yml with: docker compose ${COMPOSE_PROJECT[*]} down -v"
}

# The exit trap: teardown never changes the run's exit status.
on_exit() {
  local status=$?
  set +e
  stop_started_container
  local directory
  for directory in "${RUN_DIRS[@]}"; do
    if ! rm -rf "$directory"; then
      echo "warning: could not remove the run directory '$directory'; remove it by hand." >&2
    fi
  done
  exit "$status"
}

# Sets URL, URL_WAS_GIVEN, HOST and PORT from --url or the target default.
resolve_endpoint() {
  URL="$DEFAULT_URL"
  URL_WAS_GIVEN=0
  if has_option --url "${PASSTHROUGH[@]}"; then
    URL="$(value_of --url "${PASSTHROUGH[@]}")"
    URL_WAS_GIVEN=1
  fi
  HOST="$(host_of_url "$URL")"
  PORT="$(port_of_url "$URL" "$DEFAULT_PORT")"
}

# Starts the compose service when the endpoint is dead and the script may, then waits until it is ready.
# FRESH_CONTAINER=1 refuses an endpoint that already answers; a before_compose_start function runs before the start.
start_endpoint_if_needed() {
  if probe_endpoint "$HOST" "$PORT" && [[ "${FRESH_CONTAINER:-0}" -eq 1 ]]; then
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
    if declare -F before_compose_start >/dev/null; then
      before_compose_start
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
}

trap on_exit EXIT
