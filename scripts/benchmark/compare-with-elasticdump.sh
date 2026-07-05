#!/usr/bin/env bash
set -euo pipefail

# Maintainer-only benchmark for comparing elasticdump-rs with the original
# Node.js elasticdump. The default Elasticsearch setup models this project's
# production dump target: a static, read-optimized index that uses the default
# Elasticsearch codec and is force-merged before timed export runs.
#
# Docker note: local benchmark quality depends on filesystem cache. If you use
# this repo's docker-compose.yaml, recreate the Elasticsearch container after
# compose memory changes so the heap/cache balance matches the benchmark intent.
#
# CPU-core limiting: --cpu-cores N pins each timed tool run to the first N
# cores via taskset on Linux. macOS has no core pinning, so there N is
# enforced as an aggregate N*100% duty-cycle cap via cpulimit (brew install
# cpulimit); macOS wall-clock numbers under this option are indicative, not
# authoritative.

ES_URL="${ES_URL:-http://localhost:9200}"
BENCH_DOCS="${BENCH_DOCS:-2000000}"
BENCH_BULK_SIZE="${BENCH_BULK_SIZE:-5000}"
BENCH_LIMIT="${BENCH_LIMIT:-10000}"
BENCH_TEXT_BYTES="${BENCH_TEXT_BYTES:-256}"
BENCH_WARMUP_RUNS="${BENCH_WARMUP_RUNS:-1}"
BENCH_MEASURED_RUNS="${BENCH_MEASURED_RUNS:-2}"
BENCH_KEEP_ARTIFACTS="${BENCH_KEEP_ARTIFACTS:-0}"
BENCH_CODEC="${BENCH_CODEC:-default}"
BENCH_FORCE_MERGE="${BENCH_FORCE_MERGE:-1}"
BENCH_MAX_NUM_SEGMENTS="${BENCH_MAX_NUM_SEGMENTS:-1}"
BENCH_USE_EXPLICIT_IDS="${BENCH_USE_EXPLICIT_IDS:-1}"
BENCH_SEARCH_TYPES="${BENCH_SEARCH_TYPES:-scroll,pit}"
BENCH_SCROLL_TIME="${BENCH_SCROLL_TIME:-10m}"
BENCH_PIT_KEEP_ALIVE="${BENCH_PIT_KEEP_ALIVE:-10m}"
BENCH_INDEX_PREFIX="${BENCH_INDEX_PREFIX:-elasticdump_rs_bench}"
BENCH_INDEX_NAME="${BENCH_INDEX_NAME:-}"
BENCH_WORKDIR="${BENCH_WORKDIR:-}"
BENCH_RS_BIN="${BENCH_RS_BIN:-}"
BENCH_ELASTICDUMP_CMD="${BENCH_ELASTICDUMP_CMD:-elasticdump}"
BENCH_CPU_CORES="${BENCH_CPU_CORES:-}"
BENCH_RS_SLICES="${BENCH_RS_SLICES:-}"
BENCH_RS_WORKERS="${BENCH_RS_WORKERS:-}"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
SCRIPT_NAME="$(basename -- "${BASH_SOURCE[0]}")"

WORKDIR=""
WORKDIR_CREATED=0
BENCH_INDEX=""
PYTHON_BIN=""
RS_BIN=""
declare -a ELASTICDUMP_CMD=()
declare -a BENCH_SEARCH_TYPE_LIST=()
declare -a BENCH_VARIANTS=()
declare -a CPU_LIMIT_WRAPPER=()

log() {
  printf '[bench] %s\n' "$*"
}

die() {
  printf '[bench] error: %s\n' "$*" >&2
  exit 1
}

print_usage() {
  cat <<EOF
Usage: ${SCRIPT_NAME} [options]

Options:
  --es-url URL                Elasticsearch base URL
  --docs N                    Number of seeded documents
  --bulk-size N               Bulk indexing batch size
  --limit N                   Dump batch size for both tools
  --text-bytes N              Approximate ASCII bytes for message payload
  --warmup-runs N             Warmup runs per tool
  --measured-runs N           Measured runs per tool
  --keep-artifacts 0|1        Keep benchmark index and files after exit
  --codec default|best_compression
                              Benchmark index codec; default omits index.codec
  --force-merge 0|1           Force-merge the read-only index before timed runs
  --max-num-segments N        max_num_segments for force-merge
  --explicit-ids 0|1          Seed deterministic document IDs
  --search-types TYPES        Comma-separated search types: scroll,pit
  --scroll-time TIME          Scroll keepalive passed to scroll variants
  --pit-keep-alive TIME       PIT keepalive passed to PIT variants
  --index-name NAME           Explicit benchmark index name
  --index-prefix PREFIX       Prefix for generated benchmark index names
  --workdir DIR               Artifact work directory
  --rs-bin PATH               Path to the elasticdump-rs binary
  --elasticdump-cmd CMD       Command used to invoke the original elasticdump
  --cpu-cores N               Limit each timed tool run to N CPU cores
                              (Linux: taskset affinity; macOS: cpulimit
                              duty-cycle cap; empty = unlimited)
  --rs-slices N               Pass --slices N to elasticdump-rs runs
                              (0 or empty = unsliced; N >= 2 enables sliced
                              retrieval and suffixes rs variant labels with
                              -sN; Elasticsearch rejects slice.max=1)
  --rs-workers N              Pass --workers N to elasticdump-rs runs
                              (empty = match --rs-slices when sliced,
                              otherwise the tool default)
  --help                      Show this help text

Environment variables with the same names are also supported.
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --es-url)
        require_option_value "$@"
        ES_URL="$2"
        shift 2
        ;;
      --docs)
        require_option_value "$@"
        BENCH_DOCS="$2"
        shift 2
        ;;
      --bulk-size)
        require_option_value "$@"
        BENCH_BULK_SIZE="$2"
        shift 2
        ;;
      --limit)
        require_option_value "$@"
        BENCH_LIMIT="$2"
        shift 2
        ;;
      --text-bytes)
        require_option_value "$@"
        BENCH_TEXT_BYTES="$2"
        shift 2
        ;;
      --warmup-runs)
        require_option_value "$@"
        BENCH_WARMUP_RUNS="$2"
        shift 2
        ;;
      --measured-runs)
        require_option_value "$@"
        BENCH_MEASURED_RUNS="$2"
        shift 2
        ;;
      --keep-artifacts)
        require_option_value "$@"
        BENCH_KEEP_ARTIFACTS="$2"
        shift 2
        ;;
      --codec)
        require_option_value "$@"
        BENCH_CODEC="$2"
        shift 2
        ;;
      --force-merge)
        require_option_value "$@"
        BENCH_FORCE_MERGE="$2"
        shift 2
        ;;
      --max-num-segments)
        require_option_value "$@"
        BENCH_MAX_NUM_SEGMENTS="$2"
        shift 2
        ;;
      --explicit-ids)
        require_option_value "$@"
        BENCH_USE_EXPLICIT_IDS="$2"
        shift 2
        ;;
      --search-types)
        require_option_value "$@"
        BENCH_SEARCH_TYPES="$2"
        shift 2
        ;;
      --scroll-time)
        require_option_value "$@"
        BENCH_SCROLL_TIME="$2"
        shift 2
        ;;
      --pit-keep-alive)
        require_option_value "$@"
        BENCH_PIT_KEEP_ALIVE="$2"
        shift 2
        ;;
      --index-name)
        require_option_value "$@"
        BENCH_INDEX_NAME="$2"
        shift 2
        ;;
      --index-prefix)
        require_option_value "$@"
        BENCH_INDEX_PREFIX="$2"
        shift 2
        ;;
      --workdir)
        require_option_value "$@"
        BENCH_WORKDIR="$2"
        shift 2
        ;;
      --rs-bin)
        require_option_value "$@"
        BENCH_RS_BIN="$2"
        shift 2
        ;;
      --elasticdump-cmd)
        require_option_value "$@"
        BENCH_ELASTICDUMP_CMD="$2"
        shift 2
        ;;
      --cpu-cores)
        require_option_value "$@"
        BENCH_CPU_CORES="$2"
        shift 2
        ;;
      --rs-slices)
        require_option_value "$@"
        BENCH_RS_SLICES="$2"
        shift 2
        ;;
      --rs-workers)
        require_option_value "$@"
        BENCH_RS_WORKERS="$2"
        shift 2
        ;;
      --help)
        print_usage
        exit 0
        ;;
      *)
        die "Unknown argument: $1"
        ;;
    esac
  done
}

require_option_value() {
  local option="$1"

  [[ $# -ge 2 ]] || die "Missing value for ${option}"
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

resolve_command_path() {
  local command_name="$1"
  local resolved_path

  resolved_path="$(command -v "${command_name}")" || die "Missing required command: ${command_name}"
  printf '%s\n' "${resolved_path}"
}

es_url() {
  local path="${1:-}"
  printf '%s%s\n' "${ES_URL%/}" "${path}"
}

parse_shell_words() {
  local raw="$1"
  local item

  while IFS= read -r item; do
    ELASTICDUMP_CMD+=("$item")
  done < <(
    "${PYTHON_BIN}" - "$raw" <<'PY'
import shlex
import sys

for part in shlex.split(sys.argv[1]):
    print(part)
PY
  )
}

validate_uint() {
  local name="$1"
  local value="$2"

  [[ "$value" =~ ^[0-9]+$ ]] || die "${name} must be an unsigned integer, got: ${value}"
  [[ ! "$value" =~ ^0[0-9]+$ ]] || die "${name} must not use leading-zero notation, got: ${value}"
}

cpu_limit_platform() {
  local kernel_name

  kernel_name="$(uname -s)"
  case "${kernel_name}" in
    Linux)
      printf 'linux\n'
      ;;
    Darwin)
      printf 'darwin\n'
      ;;
    *)
      die "BENCH_CPU_CORES is not supported on this platform: ${kernel_name}"
      ;;
  esac
}

available_cpu_cores() {
  local platform="$1"

  if [[ "${platform}" == "linux" ]]; then
    nproc
  else
    sysctl -n hw.ncpu
  fi
}

split_search_types() {
  local raw="$1"
  local part

  BENCH_SEARCH_TYPE_LIST=()
  IFS=',' read -r -a BENCH_SEARCH_TYPE_LIST <<< "${raw}"

  for part in "${BENCH_SEARCH_TYPE_LIST[@]}"; do
    [[ -n "${part}" ]] || die "BENCH_SEARCH_TYPES must not contain empty entries"
    case "${part}" in
      scroll | pit) ;;
      *)
        die "BENCH_SEARCH_TYPES entries must be scroll or pit, got: ${part}"
        ;;
    esac
  done
}

validate_index_name() {
  local value="$1"

  [[ -n "${value}" ]] || die "BENCH_INDEX must not be empty"
  [[ "${value}" != .* ]] || die "BENCH_INDEX must not start with '.', got: ${value}"
  [[ "${value}" != -* ]] || die "BENCH_INDEX must not start with '-', got: ${value}"
  [[ "${value}" != +* ]] || die "BENCH_INDEX must not start with '+', got: ${value}"
  [[ ! "${value}" =~ [%,#/\\*?\"] ]] || die "BENCH_INDEX must be a single safe index name, got: ${value}"
  [[ ! "${value}" =~ [[:space:]] ]] || die "BENCH_INDEX must not contain whitespace, got: ${value}"
  [[ "${value}" != */* ]] || die "BENCH_INDEX must not contain path separators, got: ${value}"
  [[ "${value}" != *'<'* ]] || die "BENCH_INDEX must not be a special target expression, got: ${value}"
  [[ "${value}" != *'>'* ]] || die "BENCH_INDEX must not be a special target expression, got: ${value}"
  [[ ! "${value}" =~ ^(_all|\*)$ ]] || die "BENCH_INDEX must not be a multi-target expression, got: ${value}"
}

validate_config() {
  local cpu_limit_platform_name
  local available_cores

  validate_uint "BENCH_DOCS" "${BENCH_DOCS}"
  validate_uint "BENCH_BULK_SIZE" "${BENCH_BULK_SIZE}"
  validate_uint "BENCH_LIMIT" "${BENCH_LIMIT}"
  validate_uint "BENCH_TEXT_BYTES" "${BENCH_TEXT_BYTES}"
  validate_uint "BENCH_WARMUP_RUNS" "${BENCH_WARMUP_RUNS}"
  validate_uint "BENCH_MEASURED_RUNS" "${BENCH_MEASURED_RUNS}"
  validate_uint "BENCH_MAX_NUM_SEGMENTS" "${BENCH_MAX_NUM_SEGMENTS}"

  case "${BENCH_KEEP_ARTIFACTS}" in
    0 | 1) ;;
    *)
      die "BENCH_KEEP_ARTIFACTS must be 0 or 1, got: ${BENCH_KEEP_ARTIFACTS}"
      ;;
  esac

  case "${BENCH_CODEC}" in
    default | best_compression) ;;
    *)
      die "BENCH_CODEC must be default or best_compression, got: ${BENCH_CODEC}"
      ;;
  esac

  case "${BENCH_FORCE_MERGE}" in
    0 | 1) ;;
    *)
      die "BENCH_FORCE_MERGE must be 0 or 1, got: ${BENCH_FORCE_MERGE}"
      ;;
  esac

  case "${BENCH_USE_EXPLICIT_IDS}" in
    0 | 1) ;;
    *)
      die "BENCH_USE_EXPLICIT_IDS must be 0 or 1, got: ${BENCH_USE_EXPLICIT_IDS}"
      ;;
  esac

  split_search_types "${BENCH_SEARCH_TYPES}"

  (( BENCH_BULK_SIZE > 0 )) || die "BENCH_BULK_SIZE must be greater than zero"
  (( BENCH_LIMIT > 0 )) || die "BENCH_LIMIT must be greater than zero"
  (( BENCH_MAX_NUM_SEGMENTS > 0 )) || die "BENCH_MAX_NUM_SEGMENTS must be greater than zero"

  if [[ -n "${BENCH_CPU_CORES}" ]]; then
    validate_uint "BENCH_CPU_CORES" "${BENCH_CPU_CORES}"
    (( BENCH_CPU_CORES > 0 )) || die "BENCH_CPU_CORES must be greater than zero"
    cpu_limit_platform_name="$(cpu_limit_platform)"
    available_cores="$(available_cpu_cores "${cpu_limit_platform_name}")"
    (( BENCH_CPU_CORES <= available_cores )) || die "BENCH_CPU_CORES must not exceed available CPU cores (${available_cores}), got: ${BENCH_CPU_CORES}"
  fi

  if [[ -n "${BENCH_RS_SLICES}" ]]; then
    validate_uint "BENCH_RS_SLICES" "${BENCH_RS_SLICES}"
    (( BENCH_RS_SLICES != 1 )) \
      || die "BENCH_RS_SLICES must be 0 (unsliced) or >= 2 (Elasticsearch rejects slice.max=1)"
  fi
  if [[ -n "${BENCH_RS_WORKERS}" ]]; then
    validate_uint "BENCH_RS_WORKERS" "${BENCH_RS_WORKERS}"
    (( BENCH_RS_WORKERS > 0 )) || die "BENCH_RS_WORKERS must be greater than zero"
  fi
}

# Sliced retrieval is engaged only for an explicit slice count >= 2; 0 or
# empty keeps the historical unsliced invocation (the tool's own default).
rs_slices_enabled() {
  [[ -n "${BENCH_RS_SLICES}" ]] && (( BENCH_RS_SLICES >= 2 ))
}

resolve_python() {
  PYTHON_BIN="$(resolve_command_path python3)"
}

create_index_body() {
  "${PYTHON_BIN}" - "${BENCH_CODEC}" <<'PY'
import json
import sys

codec = sys.argv[1]

index_settings = {
    "number_of_shards": 2,
    "number_of_replicas": 0,
    "refresh_interval": "-1",
}
if codec != "default":
    index_settings["codec"] = codec

body = {
    "settings": {
        "index": index_settings,
    },
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "@timestamp": {"type": "date"},
            "level": {"type": "keyword"},
            "service": {"type": "keyword"},
            "host": {"type": "keyword"},
            "message": {"type": "text"},
            "trace_id": {"type": "keyword"},
            "span_id": {"type": "keyword"},
            "request_id": {"type": "keyword"},
            "env": {"type": "keyword"},
            "region": {"type": "keyword"},
            "metadata": {
                "properties": {
                    "attempt": {"type": "integer"},
                    "bytes": {"type": "integer"},
                    "status": {"type": "integer"},
                    "success": {"type": "boolean"},
                    "source": {"type": "keyword"},
                }
            },
        },
    },
}

print(json.dumps(body, separators=(",", ":")))
PY
}

delete_index_if_exists() {
  local response_file
  local http_code

  response_file="$(mktemp "${WORKDIR}/delete-index.XXXXXX.json")"
  http_code="$(
    curl --silent --show-error \
      --output "${response_file}" \
      --write-out '%{http_code}' \
      --request DELETE \
      "$(es_url "/${BENCH_INDEX}")"
  )"

  case "${http_code}" in
    200 | 202)
      rm -f -- "${response_file}"
      return 0
      ;;
    404)
      rm -f -- "${response_file}"
      return 0
      ;;
    *)
      log "Unexpected response while deleting index ${BENCH_INDEX} (HTTP ${http_code})"
      cat -- "${response_file}" >&2
      rm -f -- "${response_file}"
      return 1
      ;;
  esac
}

create_index() {
  local response_file
  local http_code

  response_file="$(mktemp "${WORKDIR}/create-index.XXXXXX.json")"
  http_code="$(
    curl --silent --show-error \
      --output "${response_file}" \
      --write-out '%{http_code}' \
      --request PUT \
      --header 'Content-Type: application/json' \
      --data-binary "$(create_index_body)" \
      "$(es_url "/${BENCH_INDEX}")"
  )"

  case "${http_code}" in
    200 | 201)
      rm -f -- "${response_file}"
      ;;
    *)
      cat -- "${response_file}" >&2
      rm -f -- "${response_file}"
      die "Failed to create benchmark index ${BENCH_INDEX} (HTTP ${http_code})"
      ;;
  esac
}

generate_bulk_batch() {
  local start_doc="$1"
  local batch_size="$2"
  local output_file="$3"

  "${PYTHON_BIN}" - "${BENCH_INDEX}" "${start_doc}" "${batch_size}" "${BENCH_TEXT_BYTES}" "${BENCH_USE_EXPLICIT_IDS}" "${output_file}" <<'PY'
import json
import sys

index_name = sys.argv[1]
start_doc = int(sys.argv[2])
batch_size = int(sys.argv[3])
text_bytes = int(sys.argv[4])
use_explicit_ids = sys.argv[5] == "1"
output_file = sys.argv[6]

LEVELS = ("INFO", "WARN", "ERROR", "DEBUG")
SERVICES = ("api", "worker", "ingest", "search")
HOSTS = ("host-a", "host-b", "host-c", "host-d")
REGIONS = ("us-east-1", "us-west-2", "eu-central-1")
ENVS = ("prod", "staging")


def build_message(doc_id: int, size: int) -> str:
    prefix = f"log-{doc_id:08d}-"
    if size <= len(prefix):
        return prefix[:size]
    pattern = "abcdefghijklmnopqrstuvwxyz0123456789"
    needed = size - len(prefix)
    repeats = (needed + len(pattern) - 1) // len(pattern)
    return prefix + (pattern * repeats)[:needed]


with open(output_file, "w", encoding="ascii", newline="\n") as handle:
    for doc_id in range(start_doc, start_doc + batch_size):
        source = {
            "@timestamp": f"2026-01-01T00:{(doc_id // 60) % 60:02d}:{doc_id % 60:02d}Z",
            "level": LEVELS[doc_id % len(LEVELS)],
            "service": SERVICES[doc_id % len(SERVICES)],
            "host": HOSTS[doc_id % len(HOSTS)],
            "message": build_message(doc_id, text_bytes),
            "trace_id": f"{doc_id:032x}",
            "span_id": f"{doc_id:016x}",
            "request_id": f"req-{doc_id:08d}",
            "env": ENVS[doc_id % len(ENVS)],
            "region": REGIONS[doc_id % len(REGIONS)],
            "metadata": {
                "attempt": (doc_id % 5) + 1,
                "bytes": text_bytes,
                "status": 200 + (doc_id % 5),
                "success": (doc_id % 7) != 0,
                "source": "benchmark-seed",
            },
        }
        index_action = {"_index": index_name}
        if use_explicit_ids:
            index_action["_id"] = f"doc-{doc_id:08d}"
        action = {"index": index_action}
        handle.write(json.dumps(action, separators=(",", ":")))
        handle.write("\n")
        handle.write(json.dumps(source, separators=(",", ":")))
        handle.write("\n")
PY
}

validate_bulk_response() {
  local response_file="$1"
  local expected_items="$2"

  "${PYTHON_BIN}" - "${response_file}" "${expected_items}" <<'PY'
import json
import sys

response_file = sys.argv[1]
expected_items = int(sys.argv[2])

with open(response_file, "r", encoding="utf-8") as handle:
    payload = json.load(handle)

items = payload.get("items")
if payload.get("errors"):
    print("Bulk response reported item failures", file=sys.stderr)
    sys.exit(1)
if not isinstance(items, list):
    print("Bulk response missing items array", file=sys.stderr)
    sys.exit(1)
if len(items) != expected_items:
    print(
        f"Bulk response item count mismatch: expected {expected_items}, got {len(items)}",
        file=sys.stderr,
    )
    sys.exit(1)
for idx, item in enumerate(items):
    entry = item.get("index")
    if not isinstance(entry, dict):
        print(f"Bulk response item {idx} missing index result", file=sys.stderr)
        sys.exit(1)
    status = entry.get("status")
    if not isinstance(status, int) or status < 200 or status >= 300:
        print(f"Bulk response item {idx} failed with status {status}", file=sys.stderr)
        sys.exit(1)
PY
}

seed_index() {
  local start_doc=0
  local remaining="${BENCH_DOCS}"

  while (( remaining > 0 )); do
    local batch_size="${BENCH_BULK_SIZE}"
    local seed_file
    local response_file
    local http_code

    if (( remaining < batch_size )); then
      batch_size="${remaining}"
    fi

    seed_file="$(mktemp "${WORKDIR}/seed-batch.XXXXXX.ndjson")"
    response_file="$(mktemp "${WORKDIR}/seed-response.XXXXXX.json")"

    generate_bulk_batch "${start_doc}" "${batch_size}" "${seed_file}"
    http_code="$(
      curl --silent --show-error \
        --output "${response_file}" \
        --write-out '%{http_code}' \
        --request POST \
        --header 'Content-Type: application/x-ndjson' \
        --data-binary "@${seed_file}" \
        "$(es_url "/_bulk")"
    )"

    if [[ "${http_code}" != "200" ]]; then
      cat -- "${response_file}" >&2
      rm -f -- "${seed_file}" "${response_file}"
      die "Bulk seed request failed (HTTP ${http_code})"
    fi

    validate_bulk_response "${response_file}" "${batch_size}"
    rm -f -- "${seed_file}" "${response_file}"

    start_doc=$(( start_doc + batch_size ))
    remaining=$(( remaining - batch_size ))
  done
}

prepare_index_for_reads() {
  local settings_response
  local merge_response
  local refresh_response
  local settings_code
  local merge_code
  local refresh_code
  local settings_body

  settings_response="$(mktemp "${WORKDIR}/refresh-settings.XXXXXX.json")"
  merge_response="$(mktemp "${WORKDIR}/force-merge.XXXXXX.json")"
  refresh_response="$(mktemp "${WORKDIR}/refresh-index.XXXXXX.json")"

  if [[ "${BENCH_FORCE_MERGE}" == "1" ]]; then
    settings_body='{"index.refresh_interval":"1s","index.blocks.write":true}'
  else
    settings_body='{"index.refresh_interval":"1s"}'
  fi

  settings_code="$(
    curl --silent --show-error \
      --output "${settings_response}" \
      --write-out '%{http_code}' \
      --request PUT \
      --header 'Content-Type: application/json' \
      --data "${settings_body}" \
      "$(es_url "/${BENCH_INDEX}/_settings")"
  )"

  if [[ "${settings_code}" != "200" ]]; then
    cat -- "${settings_response}" >&2
    rm -f -- "${settings_response}" "${merge_response}" "${refresh_response}"
    die "Failed to prepare read settings on ${BENCH_INDEX} (HTTP ${settings_code})"
  fi

  if [[ "${BENCH_FORCE_MERGE}" == "1" ]]; then
    merge_code="$(
      curl --silent --show-error \
        --output "${merge_response}" \
        --write-out '%{http_code}' \
        --request POST \
        "$(es_url "/${BENCH_INDEX}/_forcemerge?max_num_segments=${BENCH_MAX_NUM_SEGMENTS}")"
    )"

    if [[ "${merge_code}" != "200" ]]; then
      cat -- "${merge_response}" >&2
      rm -f -- "${settings_response}" "${merge_response}" "${refresh_response}"
      die "Failed to force-merge benchmark index ${BENCH_INDEX} (HTTP ${merge_code})"
    fi
  fi

  refresh_code="$(
    curl --silent --show-error \
      --output "${refresh_response}" \
      --write-out '%{http_code}' \
      --request POST \
      "$(es_url "/${BENCH_INDEX}/_refresh")"
  )"

  if [[ "${refresh_code}" != "200" ]]; then
    cat -- "${refresh_response}" >&2
    rm -f -- "${settings_response}" "${merge_response}" "${refresh_response}"
    die "Failed to refresh benchmark index ${BENCH_INDEX} (HTTP ${refresh_code})"
  fi

  rm -f -- "${settings_response}" "${merge_response}" "${refresh_response}"
}

count_index_documents() {
  local response_file
  local http_code

  response_file="$(mktemp "${WORKDIR}/count-index.XXXXXX.json")"
  http_code="$(
    curl --silent --show-error \
      --output "${response_file}" \
      --write-out '%{http_code}' \
      --request GET \
      "$(es_url "/${BENCH_INDEX}/_count")"
  )"

  if [[ "${http_code}" != "200" ]]; then
    cat -- "${response_file}" >&2
    rm -f -- "${response_file}"
    die "Failed to count documents in ${BENCH_INDEX} (HTTP ${http_code})"
  fi

  "${PYTHON_BIN}" - "${response_file}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    payload = json.load(handle)

count = payload.get("count")
if not isinstance(count, int):
    print("Count response missing integer count", file=sys.stderr)
    sys.exit(1)

print(count)
PY
  rm -f -- "${response_file}"
}

resolve_elasticdump_command() {
  parse_shell_words "${BENCH_ELASTICDUMP_CMD}"
  (( ${#ELASTICDUMP_CMD[@]} > 0 )) || die "BENCH_ELASTICDUMP_CMD did not produce a runnable command"
  require_command "${ELASTICDUMP_CMD[0]}"
}

resolve_rs_binary() {
  if [[ -n "${BENCH_RS_BIN}" ]]; then
    [[ -x "${BENCH_RS_BIN}" ]] || die "BENCH_RS_BIN is not executable: ${BENCH_RS_BIN}"
    RS_BIN="${BENCH_RS_BIN}"
    return
  fi

  if [[ -x "${REPO_ROOT}/target/release/elasticdump-rs" ]]; then
    RS_BIN="${REPO_ROOT}/target/release/elasticdump-rs"
    return
  fi

  require_command cargo
  log "Building release elasticdump-rs binary"
  cargo build --release --bin elasticdump-rs --manifest-path "${REPO_ROOT}/Cargo.toml"
  RS_BIN="${REPO_ROOT}/target/release/elasticdump-rs"
}

resolve_cpu_limit_wrapper() {
  local platform

  CPU_LIMIT_WRAPPER=()
  [[ -n "${BENCH_CPU_CORES}" ]] || return 0

  platform="$(cpu_limit_platform)"
  if [[ "${platform}" == "linux" ]]; then
    require_command taskset
    if (( BENCH_CPU_CORES == 1 )); then
      CPU_LIMIT_WRAPPER=(taskset -c 0)
    else
      CPU_LIMIT_WRAPPER=(taskset -c "0-$((BENCH_CPU_CORES - 1))")
    fi
  else
    command -v cpulimit >/dev/null 2>&1 \
      || die "Missing required command: cpulimit (install with: brew install cpulimit)"
    CPU_LIMIT_WRAPPER=(cpulimit "--limit=$((BENCH_CPU_CORES * 100))" --include-children --)
    log "macOS CPU limit uses cpulimit duty-cycling (aggregate ${BENCH_CPU_CORES}00% cap, not core pinning); wall-clock results are indicative"
  fi

  "${CPU_LIMIT_WRAPPER[@]}" true >/dev/null 2>&1 \
    || die "CPU limit wrapper failed self-test: ${CPU_LIMIT_WRAPPER[*]}"
}

count_file_lines() {
  local file_path="$1"
  local line_count

  [[ -f "${file_path}" ]] || die "Expected output file does not exist: ${file_path}"
  line_count="$(wc -l < "${file_path}")"
  line_count="${line_count//[[:space:]]/}"
  printf '%s\n' "${line_count}"
}

count_file_bytes() {
  local file_path="$1"
  local byte_count

  [[ -f "${file_path}" ]] || die "Expected output file does not exist: ${file_path}"
  byte_count="$(wc -c < "${file_path}")"
  byte_count="${byte_count//[[:space:]]/}"
  printf '%s\n' "${byte_count}"
}

format_throughput() {
  local docs="$1"
  local bytes="$2"
  local seconds="$3"

  "${PYTHON_BIN}" - "${docs}" "${bytes}" "${seconds}" <<'PY'
import sys

docs = float(sys.argv[1])
byte_count = float(sys.argv[2])
seconds = float(sys.argv[3])

if seconds <= 0:
    print("n/a docs/sec, n/a/sec")
    raise SystemExit(0)

docs_per_sec = docs / seconds
bytes_per_sec = byte_count / seconds

units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
index = 0
rate = bytes_per_sec
while rate >= 1024 and index < len(units) - 1:
    rate /= 1024
    index += 1

print(f"{docs_per_sec:.0f} docs/sec, {rate:.1f} {units[index]}/sec")
PY
}

run_timed_command() {
  local metrics_file="$1"

  shift
  /usr/bin/time -p sh -c 'exec "$@" 2>&3' sh \
    ${CPU_LIMIT_WRAPPER[@]+"${CPU_LIMIT_WRAPPER[@]}"} "$@" 3>&2 2>"${metrics_file}"
}

read_timing_metrics() {
  local metrics_file="$1"

  [[ -f "${metrics_file}" ]] || die "Expected metrics file does not exist: ${metrics_file}"
  "${PYTHON_BIN}" - "${metrics_file}" <<'PY'
import pathlib
import sys

metrics_file = pathlib.Path(sys.argv[1])
values = {"real": [], "user": [], "sys": []}

for raw_line in metrics_file.read_text(encoding="utf-8").splitlines():
    line = raw_line.strip()
    if not line:
        continue
    key, sep, value = line.partition(" ")
    if key not in values or not sep:
        raise SystemExit(f"Invalid timing line in {metrics_file}: {raw_line}")
    try:
        parsed = float(value.strip())
    except ValueError as exc:
        raise SystemExit(f"Invalid {key} value in {metrics_file}: {value}") from exc
    values[key].append(parsed)

for key, entries in values.items():
    if len(entries) != 1:
        raise SystemExit(
            f"Expected exactly one {key} entry in {metrics_file}, got {len(entries)}"
        )

real = values["real"][0]
user = values["user"][0]
sys_seconds = values["sys"][0]
cpu = user + sys_seconds
print(f"{real:.2f}\t{user:.2f}\t{sys_seconds:.2f}\t{cpu:.2f}")
PY
}

write_results_header() {
  local results_file="$1"

  printf 'tool\trun\treal_seconds\tuser_seconds\tsys_seconds\tcpu_seconds\tlines\tbytes\n' > "${results_file}"
}

append_result() {
  local results_file="$1"
  local tool_name="$2"
  local run_number="$3"
  local real_seconds="$4"
  local user_seconds="$5"
  local sys_seconds="$6"
  local cpu_seconds="$7"
  local line_count="$8"
  local byte_count="$9"

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${tool_name}" \
    "${run_number}" \
    "${real_seconds}" \
    "${user_seconds}" \
    "${sys_seconds}" \
    "${cpu_seconds}" \
    "${line_count}" \
    "${byte_count}" >> "${results_file}"
}

validate_line_count() {
  local tool_name="$1"
  local output_file="$2"
  local expected_lines="$3"
  local actual_lines="$4"

  [[ "${actual_lines}" == "${expected_lines}" ]] || die \
    "${tool_name} output line count mismatch for ${output_file}: expected ${expected_lines}, got ${actual_lines}"
}

log_run_metrics() {
  local tool_name="$1"
  local phase="$2"
  local run_number="$3"
  local run_total="$4"
  local real_seconds="$5"
  local user_seconds="$6"
  local sys_seconds="$7"
  local cpu_seconds="$8"
  local line_count="$9"
  local byte_count="${10}"
  local throughput

  throughput="$(format_throughput "${line_count}" "${byte_count}" "${real_seconds}")"
  log "${phase} ${run_number}/${run_total} ${tool_name}: wall ${real_seconds}s | cpu ${cpu_seconds}s (user ${user_seconds}s + sys ${sys_seconds}s), ${line_count} lines, ${byte_count} bytes, ${throughput}"
}

build_benchmark_variants() {
  local search_type

  BENCH_VARIANTS=()
  for search_type in "${BENCH_SEARCH_TYPE_LIST[@]}"; do
    BENCH_VARIANTS+=("elasticdump-rs:${search_type}")
    BENCH_VARIANTS+=("elasticdump:${search_type}")
  done
}

variant_label() {
  local tool_name="$1"
  local search_type="$2"

  printf '%s-%s\n' "${tool_name}" "${search_type}"
}

# rs variants carry an -sN suffix when sliced so run logs, the TSV, and the
# summary make the sliced configuration explicit (the Node.js elasticdump has
# no slicing support, so its labels never change).
rs_variant_label() {
  local search_type="$1"
  local label

  label="$(variant_label "elasticdump-rs" "${search_type}")"
  if rs_slices_enabled; then
    label="${label}-s${BENCH_RS_SLICES}"
  fi
  printf '%s\n' "${label}"
}

run_one_series_entry() {
  local tool_name="$1"
  local phase="$2"
  local run_number="$3"
  local run_total="$4"
  local output_file="$5"
  local metrics_file="$6"
  local results_file="$7"
  local real_seconds
  local user_seconds
  local sys_seconds
  local cpu_seconds
  local line_count
  local byte_count
  local timing_fields

  shift 7

  run_timed_command "${metrics_file}" "$@" || die "${tool_name} ${phase} run ${run_number} failed"
  timing_fields="$(read_timing_metrics "${metrics_file}")"
  IFS=$'\t' read -r real_seconds user_seconds sys_seconds cpu_seconds <<< "${timing_fields}"
  line_count="$(count_file_lines "${output_file}")"
  byte_count="$(count_file_bytes "${output_file}")"
  validate_line_count "${tool_name}" "${output_file}" "${BENCH_DOCS}" "${line_count}"
  log_run_metrics \
    "${tool_name}" \
    "${phase}" \
    "${run_number}" \
    "${run_total}" \
    "${real_seconds}" \
    "${user_seconds}" \
    "${sys_seconds}" \
    "${cpu_seconds}" \
    "${line_count}" \
    "${byte_count}"

  if [[ "${phase}" == "measured" ]]; then
    append_result \
      "${results_file}" \
      "${tool_name}" \
      "${run_number}" \
      "${real_seconds}" \
      "${user_seconds}" \
      "${sys_seconds}" \
      "${cpu_seconds}" \
      "${line_count}" \
      "${byte_count}"
  fi
}

run_elasticdump_rs_entry() {
  local phase="$1"
  local run_number="$2"
  local run_total="$3"
  local results_file="$4"
  local search_type="$5"
  local output_file
  local metrics_file
  local input_url
  local tool_label
  local -a rs_parallel_args=()

  input_url="$(es_url "/${BENCH_INDEX}")"
  tool_label="$(rs_variant_label "${search_type}")"
  output_file="${WORKDIR}/${tool_label}.${phase}.${run_number}.jsonl"
  metrics_file="${WORKDIR}/${tool_label}.${phase}.${run_number}.metrics"

  if rs_slices_enabled; then
    rs_parallel_args+=(--slices "${BENCH_RS_SLICES}")
    # The tool clamps workers to min(workers, slices), so default the worker
    # count to the slice count to keep one extraction worker per slice.
    rs_parallel_args+=(--workers "${BENCH_RS_WORKERS:-${BENCH_RS_SLICES}}")
  elif [[ -n "${BENCH_RS_WORKERS}" ]]; then
    rs_parallel_args+=(--workers "${BENCH_RS_WORKERS}")
  fi

  run_one_series_entry \
    "${tool_label}" \
    "${phase}" \
    "${run_number}" \
    "${run_total}" \
    "${output_file}" \
    "${metrics_file}" \
    "${results_file}" \
    "${RS_BIN}" \
    --input "${input_url}" \
    --output "${output_file}" \
    --type data \
    --limit "${BENCH_LIMIT}" \
    --overwrite \
    --quiet \
    ${rs_parallel_args[@]+"${rs_parallel_args[@]}"} \
    $(if [[ "${search_type}" == "scroll" ]]; then
        printf '%s\n' "--scrollTime" "${BENCH_SCROLL_TIME}" "--searchType" "scroll"
      else
        printf '%s\n' "--searchType" "pit" "--pitKeepAlive" "${BENCH_PIT_KEEP_ALIVE}"
      fi)
}

run_elasticdump_entry() {
  local phase="$1"
  local run_number="$2"
  local run_total="$3"
  local results_file="$4"
  local search_type="$5"
  local output_file
  local metrics_file
  local input_url
  local tool_label

  input_url="$(es_url "/${BENCH_INDEX}")"
  tool_label="$(variant_label "elasticdump" "${search_type}")"
  output_file="${WORKDIR}/${tool_label}.${phase}.${run_number}.jsonl"
  metrics_file="${WORKDIR}/${tool_label}.${phase}.${run_number}.metrics"

  run_one_series_entry \
    "${tool_label}" \
    "${phase}" \
    "${run_number}" \
    "${run_total}" \
    "${output_file}" \
    "${metrics_file}" \
    "${results_file}" \
    "${ELASTICDUMP_CMD[@]}" \
    --input "${input_url}" \
    --output "${output_file}" \
    --limit "${BENCH_LIMIT}" \
    --quiet \
    --overwrite \
    --type=data \
    $(if [[ "${search_type}" == "scroll" ]]; then
        printf '%s\n' "--scrollTime" "${BENCH_SCROLL_TIME}"
      else
        printf '%s\n' "--pit" "--pitKeepAlive" "${BENCH_PIT_KEEP_ALIVE}"
      fi)
}

run_benchmark_round() {
  local phase="$1"
  local run_number="$2"
  local run_total="$3"
  local results_file="$4"
  local round_index="$5"
  local variant_count="${#BENCH_VARIANTS[@]}"
  local offset
  local variant_index
  local variant
  local tool_name
  local search_type

  for (( offset = 0; offset < variant_count; offset++ )); do
    variant_index=$(( (round_index - 1 + offset) % variant_count ))
    variant="${BENCH_VARIANTS[variant_index]}"
    IFS=':' read -r tool_name search_type <<< "${variant}"

    if [[ "${tool_name}" == "elasticdump-rs" ]]; then
      run_elasticdump_rs_entry "${phase}" "${run_number}" "${run_total}" "${results_file}" "${search_type}"
    else
      run_elasticdump_entry "${phase}" "${run_number}" "${run_total}" "${results_file}" "${search_type}"
    fi
  done
}

run_benchmark_series() {
  local results_file="$1"
  local run_number
  local round_index=1

  for (( run_number = 1; run_number <= BENCH_WARMUP_RUNS; run_number++ )); do
    run_benchmark_round "warmup" "${run_number}" "${BENCH_WARMUP_RUNS}" "${results_file}" "${round_index}"
    round_index=$(( round_index + 1 ))
  done

  for (( run_number = 1; run_number <= BENCH_MEASURED_RUNS; run_number++ )); do
    run_benchmark_round "measured" "${run_number}" "${BENCH_MEASURED_RUNS}" "${results_file}" "${round_index}"
    round_index=$(( round_index + 1 ))
  done
}

print_summary() {
  local results_file="$1"
  local rs_scroll_tool
  local rs_pit_tool

  rs_scroll_tool="$(rs_variant_label "scroll")"
  rs_pit_tool="$(rs_variant_label "pit")"

  "${PYTHON_BIN}" - "${results_file}" "${BENCH_MEASURED_RUNS}" \
    "${rs_scroll_tool}" "${rs_pit_tool}" <<'PY'
import csv
import sys

results_file = sys.argv[1]
expected_runs = int(sys.argv[2])
rs_scroll_tool = sys.argv[3]
rs_pit_tool = sys.argv[4]
tools = []
rows_by_tool = {}


def format_throughput(docs, byte_count, seconds):
    if seconds <= 0:
        return "n/a docs/sec, n/a/sec"
    docs_per_sec = docs / seconds
    bytes_per_sec = byte_count / seconds
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    index = 0
    rate = bytes_per_sec
    while rate >= 1024 and index < len(units) - 1:
        rate /= 1024
        index += 1
    return f"{docs_per_sec:.0f} docs/sec, {rate:.1f} {units[index]}/sec"


with open(results_file, "r", encoding="utf-8", newline="") as handle:
    reader = csv.DictReader(handle, delimiter="\t")
    expected_fields = [
        "tool",
        "run",
        "real_seconds",
        "user_seconds",
        "sys_seconds",
        "cpu_seconds",
        "lines",
        "bytes",
    ]
    if reader.fieldnames != expected_fields:
        raise SystemExit(
            f"Unexpected results header in {results_file}: {reader.fieldnames!r}"
        )

    for row in reader:
        tool = row["tool"]
        if tool not in rows_by_tool:
            rows_by_tool[tool] = []
            tools.append(tool)
        rows_by_tool[tool].append(
            {
                "run": int(row["run"]),
                "real_seconds": float(row["real_seconds"]),
                "user_seconds": float(row["user_seconds"]),
                "sys_seconds": float(row["sys_seconds"]),
                "cpu_seconds": float(row["cpu_seconds"]),
                "lines": int(row["lines"]),
                "bytes": int(row["bytes"]),
            }
        )

for tool, rows in rows_by_tool.items():
    if len(rows) != expected_runs:
        raise SystemExit(
            f"Measured run count mismatch for {tool}: expected {expected_runs}, got {len(rows)}"
        )

print("Measured runs (warmups excluded)")
for tool in tools:
    for row in rows_by_tool[tool]:
        print(
            f"  {tool} run {row['run']}: "
            f"wall {row['real_seconds']:.2f}s | "
            f"cpu {row['cpu_seconds']:.2f}s "
            f"(user {row['user_seconds']:.2f}s + sys {row['sys_seconds']:.2f}s), "
            f"{row['lines']} lines, {row['bytes']} bytes, "
            f"{format_throughput(row['lines'], row['bytes'], row['real_seconds'])}"
        )

print("Averages")

def display_seconds_text(value):
    return f"{value:.2f}"


def display_seconds_value(value):
    return float(display_seconds_text(value))


averages = {}
for tool in tools:
    rows = rows_by_tool[tool]
    averages[tool] = {
        "real_seconds": sum(row["real_seconds"] for row in rows) / len(rows),
        "user_seconds": sum(row["user_seconds"] for row in rows) / len(rows),
        "sys_seconds": sum(row["sys_seconds"] for row in rows) / len(rows),
        "cpu_seconds": sum(row["cpu_seconds"] for row in rows) / len(rows),
        "lines": round(sum(row["lines"] for row in rows) / len(rows)),
        "bytes": round(sum(row["bytes"] for row in rows) / len(rows)),
    }
    avg = averages[tool]
    avg["display_user_seconds"] = display_seconds_text(avg["user_seconds"])
    avg["display_sys_seconds"] = display_seconds_text(avg["sys_seconds"])
    avg["display_cpu_seconds"] = display_seconds_text(avg["cpu_seconds"])
    print(
        f"  {tool}: "
        f"wall {display_seconds_text(avg['real_seconds'])}s avg | "
        f"cpu {avg['display_cpu_seconds']}s avg "
        f"(user {avg['display_user_seconds']}s; sys {avg['display_sys_seconds']}s), "
        f"{avg['lines']} lines avg, {avg['bytes']} bytes avg, "
        f"{format_throughput(avg['lines'], avg['bytes'], avg['real_seconds'])} avg"
    )

def print_comparison(label, lhs, rhs):
    if lhs not in averages or rhs not in averages:
        return

    lhs_real = averages[lhs]["real_seconds"]
    rhs_real = averages[rhs]["real_seconds"]
    lhs_cpu = averages[lhs]["cpu_seconds"]
    rhs_cpu = averages[rhs]["cpu_seconds"]

    lhs_real_text = display_seconds_text(lhs_real)
    rhs_real_text = display_seconds_text(rhs_real)
    lhs_cpu_text = display_seconds_text(lhs_cpu)
    rhs_cpu_text = display_seconds_text(rhs_cpu)

    lhs_real_display = display_seconds_value(lhs_real)
    rhs_real_display = display_seconds_value(rhs_real)
    lhs_cpu_display = display_seconds_value(lhs_cpu)
    rhs_cpu_display = display_seconds_value(rhs_cpu)

    if lhs_real_display == rhs_real_display:
        wall_summary = (
            f"{label} wall-clock: tied at {lhs_real_text}s avg"
        )
    elif lhs_real_display == 0:
        wall_summary = (
            f"{label} wall-clock: {lhs} completed faster "
            f"({lhs_real_text}s avg vs {rhs_real_text}s avg)"
        )
    elif rhs_real_display == 0:
        wall_summary = (
            f"{label} wall-clock: {rhs} completed faster "
            f"({rhs_real_text}s avg vs {lhs_real_text}s avg)"
        )
    elif lhs_real < rhs_real:
        wall_summary = (
            f"{label} wall-clock: {lhs} was {rhs_real / lhs_real:.2f}x faster "
            f"({lhs_real_text}s avg vs {rhs_real_text}s avg)"
        )
    else:
        wall_summary = (
            f"{label} wall-clock: {rhs} was {lhs_real / rhs_real:.2f}x faster "
            f"({rhs_real_text}s avg vs {lhs_real_text}s avg)"
        )

    if lhs_cpu_display == rhs_cpu_display:
        cpu_summary = f"{label} CPU total: tied at {lhs_cpu_text}s avg"
    elif lhs_cpu_display == 0:
        cpu_summary = (
            f"{label} CPU total: {lhs} used {0.0:.2f}x the CPU time of {rhs} "
            f"({lhs_cpu_text}s avg vs {rhs_cpu_text}s avg)"
        )
    elif rhs_cpu_display == 0:
        cpu_summary = (
            f"{label} CPU total: {rhs} used {0.0:.2f}x the CPU time of {lhs} "
            f"({rhs_cpu_text}s avg vs {lhs_cpu_text}s avg)"
        )
    elif lhs_cpu_display > rhs_cpu_display:
        cpu_summary = (
            f"{label} CPU total: {lhs} used {lhs_cpu_display / rhs_cpu_display:.2f}x the CPU time of {rhs} "
            f"({lhs_cpu_text}s avg vs {rhs_cpu_text}s avg)"
        )
    else:
        cpu_summary = (
            f"{label} CPU total: {rhs} used {rhs_cpu_display / lhs_cpu_display:.2f}x the CPU time of {lhs} "
            f"({rhs_cpu_text}s avg vs {lhs_cpu_text}s avg)"
        )

    print(wall_summary)
    print(cpu_summary)

print_comparison("Scroll rs vs node", rs_scroll_tool, "elasticdump-scroll")
print_comparison("PIT rs vs node", rs_pit_tool, "elasticdump-pit")
print_comparison("elasticdump-rs PIT vs Scroll", rs_pit_tool, rs_scroll_tool)
print_comparison("elasticdump PIT vs Scroll", "elasticdump-pit", "elasticdump-scroll")
PY
}

setup_runtime() {
  if [[ -n "${BENCH_INDEX_NAME}" ]]; then
    BENCH_INDEX="${BENCH_INDEX_NAME}"
  else
    BENCH_INDEX="${BENCH_INDEX_PREFIX}_$(date -u +%Y%m%dt%H%M%Sz)_$$"
  fi

  validate_index_name "${BENCH_INDEX}"

  if [[ -n "${BENCH_WORKDIR}" ]]; then
    mkdir -p -- "${BENCH_WORKDIR}"
    WORKDIR="$(mktemp -d "${BENCH_WORKDIR}/elasticdump-rs-bench.XXXXXX")"
  else
    WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/elasticdump-rs-bench.XXXXXX")"
  fi
  WORKDIR_CREATED=1
}

cleanup() {
  local exit_code=$?

  if [[ "${BENCH_KEEP_ARTIFACTS}" == "1" ]]; then
    [[ -n "${BENCH_INDEX}" ]] && log "Keeping benchmark index ${BENCH_INDEX}"
    [[ -n "${WORKDIR}" ]] && log "Keeping benchmark artifacts in ${WORKDIR}"
    return "${exit_code}"
  fi

  if [[ -n "${BENCH_INDEX}" && -n "${WORKDIR}" && -d "${WORKDIR}" ]]; then
    if ! delete_index_if_exists >/dev/null 2>&1; then
      log "Failed to delete benchmark index ${BENCH_INDEX} during cleanup"
    fi
  fi

  if [[ "${WORKDIR_CREATED}" == "1" && -n "${WORKDIR}" && -d "${WORKDIR}" ]]; then
    rm -rf -- "${WORKDIR}"
  fi

  return "${exit_code}"
}

main() {
  local total_runs
  local seeded_docs
  local results_file

  parse_args "$@"
  validate_config
  require_command curl
  require_command mktemp
  require_command /usr/bin/time
  resolve_python
  resolve_cpu_limit_wrapper
  setup_runtime
  trap cleanup EXIT

  log "Benchmark configuration"
  printf '  ES_URL=%s\n' "${ES_URL}"
  printf '  BENCH_DOCS=%s\n' "${BENCH_DOCS}"
  printf '  BENCH_BULK_SIZE=%s\n' "${BENCH_BULK_SIZE}"
  printf '  BENCH_LIMIT=%s\n' "${BENCH_LIMIT}"
  printf '  BENCH_TEXT_BYTES=%s\n' "${BENCH_TEXT_BYTES}"
  printf '  BENCH_WARMUP_RUNS=%s\n' "${BENCH_WARMUP_RUNS}"
  printf '  BENCH_MEASURED_RUNS=%s\n' "${BENCH_MEASURED_RUNS}"
  printf '  BENCH_KEEP_ARTIFACTS=%s\n' "${BENCH_KEEP_ARTIFACTS}"
  printf '  BENCH_CODEC=%s\n' "${BENCH_CODEC}"
  printf '  BENCH_FORCE_MERGE=%s\n' "${BENCH_FORCE_MERGE}"
  printf '  BENCH_MAX_NUM_SEGMENTS=%s\n' "${BENCH_MAX_NUM_SEGMENTS}"
  printf '  BENCH_USE_EXPLICIT_IDS=%s\n' "${BENCH_USE_EXPLICIT_IDS}"
  printf '  BENCH_SEARCH_TYPES=%s\n' "${BENCH_SEARCH_TYPES}"
  printf '  BENCH_SCROLL_TIME=%s\n' "${BENCH_SCROLL_TIME}"
  printf '  BENCH_PIT_KEEP_ALIVE=%s\n' "${BENCH_PIT_KEEP_ALIVE}"
  if [[ -n "${BENCH_CPU_CORES}" ]]; then
    printf '  BENCH_CPU_CORES=%s\n' "${BENCH_CPU_CORES}"
    printf '  CPU_LIMIT_WRAPPER=%s\n' "${CPU_LIMIT_WRAPPER[*]}"
  else
    printf '  BENCH_CPU_CORES=(unlimited)\n'
  fi
  if rs_slices_enabled; then
    printf '  BENCH_RS_SLICES=%s\n' "${BENCH_RS_SLICES}"
    printf '  BENCH_RS_WORKERS=%s\n' "${BENCH_RS_WORKERS:-${BENCH_RS_SLICES} (matched to slices)}"
  else
    printf '  BENCH_RS_SLICES=(unsliced)\n'
    if [[ -n "${BENCH_RS_WORKERS}" ]]; then
      printf '  BENCH_RS_WORKERS=%s\n' "${BENCH_RS_WORKERS}"
    fi
  fi
  printf '  BENCH_INDEX=%s\n' "${BENCH_INDEX}"
  printf '  WORKDIR=%s\n' "${WORKDIR}"

  delete_index_if_exists || die "Failed to delete existing benchmark index ${BENCH_INDEX}"
  create_index
  seed_index
  prepare_index_for_reads
  seeded_docs="$(count_index_documents)"
  [[ "${seeded_docs}" == "${BENCH_DOCS}" ]] || die "Seeded document count mismatch: expected ${BENCH_DOCS}, got ${seeded_docs}"
  log "Seeded benchmark index ${BENCH_INDEX} with ${seeded_docs} documents"

  total_runs=$(( BENCH_WARMUP_RUNS + BENCH_MEASURED_RUNS ))
  if (( total_runs == 0 )); then
    log "No benchmark runs requested; exiting after dataset preparation"
    return 0
  fi

  resolve_elasticdump_command
  resolve_rs_binary
  build_benchmark_variants
  printf '  RS_BIN=%s\n' "${RS_BIN}"
  printf '  ELASTICDUMP_CMD=%s\n' "${ELASTICDUMP_CMD[*]}"

  results_file="${WORKDIR}/measured-runs.tsv"
  write_results_header "${results_file}"

  run_benchmark_series "${results_file}"

  if (( BENCH_MEASURED_RUNS > 0 )); then
    print_summary "${results_file}"
  else
    log "No measured runs requested; warmup-only benchmark finished"
  fi
}

main "$@"
