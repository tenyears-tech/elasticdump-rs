#!/usr/bin/env bash
set -euo pipefail

ES_URL="${ES_URL:-http://localhost:9200}"
BENCH_DOCS="${BENCH_DOCS:-1000000}"
BENCH_BULK_SIZE="${BENCH_BULK_SIZE:-5000}"
BENCH_LIMIT="${BENCH_LIMIT:-10000}"
BENCH_TEXT_BYTES="${BENCH_TEXT_BYTES:-256}"
BENCH_WARMUP_RUNS="${BENCH_WARMUP_RUNS:-1}"
BENCH_MEASURED_RUNS="${BENCH_MEASURED_RUNS:-2}"
BENCH_KEEP_ARTIFACTS="${BENCH_KEEP_ARTIFACTS:-1}"
BENCH_INDEX_PREFIX="${BENCH_INDEX_PREFIX:-elasticdump_rs_bench}"
BENCH_INDEX_NAME="${BENCH_INDEX_NAME:-}"
BENCH_WORKDIR="${BENCH_WORKDIR:-}"
BENCH_RS_BIN="${BENCH_RS_BIN:-}"
BENCH_ELASTICDUMP_CMD="${BENCH_ELASTICDUMP_CMD:-elasticdump}"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
SCRIPT_NAME="$(basename -- "${BASH_SOURCE[0]}")"

WORKDIR=""
WORKDIR_CREATED=0
BENCH_INDEX=""
RS_BIN=""
declare -a ELASTICDUMP_CMD=()

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
  --index-name NAME           Explicit benchmark index name
  --index-prefix PREFIX       Prefix for generated benchmark index names
  --workdir DIR               Artifact work directory
  --rs-bin PATH               Path to the elasticdump-rs binary
  --elasticdump-cmd CMD       Command used to invoke the original elasticdump
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
    python3 - "$raw" <<'PY'
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
}

validate_config() {
  validate_uint "BENCH_DOCS" "${BENCH_DOCS}"
  validate_uint "BENCH_BULK_SIZE" "${BENCH_BULK_SIZE}"
  validate_uint "BENCH_LIMIT" "${BENCH_LIMIT}"
  validate_uint "BENCH_TEXT_BYTES" "${BENCH_TEXT_BYTES}"
  validate_uint "BENCH_WARMUP_RUNS" "${BENCH_WARMUP_RUNS}"
  validate_uint "BENCH_MEASURED_RUNS" "${BENCH_MEASURED_RUNS}"

  case "${BENCH_KEEP_ARTIFACTS}" in
    0 | 1) ;;
    *)
      die "BENCH_KEEP_ARTIFACTS must be 0 or 1, got: ${BENCH_KEEP_ARTIFACTS}"
      ;;
  esac

  (( BENCH_BULK_SIZE > 0 )) || die "BENCH_BULK_SIZE must be greater than zero"
  (( BENCH_LIMIT > 0 )) || die "BENCH_LIMIT must be greater than zero"
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
      --data @- \
      "$(es_url "/${BENCH_INDEX}")" <<'JSON'
{
  "settings": {
    "index": {
      "number_of_shards": 1,
      "number_of_replicas": 0,
      "refresh_interval": "-1",
      "codec": "best_compression"
    }
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "@timestamp": { "type": "date" },
      "level": { "type": "keyword" },
      "service": { "type": "keyword" },
      "host": { "type": "keyword" },
      "message": { "type": "text" },
      "trace_id": { "type": "keyword" },
      "span_id": { "type": "keyword" },
      "request_id": { "type": "keyword" },
      "env": { "type": "keyword" },
      "region": { "type": "keyword" },
      "metadata": {
        "properties": {
          "attempt": { "type": "integer" },
          "bytes": { "type": "integer" },
          "status": { "type": "integer" },
          "success": { "type": "boolean" },
          "source": { "type": "keyword" }
        }
      }
    }
  }
}
JSON
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

  /usr/bin/python3 - "${BENCH_INDEX}" "${start_doc}" "${batch_size}" "${BENCH_TEXT_BYTES}" "${output_file}" <<'PY'
import json
import sys

index_name = sys.argv[1]
start_doc = int(sys.argv[2])
batch_size = int(sys.argv[3])
text_bytes = int(sys.argv[4])
output_file = sys.argv[5]

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
        action = {"index": {"_index": index_name, "_id": f"doc-{doc_id:08d}"}}
        handle.write(json.dumps(action, separators=(",", ":")))
        handle.write("\n")
        handle.write(json.dumps(source, separators=(",", ":")))
        handle.write("\n")
PY
}

validate_bulk_response() {
  local response_file="$1"
  local expected_items="$2"

  /usr/bin/python3 - "${response_file}" "${expected_items}" <<'PY'
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

refresh_index() {
  local settings_response
  local refresh_response
  local settings_code
  local refresh_code

  settings_response="$(mktemp "${WORKDIR}/refresh-settings.XXXXXX.json")"
  refresh_response="$(mktemp "${WORKDIR}/refresh-index.XXXXXX.json")"

  settings_code="$(
    curl --silent --show-error \
      --output "${settings_response}" \
      --write-out '%{http_code}' \
      --request PUT \
      --header 'Content-Type: application/json' \
      --data '{"index":{"refresh_interval":"1s"}}' \
      "$(es_url "/${BENCH_INDEX}/_settings")"
  )"

  if [[ "${settings_code}" != "200" ]]; then
    cat -- "${settings_response}" >&2
    rm -f -- "${settings_response}" "${refresh_response}"
    die "Failed to restore refresh interval on ${BENCH_INDEX} (HTTP ${settings_code})"
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
    rm -f -- "${settings_response}" "${refresh_response}"
    die "Failed to refresh benchmark index ${BENCH_INDEX} (HTTP ${refresh_code})"
  fi

  rm -f -- "${settings_response}" "${refresh_response}"
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

  /usr/bin/python3 - "${response_file}" <<'PY'
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

setup_runtime() {
  if [[ -n "${BENCH_WORKDIR}" ]]; then
    mkdir -p -- "${BENCH_WORKDIR}"
    WORKDIR="$(mktemp -d "${BENCH_WORKDIR}/elasticdump-rs-bench.XXXXXX")"
  else
    WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/elasticdump-rs-bench.XXXXXX")"
  fi
  WORKDIR_CREATED=1

  if [[ -n "${BENCH_INDEX_NAME}" ]]; then
    BENCH_INDEX="${BENCH_INDEX_NAME}"
  else
    BENCH_INDEX="${BENCH_INDEX_PREFIX}_$(date -u +%Y%m%dt%H%M%sz)_$$"
  fi
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

  parse_args "$@"
  validate_config
  require_command curl
  require_command mktemp
  require_command python3
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
  printf '  BENCH_INDEX=%s\n' "${BENCH_INDEX}"
  printf '  WORKDIR=%s\n' "${WORKDIR}"

  delete_index_if_exists || die "Failed to delete existing benchmark index ${BENCH_INDEX}"
  create_index
  seed_index
  refresh_index
  seeded_docs="$(count_index_documents)"
  [[ "${seeded_docs}" == "${BENCH_DOCS}" ]] || die "Seeded document count mismatch: expected ${BENCH_DOCS}, got ${seeded_docs}"
  log "Seeded benchmark index ${BENCH_INDEX} with ${seeded_docs} documents"

  total_runs=$(( BENCH_WARMUP_RUNS + BENCH_MEASURED_RUNS ))
  if (( total_runs == 0 )); then
    log "No benchmark runs requested"
    return 0
  fi

  resolve_elasticdump_command
  resolve_rs_binary
  printf '  RS_BIN=%s\n' "${RS_BIN}"
  printf '  ELASTICDUMP_CMD=%s\n' "${ELASTICDUMP_CMD[*]}"
}

main "$@"
