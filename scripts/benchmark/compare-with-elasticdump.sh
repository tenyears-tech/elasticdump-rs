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
    BENCH_INDEX="${BENCH_INDEX_PREFIX}_$(date -u +%Y%m%dT%H%M%SZ)_$$"
  fi
}

cleanup() {
  local exit_code=$?

  if [[ "${BENCH_KEEP_ARTIFACTS}" == "1" ]]; then
    [[ -n "${BENCH_INDEX}" ]] && log "Keeping benchmark index ${BENCH_INDEX}"
    [[ -n "${WORKDIR}" ]] && log "Keeping benchmark artifacts in ${WORKDIR}"
    return "${exit_code}"
  fi

  if [[ "${WORKDIR_CREATED}" == "1" && -n "${WORKDIR}" && -d "${WORKDIR}" ]]; then
    rm -rf -- "${WORKDIR}"
  fi

  return "${exit_code}"
}

main() {
  parse_args "$@"
  validate_config
  require_command curl
  require_command mktemp
  require_command python3
  resolve_elasticdump_command
  resolve_rs_binary
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
  printf '  RS_BIN=%s\n' "${RS_BIN}"
  printf '  ELASTICDUMP_CMD=%s\n' "${ELASTICDUMP_CMD[*]}"
}

main "$@"
