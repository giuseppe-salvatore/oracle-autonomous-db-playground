#!/usr/bin/env bash
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DB="${1:?Database argument missing}"
SYMBOL="${2:?Symbol argument missing}"

case "$DB" in
  OracleDB|SQLite3DB) ;;
  *) echo "Unsupported database: $DB" >&2; exit 2 ;;
esac
if [[ ! "$SYMBOL" =~ ^[A-Za-z0-9._-]+$ ]] || [[ "$SYMBOL" == . || "$SYMBOL" == .. ]]; then
  echo "Invalid symbol: $SYMBOL" >&2
  exit 2
fi

resolve_workspace_root() {
  local candidate
  for candidate in "$HOME/workspace" "$HOME/Workspace"; do
    if [ -d "$candidate" ]; then
      echo "$candidate"
      return 0
    fi
  done

  mkdir -p "$HOME/workspace"
  echo "$HOME/workspace"
}

WORKSPACE_ROOT="$(resolve_workspace_root)"
OUTPUT_FILE="${WORKSPACE_ROOT}/oracle-pinger-${SYMBOL}-output.log"
ERROR_FILE="${WORKSPACE_ROOT}/oracle-pinger-${SYMBOL}-error.log"
exec 9>"${WORKSPACE_ROOT}/oracle-pinger-${SYMBOL}.lock"
if ! flock -n 9; then
  echo "Pinger already running for ${SYMBOL}" >&2
  exit 1
fi

# shellcheck source=/dev/null
. "${PROJECT_DIR}/.venv/bin/activate"
# shellcheck source=/dev/null
. "${PROJECT_DIR}/.env.local"

NTFY_TOPIC="${NTFY_TOPIC:?NTFY_TOPIC must be set in .env.local}"
NTFY_URL="https://ntfy.sh/${NTFY_TOPIC}"

notify_failure() {
  local message="$1"

  if ! curl -fsS \
    -H "Title: Raspberry Pi Oracle pinger failed" \
    -H "Priority: high" \
    -d "$message" \
    "$NTFY_URL" > /dev/null; then
    echo "Failed to send pinger notification" >&2
  fi
}

print_captured_logs() {
  local output_file="$1"
  local error_file="$2"

  if [ -s "$output_file" ]; then
    cat "$output_file"
  fi

  if [ -s "$error_file" ]; then
    cat "$error_file"
  fi
}

TMP_OUTPUT=""
TMP_ERROR=""
cleanup() { rm -f -- "$TMP_OUTPUT" "$TMP_ERROR"; }
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
TMP_OUTPUT="$(mktemp)" || exit 1
TMP_ERROR="$(mktemp)" || exit 1

python "${PROJECT_DIR}/count_minute_bars.py" "$DB" "$SYMBOL" > "$TMP_OUTPUT" 2> "$TMP_ERROR"
EXIT_CODE=$?

mv "$TMP_OUTPUT" "$OUTPUT_FILE"
mv "$TMP_ERROR" "$ERROR_FILE"

print_captured_logs "$OUTPUT_FILE" "$ERROR_FILE"

if [ "$EXIT_CODE" -ne 0 ]; then
  notify_failure "Script failed for ${SYMBOL}. Exit code: ${EXIT_CODE}. Error log: ${ERROR_FILE}"
  exit "$EXIT_CODE"
fi

if [ ! -s "$OUTPUT_FILE" ]; then
  notify_failure "Script ran for ${SYMBOL}, but output file is empty: ${OUTPUT_FILE}. Error log: ${ERROR_FILE}"
  exit 1
fi
