#!/usr/bin/env bash
set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# shellcheck source=/dev/null
. "${PROJECT_DIR}/.venv/bin/activate"
# shellcheck source=/dev/null
. "${PROJECT_DIR}/.env.local"
python "${PROJECT_DIR}/count_minute_bars.py" "$@"
