#!/usr/bin/env bash
# post-edit.sh — Auto-lint after Claude edits Python or SQL files.
# Receives tool call info as JSON on stdin (PostToolUse event).
# Exits 0 in all cases so it never blocks Claude.

set -euo pipefail

INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(d.get('tool_input', {}).get('file_path', ''))
except Exception:
    print('')
" 2>/dev/null || echo "")

[[ -z "$FILE_PATH" ]] && exit 0

# ── Python → ruff ─────────────────────────────────────────────────────────────
if [[ "$FILE_PATH" == *.py ]]; then
    uv run ruff check "$FILE_PATH" --fix --quiet 2>/dev/null || true
    uv run ruff format "$FILE_PATH" --quiet 2>/dev/null || true
fi

# ── dbt SQL → sqlfluff ────────────────────────────────────────────────────────
if [[ "$FILE_PATH" == *.sql && "$FILE_PATH" == *transformations/* ]]; then
    uv run sqlfluff fix "$FILE_PATH" --dialect duckdb --quiet 2>/dev/null || true
fi

exit 0
