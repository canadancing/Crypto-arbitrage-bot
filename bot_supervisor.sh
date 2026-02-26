#!/usr/bin/env bash
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
mkdir -p logs

# ── stdout.log rotation: cap at 10 MB, keep 2 backups ──────────────────────
rotate_log() {
  local log="$SCRIPT_DIR/logs/stdout.log"
  local max_bytes=10485760  # 10 MB
  if [[ -f "$log" ]] && [[ $(stat -f%z "$log" 2>/dev/null || stat -c%s "$log" 2>/dev/null) -ge $max_bytes ]]; then
    [[ -f "${log}.2" ]] && rm -f "${log}.2"
    [[ -f "${log}.1" ]] && mv "${log}.1" "${log}.2"
    mv "$log" "${log}.1"
    echo "$(date '+%Y-%m-%d %H:%M:%S') supervisor: rotated stdout.log"
  fi
}

while true; do
  rotate_log
  echo "$(date '+%Y-%m-%d %H:%M:%S') supervisor: starting run.py"
  python3 -u "$SCRIPT_DIR/run.py" >> "$SCRIPT_DIR/logs/stdout.log" 2>&1
  rc=$?
  echo "$(date '+%Y-%m-%d %H:%M:%S') supervisor: run.py exited with code $rc"
  sleep 2
done
