#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
mkdir -p logs

RUN_PATH="$SCRIPT_DIR/bot_supervisor.sh"
DASH_PATH="$SCRIPT_DIR/dashboard_server.py"

find_pids_for_target() {
  local target="$1"
  local target_base
  target_base="$(basename "$target")"
  ps -Ao pid=,args= | while read -r pid args; do
    case "$args" in
      *[Pp]ython*"$target"*|*[Pp]ython*" $target_base"*|*[Pp]ython*"/$target_base"*|*[Bb]ash*"$target"*|*[Bb]ash*" $target_base"*|*[Bb]ash*"/$target_base"*)
        case "$args" in
          *"start.sh"*|*"stop.sh"*|*"ps -Ao pid=,args="*|*"find_pids_for_target"*|*"rg -i"*|*"grep "*)
            continue
            ;;
        esac
        echo "$pid"
        ;;
    esac
  done
}

RUN_PIDS="$(find_pids_for_target "$RUN_PATH")"
if [[ -n "$RUN_PIDS" ]]; then
  echo "Bot already running:"
  echo "$RUN_PIDS"
  # Keep bot.pid aligned to a live process id.
  echo "$RUN_PIDS" | head -n 1 > logs/bot.pid
else
  nohup bash "$RUN_PATH" > logs/supervisor.log 2>&1 &
  echo $! > logs/bot.pid
  echo "Started bot with PID $(cat logs/bot.pid)"
fi

DASH_PIDS="$(find_pids_for_target "$DASH_PATH")"
if [[ -n "$DASH_PIDS" ]]; then
  echo "Dashboard already running:"
  echo "$DASH_PIDS"
  # Keep dashboard.pid aligned to a live process id.
  echo "$DASH_PIDS" | head -n 1 > logs/dashboard.pid
else
  nohup python3 "$DASH_PATH" > logs/dashboard.log 2>&1 &
  echo $! > logs/dashboard.pid
  echo "Started dashboard with PID $(cat logs/dashboard.pid)"
  # Verify dashboard stayed up; retry once if it exited during startup.
  sleep 1
  if ! kill -0 "$(cat logs/dashboard.pid)" 2>/dev/null; then
    echo "Dashboard exited during startup, retrying..."
    nohup python3 "$DASH_PATH" > logs/dashboard.log 2>&1 &
    echo $! > logs/dashboard.pid
    sleep 1
    if ! kill -0 "$(cat logs/dashboard.pid)" 2>/dev/null; then
      echo "Dashboard failed to start. Check logs/dashboard.log"
      exit 1
    fi
  fi
fi
