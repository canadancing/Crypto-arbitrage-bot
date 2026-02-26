#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
PIDS=()
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

if [[ -f logs/bot.pid ]]; then
  PIDS+=("$(cat logs/bot.pid)")
else
  echo "No PID file found; checking for running bot process"
fi

while IFS= read -r pid; do
  [[ -n "$pid" ]] && PIDS+=("$pid")
done < <(find_pids_for_target "$RUN_PATH" || true)

# Backward compatibility: also stop legacy direct run.py processes.
while IFS= read -r pid; do
  [[ -n "$pid" ]] && PIDS+=("$pid")
done < <(find_pids_for_target "$SCRIPT_DIR/run.py" || true)

if [[ ${#PIDS[@]} -eq 0 ]]; then
  echo "No bot_supervisor/run.py process found"
  rm -f logs/bot.pid
  exit 0
fi

for PID in $(printf '%s\n' "${PIDS[@]-}" | awk 'NF && !seen[$0]++'); do
  if kill -0 "$PID" 2>/dev/null; then
    kill -TERM "$PID"
    echo "Sent SIGTERM to $PID"
    for _ in {1..10}; do
      if ! kill -0 "$PID" 2>/dev/null; then
        break
      fi
      sleep 1
    done
    if kill -0 "$PID" 2>/dev/null; then
      kill -KILL "$PID" 2>/dev/null || true
      echo "Sent SIGKILL to $PID"
    fi
  else
    echo "Process $PID is not running"
  fi
done

rm -f logs/bot.pid

# Stop dashboard server as part of normal shutdown.
DASH_PIDS=()
if [[ -f logs/dashboard.pid ]]; then
  DASH_PIDS+=("$(cat logs/dashboard.pid)")
fi
while IFS= read -r pid; do
  [[ -n "$pid" ]] && DASH_PIDS+=("$pid")
done < <(find_pids_for_target "$DASH_PATH" || true)

for PID in $(printf '%s\n' "${DASH_PIDS[@]-}" | awk 'NF && !seen[$0]++'); do
  if kill -0 "$PID" 2>/dev/null; then
    kill -TERM "$PID"
    echo "Sent SIGTERM to dashboard $PID"
    for _ in {1..5}; do
      if ! kill -0 "$PID" 2>/dev/null; then
        break
      fi
      sleep 1
    done
    if kill -0 "$PID" 2>/dev/null; then
      kill -KILL "$PID" 2>/dev/null || true
      echo "Sent SIGKILL to dashboard $PID"
    fi
  fi
done
rm -f logs/dashboard.pid
