#!/usr/bin/env python3
"""
Local monitoring dashboard server for the multi-exchange funding arb bot.

Run:
  python3 dashboard_server.py

Then open:
  http://127.0.0.1:8787
"""

from __future__ import annotations

import json
import os
import re
import time
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parent
STATIC_DIR = ROOT / "dashboard"
CONFIG_PATH = ROOT / "config.yaml"
LOG_PATH = ROOT / "logs" / "bot.log"
PID_PATH = ROOT / "logs" / "bot.pid"
STATUS_PATH = ROOT / "logs" / "status.json"
HISTORY_PATH = ROOT / "logs" / "position_history.jsonl"
EQUITY_SNAPSHOT_PATH = ROOT / "logs" / "equity_snapshots.json"

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
SENSITIVE_KEYS = {"api_key", "api_secret", "env_key", "env_secret", "token", "chat_id"}


def _tail_lines(path: Path, limit: int = 100) -> list[str]:
    if not path.exists():
        return []
    try:
        raw = path.read_bytes().replace(b"\x00", b"")
        lines = raw.decode("utf-8", errors="replace").splitlines()
        cleaned = [ANSI_RE.sub("", ln).strip() for ln in lines[-limit:] if ln.strip()]
        return cleaned
    except Exception:
        return []


def _pid_info() -> dict[str, Any]:
    if not PID_PATH.exists():
        return {"pid": None, "running": False}
    try:
        pid = int(PID_PATH.read_text().strip())
        os.kill(pid, 0)  # Check if alive
        return {"pid": pid, "running": True}
    except (ValueError, OSError):
        return {"pid": None, "running": False}


def _strip_sensitive(d: Any) -> Any:
    """Recursively strip sensitive keys from a dict."""
    if isinstance(d, dict):
        return {k: _strip_sensitive(v) for k, v in d.items() if k not in SENSITIVE_KEYS}
    if isinstance(d, list):
        return [_strip_sensitive(x) for x in d]
    return d


def _read_config_safe() -> dict[str, Any]:
    """Read config.yaml and strip sensitive keys."""
    try:
        with CONFIG_PATH.open("r") as f:
            raw = yaml.safe_load(f) or {}
        return _strip_sensitive(raw)
    except Exception as e:
        return {"error": str(e)}





def collect_status() -> dict[str, Any]:
    """Collect dashboard status from bot snapshot + config fallback."""
    logs = _tail_lines(LOG_PATH)
    process = _pid_info()
    config = _read_config_safe()
    runtime_dry = config.get("runtime", {}).get("dry_run", True)

    if STATUS_PATH.exists():
        try:
            payload = json.loads(STATUS_PATH.read_text(encoding="utf-8"))
            payload["dry_run"] = runtime_dry
            payload["recent_logs"] = logs[-100:]
            payload["process"] = process
            if not process.get("running", False):
                for ex in payload.get("exchanges", []):
                    ex["running"] = False
            try:
                payload["status_age_seconds"] = max(
                    0.0,
                    float(time.time()) - float(os.path.getmtime(STATUS_PATH))
                )
            except Exception:
                pass
            return payload
        except Exception:
            pass

    # Fallback: no snapshot yet.
    exchanges = []
    for ex_cfg in config.get("exchanges", []):
        exchanges.append({
            "exchange": ex_cfg.get("name", "unknown"),
            "running": process.get("running", False),
            "dry_run": runtime_dry,
            "positions": 0,
            "max_positions": ex_cfg.get("position", {}).get("max_concurrent_positions", 3),
            "daily_pnl": 0,
            "starting_equity": 0,
            "loss_limit_exceeded": False,
            "open_positions": [],
        })

    return {
        "dry_run": runtime_dry,
        "exchanges": exchanges,
        "total_positions": 0,
        "total_daily_pnl": 0,
        "recent_logs": logs[-100:],
        "process": process,
    }


def _read_history(limit: int = 1000) -> list[dict[str, Any]]:
    if not HISTORY_PATH.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        lines = HISTORY_PATH.read_text(encoding="utf-8", errors="ignore").splitlines()
        for line in lines[-limit:]:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    except Exception:
        return []
    rows.sort(key=lambda r: float(r.get("ts", 0)), reverse=True)
    return rows


def _history_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    closes = [r for r in rows if r.get("event") in ("CLOSE", "CLOSE_FORCED")]
    total_pnl = sum(float(r.get("pnl", 0) or 0) for r in closes)
    wins = sum(1 for r in closes if float(r.get("pnl", 0) or 0) > 0)
    losses = sum(1 for r in closes if float(r.get("pnl", 0) or 0) < 0)
    return {
        "closed_trades": len(closes),
        "wins": wins,
        "losses": losses,
        "win_rate": (wins / len(closes) * 100) if closes else 0.0,
        "realized_pnl": total_pnl,
    }


def collect_history() -> dict[str, Any]:
    rows = _read_history()
    return {
        "summary": _history_summary(rows),
        "rows": rows,
    }

def collect_analytics() -> dict[str, Any]:
    rows = _read_history(limit=5000)
    closes = [r for r in rows if r.get("event") == "CLOSE"]
    
    # Sort by time ascending
    closes.sort(key=lambda r: float(r.get("ts", 0)))
    
    import datetime
    from collections import defaultdict
    
    total_cumulative = 0.0
    daily_pnl_map: dict[str, float] = defaultdict(float)
    
    for r in closes:
        ts = float(r.get("ts", 0))
        pnl = float(r.get("pnl", 0) or 0)
        total_cumulative += pnl
        day_str = datetime.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
        daily_pnl_map[day_str] += pnl
    
    # Read real equity from status.json for today_asset
    today_asset = None
    try:
        with open(STATUS_PATH, encoding="utf-8") as f:
            status = json.load(f)
        exchanges = status.get("exchanges", [])
        today_asset = sum(float(e.get("starting_equity", 0) or 0) for e in exchanges)
    except Exception:
        pass
    
    # Read yesterday's equity from server-side snapshots
    yesterday_asset = None
    daily_change = None
    snapshots = {}
    try:
        with open(EQUITY_SNAPSHOT_PATH, encoding="utf-8") as f:
            snapshots = json.load(f)
        yesterday_str = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        yesterday_asset = snapshots.get(yesterday_str)
        if yesterday_asset is not None and today_asset is not None:
            daily_change = today_asset - yesterday_asset
    except Exception:
        pass
        
    # Replace historical PnLs with true snapshot differences if available
    if snapshots:
        sorted_snap_days = sorted(snapshots.keys())
        for i in range(1, len(sorted_snap_days)):
            prev_day = sorted_snap_days[i-1]
            curr_day = sorted_snap_days[i]
            daily_pnl_map[curr_day] = snapshots[curr_day] - snapshots[prev_day]
            
    # Always set today's pnl to live daily_change if we have it
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    if daily_change is not None:
        daily_pnl_map[today_str] = daily_change
    elif today_asset is not None and yesterday_asset is not None:
        daily_pnl_map[today_str] = today_asset - yesterday_asset
    
    # Build graph_data: one entry per day, sorted ascending
    graph_data = []
    for day_str in sorted(daily_pnl_map.keys()):
        dt = datetime.datetime.strptime(day_str, "%Y-%m-%d")
        ts = dt.replace(tzinfo=datetime.timezone.utc).timestamp()
        graph_data.append({"ts": ts, "day": day_str, "daily_pnl": daily_pnl_map[day_str]})

    # Mock data if not enough real history
    if len(graph_data) < 3:
        import random
        mock_daily_map: dict[str, float] = {}
        now = time.time()
        day_seconds = 86400
        start_ts = now - (30 * day_seconds)
        
        for i in range(31):
            mock_ts = start_ts + (i * day_seconds)
            mock_daily = random.uniform(0.5, 4.0)
            day_str = datetime.datetime.utcfromtimestamp(mock_ts).strftime("%Y-%m-%d")
            mock_daily_map[day_str] = mock_daily

        graph_data = [
            {"ts": datetime.datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc).timestamp(),
             "day": d, "daily_pnl": v, "mocked": True}
            for d, v in sorted(mock_daily_map.items())
        ]
        
    return {
        "today_asset": today_asset,
        "yesterday_asset": yesterday_asset,
        "daily_change": daily_change,
        "graph_data": graph_data
    }


def snapshot_equity() -> None:
    """Save today's total equity to a server-side file for daily tracking."""
    import datetime
    try:
        with open(STATUS_PATH, encoding="utf-8") as f:
            status = json.load(f)
        exchanges = status.get("exchanges", [])
        total_equity = sum(float(e.get("starting_equity", 0) or 0) for e in exchanges)
        if total_equity <= 0:
            return
        
        today_str = datetime.datetime.now().strftime("%Y-%m-%d")
        snapshots: dict[str, float] = {}
        try:
            with open(EQUITY_SNAPSHOT_PATH, encoding="utf-8") as f:
                snapshots = json.load(f)
        except Exception:
            pass
        
        # Always update today's value (latest reading)
        snapshots[today_str] = total_equity
        
        # Keep only last 30 days
        sorted_keys = sorted(snapshots.keys())
        while len(sorted_keys) > 30:
            del snapshots[sorted_keys.pop(0)]
        
        with open(EQUITY_SNAPSHOT_PATH, "w", encoding="utf-8") as f:
            json.dump(snapshots, f)
    except Exception:
        pass


class DashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(STATIC_DIR), **kwargs)

    def _json_response(self, status: int, payload: Any):
        body = json.dumps(payload, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/api/status":
            data = collect_status()
            snapshot_equity()  # persist today's equity for yesterday tracking
            self._json_response(200, data)
        elif self.path == "/api/history":
            self._json_response(200, collect_history())
        elif self.path == "/api/analytics":
            self._json_response(200, collect_analytics())
        elif self.path.startswith("/api/logs"):
            from urllib.parse import urlparse, parse_qs
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            limit = min(int((params.get("limit", ["2000"])[0])), 5000)
            q = (params.get("q", [""])[0]).lower()
            lines = _tail_lines(LOG_PATH, limit)
            if q:
                lines = [l for l in lines if q in l.lower()]
            self._json_response(200, {"lines": list(reversed(lines)), "total": len(lines)})

        else:
            super().do_GET()

    def do_POST(self):
        self._json_response(404, {"error": "Not found"})

    def log_message(self, fmt: str, *args):
        pass  # Silence request logs


def main():
    host = "127.0.0.1"
    port = 8787
    server = HTTPServer((host, port), DashboardHandler)
    print(f"Dashboard: http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
