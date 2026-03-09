#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


HISTORY_PATH = Path("logs/position_history.jsonl")
STATUS_PATH = Path("logs/status.json")


@dataclass
class WindowConfig:
    hours: int
    profit_buffer_usd: float
    funding_interval_seconds: int
    max_funding_windows: int


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    return rows


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def ts_to_local(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone()


def format_money(value: float) -> str:
    return f"${value:+.4f}"


def format_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")


def recent_rows(rows: list[dict[str, Any]], cutoff_ts: float) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        ts = row.get("ts")
        if ts is None:
            continue
        try:
            if float(ts) >= cutoff_ts:
                result.append(row)
        except Exception:
            continue
    return result


def build_unclosed_open_index(rows: list[dict[str, Any]]) -> dict[tuple[str, str, str], dict[str, Any]]:
    closed_ids = {
        row.get("position_id")
        for row in rows
        if row.get("event") in ("CLOSE", "CLOSE_FORCED")
    }
    unmatched: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in reversed(rows):
        if row.get("event") != "OPEN":
            continue
        position_id = row.get("position_id")
        if position_id in closed_ids:
            continue
        key = (
            row.get("exchange", "unknown"),
            row.get("symbol", "unknown"),
            row.get("strategy", "unknown"),
        )
        if key not in unmatched:
            unmatched[key] = row
    return unmatched


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def summarize_realized(rows: list[dict[str, Any]], cfg: WindowConfig) -> list[str]:
    closes = [r for r in rows if r.get("event") in ("CLOSE", "CLOSE_FORCED")]
    close_only = [r for r in closes if r.get("event") == "CLOSE"]
    forced = [r for r in closes if r.get("event") == "CLOSE_FORCED"]

    realized_close = sum(safe_float(r.get("pnl", r.get("net_pnl", 0))) for r in close_only)
    realized_total = sum(safe_float(r.get("pnl", r.get("net_pnl", 0))) for r in closes)
    funding_total = sum(safe_float(r.get("funding_fee", 0)) for r in close_only)
    trading_fee_total = sum(safe_float(r.get("trading_fee", 0)) for r in close_only)

    by_exchange: dict[str, float] = defaultdict(float)
    by_reason_count: dict[str, int] = defaultdict(int)
    by_reason_pnl: dict[str, float] = defaultdict(float)
    early_closes: list[dict[str, Any]] = []
    stale_closes: list[dict[str, Any]] = []

    for row in closes:
        pnl = safe_float(row.get("pnl", row.get("net_pnl", 0)))
        exchange = row.get("exchange", "unknown")
        raw_reason = row.get("close_reason", "unknown")
        reason = "Stale position recycled" if str(raw_reason).startswith("Stale position recycled") else raw_reason
        by_exchange[exchange] += pnl
        by_reason_count[reason] += 1
        by_reason_pnl[reason] += pnl
        hold_seconds = safe_float(row.get("hold_seconds", 0))
        if row.get("event") == "CLOSE" and 0 < hold_seconds < cfg.funding_interval_seconds:
            early_closes.append(row)
        if isinstance(reason, str) and reason.startswith("Stale position recycled"):
            stale_closes.append(row)

    lines = [
        "Realized",
        f"  closes: {len(close_only)} normal, {len(forced)} forced",
        f"  pnl: {format_money(realized_close)} normal, {format_money(realized_total)} incl forced",
        f"  funding earned on closed trades: {format_money(funding_total)}",
        f"  trading fees on closed trades: {format_money(trading_fee_total)}",
    ]
    for exchange, pnl in sorted(by_exchange.items()):
        lines.append(f"  exchange {exchange}: {format_money(pnl)}")
    for reason in sorted(by_reason_count):
        lines.append(
            f"  reason {reason}: {by_reason_count[reason]} trades, {format_money(by_reason_pnl[reason])}"
        )
    lines.append(f"  closes under 8h funding window: {len(early_closes)}")
    lines.append(
        f"  stale recycled closes: {len(stale_closes)} trades, {format_money(sum(safe_float(r.get('pnl', 0)) for r in stale_closes))}"
    )

    if early_closes:
        lines.append("  early close detail:")
        for row in early_closes[:10]:
            lines.append(
                "    "
                f"{row.get('exchange')} {row.get('symbol')} "
                f"hold={safe_float(row.get('hold_seconds')) / 3600:.2f}h "
                f"pnl={format_money(safe_float(row.get('pnl', row.get('net_pnl', 0))))} "
                f"reason={row.get('close_reason', 'unknown')}"
            )
    return lines


def summarize_recent_opens(rows: list[dict[str, Any]]) -> list[str]:
    opens = [r for r in rows if r.get("event") == "OPEN"]
    if not opens:
        return ["Opened In Window", "  none"]

    by_exchange: dict[str, int] = defaultdict(int)
    skipped_fields_present = 0
    lines = ["Opened In Window", f"  opens: {len(opens)}"]
    for row in opens:
        by_exchange[row.get("exchange", "unknown")] += 1
        if row.get("expected_funding_per_window_usd") is not None:
            skipped_fields_present += 1
    for exchange, count in sorted(by_exchange.items()):
        lines.append(f"  exchange {exchange}: {count}")
    lines.append(f"  opens with projected funding fields: {skipped_fields_present}/{len(opens)}")
    return lines


def summarize_open_book(
    status: dict[str, Any],
    unmatched_opens: dict[tuple[str, str, str], dict[str, Any]],
    cfg: WindowConfig,
    now_ts: float,
) -> list[str]:
    exchanges = status.get("exchanges", [])
    total_unrealized = 0.0
    lines = ["Open Book"]
    for exchange in exchanges:
        ex_name = exchange.get("exchange", "unknown")
        open_positions = exchange.get("open_positions", [])
        ex_total = sum(safe_float(pos.get("pnl", {}).get("total_pnl", 0)) for pos in open_positions)
        total_unrealized += ex_total
        lines.append(f"  exchange {ex_name}: {len(open_positions)} positions, unrealized {format_money(ex_total)}")
    lines.append(f"  total unrealized: {format_money(total_unrealized)}")

    detail_lines: list[str] = []
    for exchange in exchanges:
        ex_name = exchange.get("exchange", "unknown")
        for pos in exchange.get("open_positions", []):
            strategy = pos.get("strategy", "unknown")
            symbol = pos.get("symbol", "unknown")
            pnl = pos.get("pnl", {})
            total_pnl = safe_float(pnl.get("total_pnl", 0))
            funding_fee = safe_float(pnl.get("funding_fee", 0))
            entry_time = safe_float(pos.get("entry_time", 0))
            elapsed_seconds = max(0.0, now_ts - entry_time)
            elapsed_windows = elapsed_seconds / cfg.funding_interval_seconds if cfg.funding_interval_seconds > 0 else 0.0
            unmatched = unmatched_opens.get((ex_name, symbol, strategy), {})
            expected_window = safe_float(unmatched.get("expected_funding_per_window_usd", 0.0))
            projected_windows = safe_float(unmatched.get("estimated_windows_to_profit_buffer", math.nan), math.nan)
            blockers: list[str] = []
            if total_pnl <= cfg.profit_buffer_usd:
                blockers.append("net")
            if funding_fee < cfg.profit_buffer_usd:
                blockers.append("fund")
            status_note = "healthy"
            if elapsed_windows >= cfg.max_funding_windows and blockers:
                status_note = "stale_now"
            elif blockers and elapsed_windows >= (cfg.max_funding_windows - 1):
                status_note = "near_stale"
            elif blockers:
                status_note = "waiting_buffer"
            detail = (
                f"  {ex_name} {symbol}: pnl={format_money(total_pnl)} "
                f"funding={format_money(funding_fee)} windows={elapsed_windows:.1f} "
                f"status={status_note}"
            )
            if expected_window > 0:
                detail += f" expected/8h={format_money(expected_window)}"
            if not math.isnan(projected_windows):
                detail += f" entry_proj_windows={projected_windows:.1f}"
            detail_lines.append(detail)

    lines.append("  detail:")
    if detail_lines:
        lines.extend(detail_lines)
    else:
        lines.append("  no open positions")
    return lines


def summarize_findings(
    status: dict[str, Any],
    unmatched_opens: dict[tuple[str, str, str], dict[str, Any]],
    cfg: WindowConfig,
    now_ts: float,
) -> list[str]:
    findings: list[str] = []
    exchanges = status.get("exchanges", [])
    for exchange in exchanges:
        ex_name = exchange.get("exchange", "unknown")
        for pos in exchange.get("open_positions", []):
            strategy = pos.get("strategy", "unknown")
            symbol = pos.get("symbol", "unknown")
            pnl = pos.get("pnl", {})
            total_pnl = safe_float(pnl.get("total_pnl", 0))
            funding_fee = safe_float(pnl.get("funding_fee", 0))
            entry_time = safe_float(pos.get("entry_time", 0))
            elapsed_windows = max(0.0, now_ts - entry_time) / cfg.funding_interval_seconds
            if funding_fee <= 0:
                findings.append(f"{ex_name} {symbol}: funding is not accruing yet")
                continue
            realized_windows_to_buffer = cfg.profit_buffer_usd / max(funding_fee / max(elapsed_windows, 1e-9), 1e-9)
            if realized_windows_to_buffer > cfg.max_funding_windows:
                findings.append(
                    f"{ex_name} {symbol}: realized funding pace implies {realized_windows_to_buffer:.1f} windows "
                    f"to reach ${cfg.profit_buffer_usd:.2f}"
                )
            if total_pnl < 0:
                findings.append(f"{ex_name} {symbol}: still negative mark-to-market at {format_money(total_pnl)}")
            unmatched = unmatched_opens.get((ex_name, symbol, strategy), {})
            expected = safe_float(unmatched.get("expected_funding_per_window_usd", 0))
            if expected > 0 and expected < (cfg.profit_buffer_usd / cfg.max_funding_windows):
                findings.append(
                    f"{ex_name} {symbol}: entry expected funding/8h {format_money(expected)} was below target pace"
                )
    lines = ["Findings"]
    if not findings:
        lines.append("  none")
    else:
        for item in findings[:12]:
            lines.append(f"  {item}")
    return lines


def build_report(hours: int) -> str:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=hours)
    now_ts = now.timestamp()
    history_rows = load_jsonl(HISTORY_PATH)
    status = load_json(STATUS_PATH)
    unmatched_opens = build_unclosed_open_index(history_rows)
    window_rows = recent_rows(history_rows, cutoff.timestamp())
    cfg = WindowConfig(
        hours=hours,
        profit_buffer_usd=0.30,
        funding_interval_seconds=28800,
        max_funding_windows=6,
    )

    lines = [
        f"{hours}h Trading Report",
        f"Window start: {format_dt(ts_to_local(cutoff.timestamp()))}",
        f"Window end:   {format_dt(ts_to_local(now_ts))}",
        f"History file: {HISTORY_PATH}",
        f"Status file:  {STATUS_PATH}",
        "",
    ]
    lines.extend(summarize_realized(window_rows, cfg))
    lines.append("")
    lines.extend(summarize_recent_opens(window_rows))
    lines.append("")
    lines.extend(summarize_open_book(status, unmatched_opens, cfg, now_ts))
    lines.append("")
    lines.extend(summarize_findings(status, unmatched_opens, cfg, now_ts))
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Recent trading report from history + live status.")
    parser.add_argument("--hours", type=int, default=24, help="Window size in hours")
    parser.add_argument("--output", type=Path, default=None, help="Optional path to write the report")
    args = parser.parse_args()

    report = build_report(args.hours)
    print(report, end="")

    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
