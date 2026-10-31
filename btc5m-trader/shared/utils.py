"""
BTC 5M shared utilities.
"""
import json, os, time
from datetime import datetime, timezone, timedelta
from pathlib import Path

CN = timezone(timedelta(hours=8))
# Default data root is <repo root>/data so the collector and the trader
# share one data directory even without an environment variable.
# Override it with BTC5M_DATA_DIR.
DATA_ROOT = Path(os.environ.get("BTC5M_DATA_DIR", str(Path(__file__).resolve().parents[2] / "data")))
# Backwards-compatible alias for the old DATA_BASE name.
DATA_BASE = DATA_ROOT


def log(msg, mode="sim", tag=""):
    ts = datetime.now(CN).strftime("%H:%M:%S")
    prefix = "🟡" if mode == "live" else "🔵"
    tag_str = f"[{tag}] " if tag else ""
    print(f"[{ts}] {prefix} {tag_str}{msg}", flush=True)


def notify(msg, mode="sim"):
    notify_dir = DATA_ROOT / mode
    with open(notify_dir / "notify.txt", "a") as f:
        f.write(f"[{datetime.now(CN).strftime('%H:%M:%S')}] {msg}\n")
    log(f"📢 {msg}", mode)


def log_trade(record, mode="sim"):
    trades_file = DATA_ROOT / mode / "trades.jsonl"
    with open(trades_file, "a") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
