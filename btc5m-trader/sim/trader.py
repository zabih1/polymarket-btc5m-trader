#!/usr/bin/env python3
"""
BTC 5M Sim Trader — simulation only, no browser or wallet connection
Fully isolated: separate config / state / trades / logs
"""
import os, sys, json, time, threading
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime, timezone, timedelta
from pathlib import Path

from shared.utils import log, log_trade, CN
from shared.config import Config
from shared.btc_price import get_btc, get_btc_fresh, btc_price_loop, extract_tokens, find_market, fetch_platform_crypto_price

# === Paths ===
MODE = "sim"
TRADER_DIR = Path(__file__).parent
# Data root: BTC5M_DATA_DIR, or ./data under the repo root by default.
# This file lives at btc5m-trader/<mode>/trader.py, so parents[2] is the repo root.
REPO_ROOT = Path(__file__).resolve().parents[2]
_data_dir_env = os.environ.get("BTC5M_DATA_DIR", "").strip()
if _data_dir_env:
    _data_root = Path(os.path.expanduser(_data_dir_env))
    if not _data_root.is_absolute():
        _data_root = REPO_ROOT / _data_root
    DATA_ROOT = _data_root
else:
    DATA_ROOT = REPO_ROOT / "data"
DATA_DIR = DATA_ROOT / MODE
DATA_DIR.mkdir(parents=True, exist_ok=True)

STATE_FILE = DATA_DIR / "state.json"
NOTIFY_FILE = DATA_DIR / "notify.txt"

ROOT_TRADES = DATA_ROOT / "trades.jsonl"

# === Shared data (read-only) ===
COLLECTOR_DATA = DATA_ROOT / "shared"
MARKETS_FILE = COLLECTOR_DATA / "markets.jsonl"
TRUE_MARKET_DIR = DATA_ROOT / "true_market"
WINDOWS_FILE = TRUE_MARKET_DIR / "windows.jsonl"
ORDERBOOK_TICKS_FILE = TRUE_MARKET_DIR / "orderbook_ticks.jsonl"
DATA_QUALITY_FILE = TRUE_MARKET_DIR / "data_quality.jsonl"

# === Runtime state ===
config = Config(str(TRADER_DIR))
cfg = config.data
bankroll = 1.0  # Default value, overridden by config.initial_capital
trade_count = 0
win_count = 0
loss_count = 0
total_withdrawn = 0.0
consecutive_losses = 0
cooldown_until = 0
btc_price = 0.0
platform_open_cache = {}


def load_state():
    global bankroll, trade_count, win_count, loss_count, total_withdrawn, consecutive_losses, cooldown_until
    state = config.load_state()
    bankroll = state.get("bankroll", float(cfg.get("initial_capital", 1.0)))
    trade_count = state.get("trade_count", 0)
    win_count = state.get("win_count", 0)
    loss_count = state.get("loss_count", 0)
    total_withdrawn = state.get("total_withdrawn", 0.0)
    consecutive_losses = state.get("consecutive_losses", 0)
    cooldown_until = state.get("cooldown_until", 0)
    log(f"State loaded: ${bankroll:.2f}/{trade_count} trades ({win_count}W/{loss_count}L)", MODE)


def save_state():
    state = {
        "bankroll": bankroll, "trade_count": trade_count,
        "win_count": win_count, "loss_count": loss_count,
        "total_withdrawn": total_withdrawn,
        "consecutive_losses": consecutive_losses,
        "cooldown_until": cooldown_until,
    }
    config.save_state(state)
    try:
        # Update legacy trader_state.json
        root_state = DATA_ROOT / "trader_state.json"
        with open(root_state, "w") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
    except:
        pass


def load_markets() -> list:
    """Load market data, preferring true_market/windows.jsonl (take the latest valid record)."""
    markets_dict = {}
    try:
        if WINDOWS_FILE.exists():
            with open(WINDOWS_FILE) as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        d = json.loads(line)
                        if d.get("source") not in ("polymarket_gamma", "ptb_retry_fill"):
                            continue
                        slug = d.get("slug", "")
                        if not slug:
                            continue
                        prices = {}
                        outcome_prices = d.get("outcome_prices", "[]")
                        if outcome_prices is None:
                            outcome_prices = "[]"
                        if isinstance(outcome_prices, str):
                            try:
                                op = json.loads(outcome_prices)
                                if len(op) >= 2:
                                    tid_up = d.get("token_up", "")
                                    tid_down = d.get("token_down", "")
                                    if tid_up:
                                        prices[tid_up] = float(op[0])
                                    if tid_down:
                                        prices[tid_down] = float(op[1])
                            except:
                                pass
                        # Later records overwrite earlier ones (take the latest data).
                        existing = markets_dict.get(slug, {})
                        ptb = d.get("ptb") or existing.get("priceToBeat", 0)
                        if not prices:
                            prices = existing.get("prices", {})
                        if not d.get("token_up"):
                            token_up = existing.get("token_up", "")
                            token_down = existing.get("token_down", "")
                        else:
                            token_up = d.get("token_up", "")
                            token_down = d.get("token_down", "")
                        markets_dict[slug] = {
                            "slug": slug,
                            "priceToBeat": ptb,
                            "token_up": token_up,
                            "token_down": token_down,
                            "prices": prices,
                            "closed": d.get("closed", False),
                        }
                    except:
                        pass
    except:
        pass
    markets = list(markets_dict.values())
    if markets:
        return markets
    
    # Fall back to markets.jsonl
    try:
        if MARKETS_FILE.exists():
            with open(MARKETS_FILE) as f:
                return [json.loads(l) for l in f if l.strip()]
    except:
        pass
    return []


def fetch_ptb(slug: str) -> float:
    """Get the verified PTB from true_market/windows.jsonl (take the latest valid record)."""
    best_ptb = 0.0
    try:
        if WINDOWS_FILE.exists():
            with open(WINDOWS_FILE) as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        d = json.loads(line)
                        if d.get("slug") != slug:
                            continue
                        src = d.get("source", "")
                        if src not in ("polymarket_gamma", "ptb_retry_fill", "platform_validation"):
                            continue
                        if src == "platform_validation":
                            ptb = d.get("platform_ptb") or 0
                            diff = parse_float(d.get("diff"), 999999)
                            if diff > 1:
                                continue
                        else:
                            if d.get("ptb_quality") != "platform" or d.get("exclude_from_backtest"):
                                continue
                            ptb = d.get("ptb") or 0
                        if ptb and float(ptb) > 0:
                            best_ptb = float(ptb)
                    except:
                        pass
    except:
        pass
    return best_ptb
    
    # Fall back to markets.jsonl
    try:
        if MARKETS_FILE.exists():
            with open(MARKETS_FILE) as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        d = json.loads(line)
                        if d.get("slug") == slug and d.get("type") == "market_open":
                            ptb = d.get("priceToBeat", 0)
                            if ptb and ptb > 0:
                                return float(ptb)
                    except:
                        pass
    except:
        pass
    return 0.0


def parse_float(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except:
        return default


def calc_bet_size(current_bankroll: float) -> float:
    """Return a realistic simulated stake. Fixed $1 is the current baseline."""
    mode = str(cfg.get("bet_mode", "fraction") or "fraction")
    if mode in ("amount", "fixed_amount"):
        target = parse_float(cfg.get("fixed_bet_amount", 1.0), 1.0)
    else:
        target = current_bankroll * parse_float(cfg.get("bet_fraction", 1.0), 1.0)
    target = max(1.0, target)
    return round(min(target, max(1.0, current_bankroll)), 6)


def fetch_platform_open_cached(window_start_ts: int, window_end_ts: int) -> float:
    """Use Polymarket's official open price for the current 5m market."""
    now = time.time()
    cached = platform_open_cache.get(window_start_ts)
    if cached and cached.get("open_price", 0) > 0:
        return float(cached["open_price"])
    if cached and now - cached.get("checked_at", 0) < 2:
        return 0.0

    platform_price = fetch_platform_crypto_price(
        window_start_ts,
        window_end_ts,
        max_retries=1,
        retry_interval=0,
    )
    open_price = platform_price.get("openPrice") or 0
    platform_open_cache[window_start_ts] = {
        "open_price": float(open_price or 0),
        "checked_at": now,
        "source": platform_price.get("source", ""),
    }
    return float(open_price or 0)


def tail_jsonl(path: Path, n=300):
    if not path.exists():
        return []
    try:
        import subprocess
        r = subprocess.run(["tail", "-n", str(n), str(path)], capture_output=True, text=True, timeout=3)
        rows = []
        for line in r.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except:
                pass
        return rows
    except:
        return []


def latest_data_quality(slug: str) -> dict:
    for row in reversed(tail_jsonl(DATA_QUALITY_FILE, 200)):
        if row.get("slug") == slug:
            return row
    return {}


def data_quality_allows_entry(slug: str, current_5m: int) -> tuple[bool, str]:
    q = latest_data_quality(slug)
    if not q:
        return False, "Data quality missing"
    if q.get("current_market_slug") != slug:
        return False, "Collector market not in sync"
    if int(q.get("current_window_start_ts") or 0) != int(current_5m):
        return False, "Collector window time mismatch"
    if not q.get("token_ids_ready"):
        return False, "Market tokens not ready"
    if not q.get("clob_ws_online"):
        return False, "Order book WebSocket offline"
    orderbook_age = parse_float(q.get("last_orderbook_tick_age_ms"), 999999)
    if abs(orderbook_age) > 3000:
        return False, f"Order book lag {orderbook_age:.0f}ms"
    if q.get("ptb_pending"):
        return False, "Platform open price pending confirmation"
    return True, ""


def _side_quote(side_data: dict, sim_data: dict | None = None):
    side_data = side_data if isinstance(side_data, dict) else {}
    bids = side_data.get("bids", []) if isinstance(side_data.get("bids"), list) else []
    asks = side_data.get("asks", []) if isinstance(side_data.get("asks"), list) else []
    bid = parse_float(side_data.get("bid1_price"), 0)
    ask = parse_float(side_data.get("ask1_price"), 0)
    if bid <= 0 and bids:
        bid = parse_float((bids[0] or {}).get("price"), 0)
    if ask <= 0 and asks:
        ask = parse_float((asks[0] or {}).get("price"), 0)
    mid = round((bid + ask) / 2, 4) if bid > 0 and ask > 0 else ask or bid or 0
    sim_data = sim_data if isinstance(sim_data, dict) else {}
    avg_fill = parse_float(sim_data.get("simulated_avg_fill_price"), 0)
    fill_quality = sim_data.get("fill_quality", "")
    # Live-oriented simulation: a signal is tradable only if a full $1 market
    # buy can be filled from the current orderbook.
    probability = avg_fill if avg_fill > 0 and fill_quality == "full" else 0
    return {
        "probability": probability,
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "avg_fill": avg_fill,
        "fill_quality": fill_quality,
        "available_liquidity_at_entry": parse_float(sim_data.get("available_liquidity_at_entry"), 0),
        "slippage_vs_best_ask": parse_float(sim_data.get("slippage_vs_best_ask"), 0),
    }


def latest_entry_quote(slug: str, direction: str, max_age_seconds=3):
    """Take the latest order book probability at entry; do not use the market-open outcome_prices."""
    side_key = "up" if direction.lower() == "up" else "down"
    sim_key = f"{side_key}_sim"
    now = datetime.now(timezone.utc)
    allowed_reasons = {"market_open", "periodic", "periodic_1s", "pre_close_1s"}
    cache_rows = list(reversed(tail_jsonl(ORDERBOOK_TICKS_FILE, 2000)))
    # First pass: exact slug match (normal case)
    for row in cache_rows:
        if row.get("slug") != slug:
            continue
        if row.get("reason") not in allowed_reasons:
            continue
        age = None
        try:
            ts = datetime.fromisoformat(str(row.get("received_at") or row.get("server_ts") or "").replace("Z", "+00:00"))
            age = (now - ts).total_seconds()
        except:
            pass
        if age is not None and age > max_age_seconds:
            continue
        if isinstance(row.get(side_key), dict) and isinstance(row.get(sim_key), dict):
            quote = _side_quote(row.get(side_key), row.get(sim_key))
            if quote["probability"] > 0 and quote.get("fill_quality") == "full":
                quote["source"] = row.get("reason", "orderbook_snapshot")
                quote["age_seconds"] = round(age, 3) if age is not None else None
                return quote
    return {"probability": 0, "source": "missing_orderbook", "age_seconds": None}




def _slug_exists_in_trades(slug):
    """Check whether trades.jsonl already has a record for this slug."""
    try:
        if ROOT_TRADES.exists():
            with open(ROOT_TRADES, "r", encoding="utf-8-sig") as f:
                for line in f:
                    if slug in line:
                        return True
    except:
        pass
    return False

def record_skip(slug, reason, current_5m, btc_price=0, ptb=0, gap=0, buy_price=0, direction="none", quote=None, monitor=None):
    """Record a skipped market."""
    if _slug_exists_in_trades(slug):
        return
    now_ts = int(time.time())
    window_end_ts = current_5m + 300
    remaining = window_end_ts - now_ts
    entry_seconds_before = max(0, remaining)
    market_time_short = datetime.fromtimestamp(current_5m, CN).strftime("%m-%d %H:%M") + "-" + datetime.fromtimestamp(window_end_ts, CN).strftime("%H:%M")
    quote = quote or {}
    monitor = monitor or {}
    record = {
        "slug": slug,
        "time": datetime.now(CN).isoformat(),
        "mode": "sim",
        "direction": direction if direction in ("Up", "Down") else "none",
        "settlement_direction": None,
        "market_slug": slug,
        "window_start_ts": current_5m,
        "window_end_ts": window_end_ts,
        "market_time": market_time_short,
        "market_time_short": market_time_short,
        "actual_entry_ts": now_ts,
        "entry_seconds_before": entry_seconds_before,
        "open_price": round(ptb, 2) if ptb else None,
        "platform_open_price": round(ptb, 2) if ptb else None,
        "btc_entry": round(btc_price, 2) if btc_price else None,
        "btc_final": None,
        "platform_close_price": None,
        "entry_gap": round(gap, 2) if gap else None,
        "buy_gap": round(gap, 2) if gap else None,
        "settlement_gap": None,
        "close_gap": None,
        "probability": buy_price if buy_price else None,
        "entry_probability": buy_price if buy_price else None,
        "probability_source": quote.get("source", ""),
        "probability_age_seconds": quote.get("age_seconds"),
        "best_bid": quote.get("bid"),
        "best_ask": quote.get("ask"),
        "avg_fill_price": quote.get("avg_fill"),
        "fill_quality": quote.get("fill_quality"),
        "available_liquidity_at_entry": quote.get("available_liquidity_at_entry"),
        "slippage_vs_best_ask": quote.get("slippage_vs_best_ask"),
        "amount": 0,
        "pnl": 0,
        "return": 0,
        "settlement_status": "skipped",
        "settle_source": "",
        "settle_confirmed_at": None,
        "skip_reason": reason,
        "exclude_from_backtest": True,
        "status": "skipped",
        "monitor_window_seconds": monitor.get("window_seconds"),
        "monitor_started_at": monitor.get("started_at"),
        "monitor_evaluations": monitor.get("evaluations"),
        "best_probability_seen": monitor.get("best_probability"),
        "best_gap_seen": monitor.get("best_gap"),
    }
    log_trade(record, MODE)
    try:
        with open(ROOT_TRADES, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except:
        pass


def notify(msg):
    with open(NOTIFY_FILE, "a") as f:
        f.write(f"[{datetime.now(CN).strftime('%H:%M:%S')}] {msg}\n")
    log(f"📢 {msg}", MODE)


# ===== Settle old records =====
_last_settle_scan = 0

def settle_old_records():
    """Scan trades.jsonl for unsettled old records and fetch the platform settlement price."""
    global _last_settle_scan
    now = time.time()
    # Scan every 10 seconds
    if now - _last_settle_scan < 10:
        return
    _last_settle_scan = now
    
    try:
        if not ROOT_TRADES.exists():
            return
        with open(ROOT_TRADES, "r", encoding="utf-8-sig") as f:
            lines = f.readlines()
        
        updated = False
        settled_count = 0
        for i, line in enumerate(lines):
            # Settle at most 1 record per pass to avoid blocking the main loop
            if settled_count >= 1:
                break
            if not line.strip():
                continue
            record = json.loads(line)
            # Only handle skipped or pending records that have no settlement price yet
            if record.get("settlement_status") not in ("skipped", "pending"):
                continue
            if record.get("btc_final") is not None:
                continue
            # Check that the window has ended (at least 30 seconds ago)
            window_end = record.get("window_end_ts", 0)
            if window_end <= 0 or now < window_end + 30:
                continue
            
            # Fetch the settlement price
            window_start = record.get("window_start_ts", 0)
            try:
                platform_price = fetch_platform_crypto_price(window_start, window_end, max_retries=2, retry_interval=0.5)
                btc_final = platform_price.get("closePrice")
                if btc_final and btc_final > 0:
                    open_price = record.get("open_price", 0) or 0
                    if open_price > 0:
                        actual_winner = "Up" if btc_final >= open_price else "Down"
                    else:
                        actual_winner = ""
                    record["btc_final"] = round(btc_final, 2)
                    record["platform_close_price"] = round(btc_final, 2)
                    if open_price > 0:
                        record["settlement_gap"] = round(btc_final - open_price, 2)
                        record["close_gap"] = round(btc_final - open_price, 2)
                    record["settlement_direction"] = actual_winner
                    record["settle_source"] = platform_price.get("source", "")
                    record["settle_confirmed_at"] = datetime.now(CN).isoformat()
                    lines[i] = json.dumps(record, ensure_ascii=False) + "\n"
                    updated = True
                    log(f"✅ Settlement price updated: {record.get('slug')} close=${btc_final:,.2f} winner={actual_winner}", MODE)
                    settled_count += 1
            except Exception as e:
                log(f"⚠️ Failed to fetch settlement price {record.get('slug')}: {e}", MODE)
        
        if updated:
            with open(ROOT_TRADES, "w", encoding="utf-8") as f:
                f.writelines(lines)
    except Exception as e:
        log(f"⚠️ Settlement scan error: {e}", MODE)


# ===== Main loop =====
def main():
    global cfg, btc_price, bankroll, trade_count, win_count, loss_count, consecutive_losses, cooldown_until

    log("=" * 50, MODE)
    log("BTC 5M SIM TRADER", MODE)
    log("Mode: simulation only - no browser connection, no real orders", MODE)
    log("=" * 50, MODE)

    config.load()
    load_state()

    # Start the price thread
    threading.Thread(target=btc_price_loop, daemon=True).start()

    time.sleep(2)
    btc_price = get_btc()

    processed = set()
    pending_settlements = {}  # slug -> record_path
    monitor_state = {}

    while True:
        try:
            # Check old-record settlement on every loop
            settle_old_records()
            
            config.load()
            cfg = config.data

            now = int(time.time())
            current_5m = (now // 300) * 300
            slug = f"btc-updown-5m-{current_5m}"
            remaining = current_5m + 300 - now

            for old_slug, state in list(monitor_state.items()):
                if old_slug != slug and old_slug not in processed:
                    record_skip(
                        old_slug,
                        state.get("last_reason", "entry_window_ended_without_signal"),
                        state.get("current_5m", current_5m - 300),
                        state.get("btc_price", 0),
                        state.get("ptb", 0),
                        state.get("gap", 0),
                        state.get("buy_price", 0),
                        state.get("direction", "none"),
                        state.get("quote", {}),
                        state,
                    )
                    processed.add(old_slug)
                    monitor_state.pop(old_slug, None)

            if cfg.get("paused", False):
                time.sleep(5)
                continue
            if cooldown_until > now:
                time.sleep(3)
                continue
            if slug in processed:
                time.sleep(0.5)
                continue

            entry_window_seconds = max(1, min(120, int(float(cfg.get("entry_second", 25)))))
            if remaining > entry_window_seconds:
                # Sleep until the entry window starts, but at most 2 seconds to avoid missing it
                time.sleep(min(max(remaining - entry_window_seconds, 0.2), 2))
                continue
            if remaining <= 0:
                # Window ended; record the skip reason
                monitor = monitor_state.get(slug, {})
                last_reason = monitor.get("last_reason", "Window ended without an entry trigger")
                # Only re-evaluate when last_reason is still the initial value
                if last_reason == "waiting_for_signal":
                    # Check whether there is specific data
                    best_prob = monitor.get("best_probability", 0)
                    best_gap = monitor.get("best_gap", 0)
                    if best_prob > 0 and best_prob < cfg.get("min_buy_price", 0.6):
                        last_reason = f"probability {best_prob:.0%} < {cfg['min_buy_price']:.0%}"
                    elif best_gap > 0 and abs(best_gap) < cfg.get("gap_threshold", 10):
                        last_reason = f"gap ${best_gap:.0f} < ${cfg['gap_threshold']}"
                    elif best_prob <= 0:
                        last_reason = "No valid probability data"
                    else:
                        last_reason = "Window ended without an entry trigger"
                log(f"⏭ {slug} {last_reason} skipped", MODE, tag="SIM")
                record_skip(slug, last_reason, current_5m, 
                           monitor.get("btc_price", 0), 
                           monitor.get("ptb", 0), 
                           monitor.get("gap", 0), 
                           monitor.get("buy_price", 0),
                           monitor.get("direction", "none"),
                           monitor.get("quote", {}),
                           monitor)
                processed.add(slug)
                monitor_state.pop(slug, None)
                time.sleep(0.2)
                continue

            monitor = monitor_state.setdefault(slug, {
                "current_5m": current_5m,
                "window_seconds": entry_window_seconds,
                "started_at": datetime.now(CN).isoformat(),
                "evaluations": 0,
                "best_probability": 0,
                "best_gap": 0,
                "last_reason": "waiting_for_signal",
                "ptb": 0,
                "ptb_source": "",
            })
            monitor["window_seconds"] = entry_window_seconds
            monitor["evaluations"] = int(monitor.get("evaluations", 0)) + 1

            quality_ok, quality_reason = data_quality_allows_entry(slug, current_5m)
            if not quality_ok:
                monitor["last_reason"] = f"Bad data: {quality_reason}"
                if remaining <= 1:
                    log(f"⏭ {slug} {monitor['last_reason']} skipped", MODE, tag="SIM")
                    record_skip(slug, monitor["last_reason"], current_5m, monitor=monitor)
                    processed.add(slug)
                    monitor_state.pop(slug, None)
                time.sleep(0.1)
                continue

            # Load markets
            markets = load_markets()

            # Fetch PTB (once only, stored in monitor and reused afterwards)
            window_end_ts = current_5m + 300
            if monitor.get("ptb", 0) > 0:
                ptb = monitor["ptb"]
                ptb_source = monitor.get("ptb_source", "cached")
            else:
                ptb = fetch_platform_open_cached(current_5m, window_end_ts)
                ptb_source = "platform_open"
                if ptb <= 0:
                    ptb_source = "collector_fallback"
                    ptb = fetch_ptb(slug)
                if ptb > 0:
                    monitor["ptb"] = ptb
                    monitor["ptb_source"] = ptb_source
            if ptb <= 0:
                monitor["last_reason"] = "PTB not fetched"
                if remaining <= 1:
                    record_skip(slug, monitor["last_reason"], current_5m, monitor=monitor)
                    processed.add(slug)
                    monitor_state.pop(slug, None)
                time.sleep(0.1)
                continue

            btc_price = get_btc()
            if btc_price <= 0:
                monitor["last_reason"] = "BTC spot price not fetched"
                time.sleep(0.1)
                continue

            gap = btc_price - ptb

            # Calculate direction and probability
            direction = "Up" if gap > 0 else "Down"
            quote = latest_entry_quote(slug, direction, max_age_seconds=3)
            buy_price = quote.get("probability", 0) or 0

            # If the current direction probability is too low (< min_buy_price), try the reverse direction
            if False and buy_price > 0 and buy_price < cfg["min_buy_price"]:
                reverse_dir = "Down" if direction == "Up" else "Up"
                reverse_quote = latest_entry_quote(slug, reverse_dir, max_age_seconds=3)
                reverse_price = reverse_quote.get("probability", 0) or 0
                if reverse_price > buy_price and reverse_price > cfg["min_buy_price"]:
                    log(f"🔄 {slug} {direction}probability {buy_price:.0%} too low, switching to {reverse_dir} at {reverse_price:.0%}", MODE, tag="SIM")
                    direction = reverse_dir
                    buy_price = reverse_price
                    quote = reverse_quote

            # Only fall back to market metadata when the order book is temporarily missing, and mark it explicitly so it is not mistaken for a second-level entry probability.
            if False and buy_price <= 0:
                m = find_market(markets, slug)
                tid_up, tid_down = extract_tokens(m)
                prices = m.get("prices", {})
                if prices and len(prices) > 0:
                    key = tid_up if direction == "Up" else tid_down
                    buy_price = prices.get(key, 0)
                    if buy_price <= 0:
                        # If the matching token is not found, try the first/second price
                        price_list = list(prices.values())
                        if len(price_list) >= 2:
                            buy_price = price_list[0] if direction == "Up" else price_list[1]
                    quote = {"source": "market_open_fallback", "age_seconds": None, "probability": buy_price}
                else:
                    # If prices is empty, try to get outcomePrices directly from the market data
                    outcome_prices_raw = m.get("outcome_prices", "[]")
                    if outcome_prices_raw is None:
                        outcome_prices_raw = "[]"
                    try:
                        op = json.loads(outcome_prices_raw) if isinstance(outcome_prices_raw, str) else outcome_prices_raw
                        if len(op) >= 2:
                            buy_price = float(op[0]) if direction == "Up" else float(op[1])
                            quote = {"source": "market_open_fallback", "age_seconds": None, "probability": buy_price}
                    except:
                        pass
                    log(f"DEBUG fallback: slug={slug} dir={direction} tid_up={tid_up[:10]}... tid_down={tid_down[:10]}... prices={prices} buy_price={buy_price}", MODE)

            monitor.update({
                "btc_price": btc_price,
                "ptb": ptb,
                "gap": gap,
                "buy_price": buy_price,
                "direction": direction,
                "quote": quote,
            })
            if abs(gap) > abs(float(monitor.get("best_gap", 0) or 0)):
                monitor["best_gap"] = round(gap, 2)
            if buy_price and buy_price > float(monitor.get("best_probability", 0) or 0):
                monitor["best_probability"] = round(buy_price, 4)

            if abs(gap) < cfg["gap_threshold"]:
                monitor["last_reason"] = f"gap=${gap:+,.0f}<${cfg['gap_threshold']}"
                if remaining <= 1:
                    log(f"⏭ {slug} {monitor['last_reason']} skipped", MODE, tag="SIM")
                    record_skip(slug, monitor["last_reason"], current_5m, btc_price, ptb, gap, buy_price, direction, quote, monitor)
                    processed.add(slug)
                    monitor_state.pop(slug, None)
                time.sleep(0.1)
                continue

            if buy_price <= 0 or buy_price > 0.99:
                monitor["last_reason"] = f"price {buy_price:.3f} not executable"
                if remaining <= 1:
                    log(f"⏭ {slug} {monitor['last_reason']} skipped", MODE, tag="SIM")
                    record_skip(slug, monitor["last_reason"], current_5m, btc_price, ptb, gap, buy_price, direction, quote, monitor)
                    processed.add(slug)
                    monitor_state.pop(slug, None)
                time.sleep(0.1)
                continue
            if buy_price < cfg["min_buy_price"]:
                monitor["last_reason"] = f"probability {buy_price:.0%} < {cfg['min_buy_price']:.0%}"
                if remaining <= 1:
                    log(f"⏭ {slug} {monitor['last_reason']} skipped", MODE, tag="SIM")
                    record_skip(slug, monitor["last_reason"], current_5m, btc_price, ptb, gap, buy_price, direction, quote, monitor)
                    processed.add(slug)
                    monitor_state.pop(slug, None)
                time.sleep(0.1)
                continue

            # === Liquidity check: a real sell order must be available to fill ===
            fill_quality = quote.get("fill_quality", "none")
            best_ask = quote.get("ask", 0) or 0
            if fill_quality != "full" or best_ask <= 0:
                monitor["last_reason"] = f"no liquidity (fill={fill_quality}, ask={best_ask})"
                if remaining <= 1:
                    log(f"⏭ {slug} {monitor['last_reason']} skipped", MODE, tag="SIM")
                    record_skip(slug, monitor["last_reason"], current_5m, btc_price, ptb, gap, buy_price, direction, quote, monitor)
                    processed.add(slug)
                    monitor_state.pop(slug, None)
                time.sleep(0.1)
                continue

            # === Simulated order ===
            actual_entry_ts = int(time.time())
            entry_seconds_before = max(0, current_5m + 300 - actual_entry_ts)
            entry_time = datetime.fromtimestamp(actual_entry_ts, CN).isoformat()
            entry_btc_price = get_btc() or btc_price
            entry_open_price = ptb  # Reuse the first fetched openPrice to avoid value changes
            entry_gap = entry_btc_price - entry_open_price
            entry_direction = "Up" if entry_gap > 0 else "Down"
            # Recheck at entry: allow if the direction matches and the gap is above half the threshold (avoids missing the trade on small BTC price moves)
            if entry_direction != direction:
                monitor["last_reason"] = f"entry_recheck_failed gap=${entry_gap:+,.2f} dir={entry_direction}"
                time.sleep(0.1)
                continue
            if abs(entry_gap) < cfg["gap_threshold"] * 0.5:
                monitor["last_reason"] = f"entry_gap=${entry_gap:+,.2f}<{cfg['gap_threshold']*0.5:.0f}"
                time.sleep(0.1)
                continue
            btc_price = entry_btc_price
            ptb = entry_open_price
            gap = entry_gap
            initial_capital = max(1.0, float(cfg.get("initial_capital", 1.0)))
            if bankroll < 1.0:
                bankroll = initial_capital
                consecutive_losses = 0
                log(f"🔄 Simulated funds below the platform minimum bet, auto-reset to ${initial_capital:.2f}", MODE, tag="SIM")
            bet_size = calc_bet_size(bankroll)
            log(f"🎯 {slug} {direction} gap=${gap:+,.0f} @{buy_price:.3f} bet ${bet_size:.2f} T-{entry_seconds_before}s", MODE, tag="SIM")

            # Simulated result: fetch the official settlement price from the Polymarket platform
            window_end_ts = current_5m + 300
            platform_price = fetch_platform_crypto_price(current_5m, window_end_ts, max_retries=300, retry_interval=2.0)
            
            btc_final = platform_price.get("closePrice")
            open_price = platform_price.get("openPrice") or ptb
            settle_source = platform_price.get("source", "unknown")
            
            if btc_final is None or btc_final <= 0:
                # Platform settlement price not returned; mark as pending settlement
                log(f"⚠️ Platform settlement price not returned, marked as pending: {slug}", MODE, tag="SIM")
                record = {
                    "slug": slug, "time": entry_time,
                    "entry_time": entry_time,
                    "mode": "sim", "direction": direction,
                    "market_slug": slug,
                    "window_start_ts": current_5m,
                    "window_end_ts": window_end_ts,
                    "actual_entry_ts": actual_entry_ts,
                    "entry_seconds_before": entry_seconds_before,
                    "market_time": datetime.fromtimestamp(current_5m, CN).strftime("%m-%d %H:%M") + "-" + datetime.fromtimestamp(window_end_ts, CN).strftime("%H:%M"),
                    "open_price": round(open_price, 2),
                    "platform_open_price": round(open_price, 2),
                    "btc_entry": round(entry_btc_price, 2),
                    "btc_final": None,
                    "platform_close_price": None,
                    "entry_gap": round(entry_gap, 2),
                    "buy_gap": round(entry_gap, 2),
                    "settlement_gap": None,
                    "close_gap": None,
                    "probability": buy_price,
                    "entry_probability": buy_price,
                    "probability_source": quote.get("source", ""),
                    "probability_age_seconds": quote.get("age_seconds"),
                    "best_bid": quote.get("bid"),
                    "best_ask": quote.get("ask"),
                    "avg_fill_price": quote.get("avg_fill"),
                    "fill_quality": quote.get("fill_quality"),
                    "available_liquidity_at_entry": quote.get("available_liquidity_at_entry"),
                    "slippage_vs_best_ask": quote.get("slippage_vs_best_ask"),
                    "amount": bet_size,
                    "bet_mode": cfg.get("bet_mode", "fraction"),
                    "fixed_bet_amount": cfg.get("fixed_bet_amount", 1.0),
                    "bet_fraction": cfg.get("bet_fraction", 1.0),
                    "pnl": None,
                    "return": None,
                    "settlement_status": "pending",
                    "settle_source": settle_source,
                    "settle_confirmed_at": None,
                    "skip_reason": "platform_close_price_missing",
                    "exclude_from_backtest": True,
                    "status": "pending",
                    "monitor_window_seconds": entry_window_seconds,
                    "monitor_started_at": monitor.get("started_at"),
                    "monitor_evaluations": monitor.get("evaluations"),
                    "best_probability_seen": monitor.get("best_probability"),
                    "best_gap_seen": monitor.get("best_gap"),
                }
                log_trade(record, MODE)
                if not _slug_exists_in_trades(slug):
                    try:
                        with open(ROOT_TRADES, "a", encoding="utf-8") as f:
                            f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    except:
                        pass
                processed.add(slug)
                monitor_state.pop(slug, None)
                continue
            
            # Correct win/loss rule: final price >= open price means Up wins
            actual_winner = "Up" if btc_final >= open_price else "Down"
            won = (direction == actual_winner)
            
            if won:
                profit = bet_size * (1 - buy_price) / buy_price
                bankroll += profit
                win_count += 1
                consecutive_losses = 0
                log(f"✅ Win! +${profit:.2f} balance=${bankroll:.2f}", MODE, tag="SIM")
            else:
                loss = bet_size
                bankroll -= loss
                loss_count += 1
                consecutive_losses += 1
                log(f"❌ Loss! -${loss:.2f} balance=${bankroll:.2f}", MODE, tag="SIM")
                # After a loss, if funds fall below the platform minimum bet, auto-reset to the configured initial capital
                if bankroll < 1.0:
                    initial = max(1.0, float(cfg.get("initial_capital", 1.0)))
                    bankroll = initial
                    consecutive_losses = 0
                    log(f"🔄 Simulated funds below the platform minimum bet, auto-reset to ${initial:.2f}", MODE, tag="SIM")

            trade_count += 1

            settle_confirmed_at = datetime.now(CN).isoformat()
            record = {
                "slug": slug, "time": entry_time,
                "entry_time": entry_time,
                "mode": "sim", "direction": direction,
                "actual_winner": actual_winner,
                "market_slug": slug,
                "window_start_ts": current_5m,
                "window_end_ts": window_end_ts,
                "actual_entry_ts": actual_entry_ts,
                "entry_seconds_before": entry_seconds_before,
                "market_time": datetime.fromtimestamp(current_5m, CN).strftime("%m-%d %H:%M") + "-" + datetime.fromtimestamp(window_end_ts, CN).strftime("%H:%M"),
                "open_price": round(open_price, 2),
                "platform_open_price": round(open_price, 2),
                "btc_entry": round(entry_btc_price, 2),
                "btc_final": round(btc_final, 2),
                "platform_close_price": round(btc_final, 2),
                "entry_gap": round(entry_gap, 2),
                "buy_gap": round(entry_gap, 2),
                "settlement_gap": round(btc_final - open_price, 2),
                "close_gap": round(btc_final - open_price, 2),
                "probability": buy_price,
                "entry_probability": buy_price,
                "probability_source": quote.get("source", ""),
                "probability_age_seconds": quote.get("age_seconds"),
                "best_bid": quote.get("bid"),
                "best_ask": quote.get("ask"),
                "avg_fill_price": quote.get("avg_fill"),
                "fill_quality": quote.get("fill_quality"),
                "available_liquidity_at_entry": quote.get("available_liquidity_at_entry"),
                "slippage_vs_best_ask": quote.get("slippage_vs_best_ask"),
                "amount": bet_size,
                "bet_mode": cfg.get("bet_mode", "fraction"),
                "fixed_bet_amount": cfg.get("fixed_bet_amount", 1.0),
                "bet_fraction": cfg.get("bet_fraction", 1.0),
                "pnl": round(profit if won else -loss, 2),
                "return": round((profit if won else -loss) / bet_size, 4),
                "settlement_status": "confirmed",
                "settle_source": settle_source,
                "settle_confirmed_at": settle_confirmed_at,
                "skip_reason": "",
                "exclude_from_backtest": False,
                "status": "won" if won else "lost",
                "monitor_window_seconds": entry_window_seconds,
                "monitor_started_at": monitor.get("started_at"),
                "monitor_evaluations": monitor.get("evaluations"),
                "best_probability_seen": monitor.get("best_probability"),
                "best_gap_seen": monitor.get("best_gap"),
            }
            log_trade(record, MODE)
            if not _slug_exists_in_trades(slug):
                try:
                    with open(ROOT_TRADES, "a", encoding="utf-8") as f:
                        f.write(json.dumps(record, ensure_ascii=False) + "\n")
                except:
                    pass
            save_state()
            processed.add(slug)
            monitor_state.pop(slug, None)


        except KeyboardInterrupt:
            log("🛑 Stopped", MODE)
            break
        except Exception as e:
            log(f"⚠️ Error: {e}", MODE)
            time.sleep(5)


if __name__ == "__main__":
    main()
