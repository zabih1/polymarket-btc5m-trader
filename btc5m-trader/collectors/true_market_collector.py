#!/usr/bin/env python3
"""
true_market_collector.py — Polymarket Real-Market Data Collector v3

Fixes:
1. orderbook_ticks: bid1/ask1 prefer the REST book top-of-book price, never 0
2. trade_ticks: only real trades; price_change goes to price_change_ticks.jsonl
3. market_resolved: filtered by condition_id, keeping only BTC market resolutions

Data sources:
  Tier A (real platform data, must take priority):
    1. Gamma API → market metadata, token IDs, order-book snapshots
    2. CLOB WebSocket -> real order book, trades, best bid/ask
    3. Polymarket RTDS Chainlink stream -> official real-time prices for BTC/ETH/SOL/XRP
    4. platform resolution results -> confirm the real winner

  Tier B (no platform history; collect it ourselves):
    1. per-second probability time series
    2. per-second order-book depth
    3. precise entry slippage

  Tier C (reference only; must not be used as backtest evidence):
    1. Binance BTC/USDT
    2. plain Chainlink latestRoundData

Output directory: data/true_market/
  - windows.jsonl          metadata for each 5-minute market window
  - price_ticks.jsonl      Chainlink RTDS official price stream
  - orderbook_ticks.jsonl  CLOB order-book snapshots (per second)
  - price_change_ticks.jsonl  CLOB price-change events (not trades)
  - trade_ticks.jsonl      CLOB real trade events
  - market_meta.jsonl      market metadata snapshots (open/T-60/T-30/T-10/close)
  - resolutions.jsonl      market resolution results (current BTC market only)
  - resolutions_debug.jsonl  all market resolution events (for debugging)
  - data_quality.jsonl     data quality status
"""

import json
import time
import datetime
import threading
import requests
import websocket
import re
import sys
import os
from pathlib import Path
from collections import deque

# ── SSL config (verification disabled to avoid proxy issues)──
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
SSL_VERIFY = False



# ── Configuration ──
GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
RTDS_WS_URL = "wss://ws-live-data.polymarket.com"
CHAINLINK_RPC = "https://polygon-bor-rpc.publicnode.com"
CHAINLINK_CONTRACT = "0xc907E116054Ad103354f2D350FD2514433D57F6f"
CHAINLINK_LATEST_ROUND_DATA_SIG = "0xfeaf968c"

REPO_ROOT = Path(__file__).resolve().parents[2]  # file is at <repo>/btc5m-trader/collectors/
DATA_ROOT = Path(os.environ.get("BTC5M_DATA_DIR") or (REPO_ROOT / "data"))
DATA_DIR = DATA_ROOT / "true_market"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Output files
WINDOWS_FILE = DATA_DIR / "windows.jsonl"
PRICE_TICKS_FILE = DATA_DIR / "price_ticks.jsonl"
ORDERBOOK_TICKS_FILE = DATA_DIR / "orderbook_ticks.jsonl"
PRICE_CHANGE_TICKS_FILE = DATA_DIR / "price_change_ticks.jsonl"
TRADE_TICKS_FILE = DATA_DIR / "trade_ticks.jsonl"
MARKET_META_FILE = DATA_DIR / "market_meta.jsonl"
RESOLUTIONS_FILE = DATA_DIR / "resolutions.jsonl"
RESOLUTIONS_DEBUG_FILE = DATA_DIR / "resolutions_debug.jsonl"
DATA_QUALITY_FILE = DATA_DIR / "data_quality.jsonl"
FALLBACK_PRICE_TICKS_FILE = DATA_DIR / "fallback_price_ticks.jsonl"
MARKET_INTEGRITY_FILE = DATA_DIR / "market_integrity.jsonl"

PRICE_DEGRADED_SECONDS = 75
PRICE_STALE_SECONDS = 180
ORDERBOOK_STALE_SECONDS = 8
EXPECTED_5M_SECONDS = 300
TAIL_SECONDS = 60
TAIL_MIN_SECONDS = 55

# Supported assets and timeframes
ASSETS = {
    "btc": {"name": "Bitcoin", "chainlink_symbol": "btc/usd", "timeframes": ["5m"]},
}

# ── RTDS tick ring buffer (10 minutes, ~1200 entries)──
rtds_tick_buffer = deque(maxlen=1200)  # 1 entry per second x 600 seconds = 1200

# ── Polymarket platform official price API ──
POLYMARKET_CRYPTO_API = "https://polymarket.com/api/crypto/crypto-price"

def fetch_platform_price(window_start_ts, window_end_ts, max_retries=10, retry_interval=2.0):
    """Fetch the official open and settlement prices from the Polymarket platform"""
    import datetime as dt
    try:
        start_dt = dt.datetime.fromtimestamp(window_start_ts, tz=dt.timezone.utc)
        end_dt = dt.datetime.fromtimestamp(window_end_ts, tz=dt.timezone.utc)
        
        params = {
            "symbol": "BTC",
            "eventStartTime": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "variant": "fiveminute",
            "endDate": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        
        for attempt in range(max_retries):
            try:
                r = requests.get(POLYMARKET_CRYPTO_API, params=params, timeout=10, verify=SSL_VERIFY)
                if r.status_code == 200:
                    data = r.json()
                    open_price = data.get("openPrice")
                    close_price = data.get("closePrice")
                    completed = data.get("completed", False)
                    
                    return {
                        "openPrice": float(open_price) if open_price else None,
                        "closePrice": float(close_price) if close_price else None,
                        "completed": completed,
                        "source": "polymarket_crypto_price_api"
                    }
                else:
                    if attempt < max_retries - 1:
                        time.sleep(retry_interval)
                        continue
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_interval)
                    continue
    except Exception as e:
        log(f"Platform API error: {e}")
    
    return {"openPrice": None, "closePrice": None, "completed": False, "source": "polymarket_crypto_price_api", "error": "failed"}



# ── Last-good order-book cache (prevents an empty book from overwriting)──
last_good_orderbook = {"up": None, "down": None}

# ── Global state ──
current_asset = "btc"
current_timeframe = "5m"
current_slug = ""
current_window_id = ""
current_tokens = []
current_ptb = 0
current_market_id = ""
current_condition_id = ""
window_start_ts = 0
window_end_ts = 0
ptb_pending = False  # whether the new window's PTB is pending
last_successful_switch_at = 0  # timestamp of the last successful switch
negative_seconds_seen = False  # whether negative seconds have ever been seen
last_price_tick_at = 0  # time the last price tick was received
last_orderbook_tick_at = 0  # time the last order-book tick was received
market_switch_reason = ""  # switch reason
last_rtds_message_at = 0
last_fallback_price_tick_at = 0
last_fallback_price = 0
last_fallback_price_ts = 0
last_fallback_price_source = ""
last_fallback_price_error = ""
chainlink_rpc_skip_until = 0
last_price_second_key = ""
last_orderbook_second_key = ""
last_rest_orderbook_fetch_at = 0
last_clob_subscribe_at = 0
finalized_market_ids = set()

# Price state
last_rtds_price = 0
last_rtds_ts = 0
last_up_bid = 0
last_up_ask = 0
last_down_bid = 0
last_down_ask = 0
price_lock = threading.Lock()

# Statistics
stats = {
    "windows": 0,
    "price_ticks": 0,
    "orderbook_ticks": 0,
    "price_change_ticks": 0,
    "trade_ticks": 0,
    "market_meta": 0,
    "resolutions": 0,
    "start_time": time.time(),
    "ws_connected": False,
    "rtds_connected": False,
    "rtds_degraded": False,
    "rtds_reconnects": 0,
    "rtds_stale_events": 0,
    "fallback_price_ticks": 0,
}

# Caches
orderbook_cache = {"up": {"bids": [], "asks": []}, "down": {"bids": [], "asks": []}}
clob_ws_conn = None
token_lookup = {}
orderbook_cache_by_window = {}
prefetched_markets = {}

# RTDS debugging
rtds_debug_count = 0
RTDS_DEBUG_FILE = REPO_ROOT / "runtime" / "rtds_debug.jsonl"


def log(msg):
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    try:
        print(line, flush=True)
    except UnicodeEncodeError:
        enc = getattr(sys.stdout, "encoding", None) or "utf-8"
        print(line.encode(enc, errors="replace").decode(enc, errors="replace"), flush=True)


def write_jsonl(path, entry):
    """Write to a jsonl file, automatically adding timestamp fields"""
    entry.setdefault("received_at", datetime.datetime.now(datetime.timezone.utc).isoformat())
    entry.setdefault("local_monotonic_ms", int(time.monotonic() * 1000))
    entry.setdefault("ts", int(time.time()))
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def fetch_chainlink_rpc_price():
    try:
        r = requests.post(CHAINLINK_RPC, json={
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": CHAINLINK_CONTRACT, "data": CHAINLINK_LATEST_ROUND_DATA_SIG}, "latest"],
            "id": 1,
        }, timeout=2, verify=SSL_VERIFY)
        if r.status_code != 200:
            return {"error": f"chainlink_rpc_http_{r.status_code}"}
        result = (r.json().get("result") or "").removeprefix("0x")
        if len(result) < 256:
            return {"error": "chainlink_rpc_short_result"}
        answer = int(result[64:128], 16)
        updated_at = int(result[192:256], 16)
        price = answer / 1e8
        if price <= 0:
            return {"error": "chainlink_rpc_non_positive_price"}
        return {"price": price, "updated_at": updated_at, "source": "chainlink_rpc_latest_round_data"}
    except Exception as e:
        return {"error": f"chainlink_rpc_{type(e).__name__}"}


def fetch_coinbase_spot_price():
    errors = []
    try:
        r = requests.get("https://api.exchange.coinbase.com/products/BTC-USD/ticker", timeout=2, verify=SSL_VERIFY)
        if r.status_code == 200:
            data = r.json()
            price = float(data.get("price") or 0)
            if price > 0:
                ts = int(time.time())
                try:
                    if data.get("time"):
                        ts = int(datetime.datetime.fromisoformat(str(data["time"]).replace("Z", "+00:00")).timestamp())
                except Exception:
                    pass
                return {"price": price, "updated_at": ts, "source": "coinbase_exchange_ticker"}
        else:
            errors.append(f"coinbase_exchange_http_{r.status_code}")
    except Exception as e:
        errors.append(f"coinbase_exchange_{type(e).__name__}")
    try:
        r = requests.get("https://api.coinbase.com/v2/prices/BTC-USD/spot", timeout=2, verify=SSL_VERIFY)
        if r.status_code != 200:
            errors.append(f"coinbase_v2_http_{r.status_code}")
            return {"error": ";".join(errors)}
        amount = ((r.json().get("data") or {}).get("amount"))
        price = float(amount)
        if price <= 0:
            errors.append("coinbase_v2_non_positive_price")
            return {"error": ";".join(errors)}
        now = int(time.time())
        return {"price": price, "updated_at": now, "source": "coinbase_v2_spot"}
    except Exception as e:
        errors.append(f"coinbase_v2_{type(e).__name__}")
        return {"error": ";".join(errors)}


def fetch_tail60_signal_price():
    global chainlink_rpc_skip_until
    now = time.time()
    if now < chainlink_rpc_skip_until:
        chainlink_quote = {"error": "chainlink_rpc_temporarily_skipped"}
    else:
        chainlink_quote = fetch_chainlink_rpc_price()
        if chainlink_quote.get("error") == "chainlink_rpc_http_403":
            chainlink_rpc_skip_until = now + 60
    if chainlink_quote.get("price"):
        return chainlink_quote
    coinbase_quote = fetch_coinbase_spot_price()
    if coinbase_quote.get("price"):
        coinbase_quote["primary_error"] = chainlink_quote.get("error")
        return coinbase_quote
    if last_fallback_price and last_fallback_price_ts and now - last_fallback_price_ts <= 5:
        return {
            "price": last_fallback_price,
            "updated_at": int(last_fallback_price_ts),
            "source": (last_fallback_price_source or "tail60_price_fallback") + "_cached",
            "stale_cache": True,
            "primary_error": chainlink_quote.get("error"),
            "secondary_error": coinbase_quote.get("error"),
        }
    return {
        "error": coinbase_quote.get("error") or chainlink_quote.get("error") or "tail60_price_unavailable",
        "primary_error": chainlink_quote.get("error"),
    }


def iso_from_ts(ts):
    return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).isoformat()


def compact_missing_ranges(missing_seconds, max_ranges=12):
    if not missing_seconds:
        return []
    ranges = []
    start = prev = missing_seconds[0]
    for sec in missing_seconds[1:]:
        if sec == prev + 1:
            prev = sec
            continue
        ranges.append([start, prev])
        start = prev = sec
        if len(ranges) >= max_ranges:
            break
    if len(ranges) < max_ranges:
        ranges.append([start, prev])
    elif ranges[-1] != [start, prev]:
        ranges.append(["more", len(missing_seconds)])
    return ranges


def iter_jsonl(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except Exception:
                    continue
    except FileNotFoundError:
        return


def parse_iso_ts(value):
    if not value:
        return None
    try:
        return datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def event_second(entry):
    if entry.get("rtds_timestamp_ms"):
        try:
            return int(float(entry["rtds_timestamp_ms"]) // 1000)
        except Exception:
            pass
    if entry.get("ts"):
        try:
            return int(float(entry["ts"]))
        except Exception:
            pass
    parsed = parse_iso_ts(entry.get("server_ts") or entry.get("received_at"))
    return int(parsed) if parsed else None


def compute_market_integrity(slug, window_id, start_ts, end_ts, reason="periodic"):
    expected = set(range(int(start_ts), int(end_ts)))
    tail_start_ts = max(int(start_ts), int(end_ts) - TAIL_SECONDS)
    tail_expected = set(range(tail_start_ts, int(end_ts)))
    official_price_seconds = set()
    fallback_price_seconds = set()
    orderbook_snapshot_seconds = set()
    orderbook_event_seconds = set()
    quality_bad = 0
    quality_degraded = 0
    window_records = []

    for entry in iter_jsonl(WINDOWS_FILE):
        if entry.get("market_window_id") == window_id or entry.get("slug") == slug:
            window_records.append(entry)

    for entry in iter_jsonl(PRICE_TICKS_FILE):
        if entry.get("market_window_id") != window_id:
            continue
        sec = event_second(entry)
        if sec in expected and entry.get("source") == "polymarket_rtds_chainlink":
            official_price_seconds.add(sec)

    for entry in iter_jsonl(FALLBACK_PRICE_TICKS_FILE):
        if entry.get("market_window_id") != window_id:
            continue
        sec = event_second(entry)
        if sec in expected:
            fallback_price_seconds.add(sec)

    combined_price_seconds = official_price_seconds | fallback_price_seconds

    for entry in iter_jsonl(ORDERBOOK_TICKS_FILE):
        if entry.get("market_window_id") != window_id:
            continue
        sec = event_second(entry)
        if sec not in expected:
            continue
        if "up_sim" in entry or entry.get("reason"):
            orderbook_snapshot_seconds.add(sec)
        else:
            orderbook_event_seconds.add(sec)

    for entry in iter_jsonl(DATA_QUALITY_FILE):
        if entry.get("market_window_id") != window_id:
            continue
        q = entry.get("current_window_quality")
        if q in ("bad", "stale"):
            quality_bad += 1
        elif q == "degraded":
            quality_degraded += 1

    missing_price = sorted(expected - official_price_seconds)
    missing_orderbook = sorted(expected - orderbook_snapshot_seconds)
    tail_official_price_seconds = official_price_seconds & tail_expected
    tail_fallback_price_seconds = fallback_price_seconds & tail_expected
    tail_combined_price_seconds = combined_price_seconds & tail_expected
    tail_orderbook_snapshot_seconds = orderbook_snapshot_seconds & tail_expected
    missing_tail_price = sorted(tail_expected - tail_official_price_seconds)
    missing_tail_combined_price = sorted(tail_expected - tail_combined_price_seconds)
    missing_tail_orderbook = sorted(tail_expected - tail_orderbook_snapshot_seconds)
    open_records = [r for r in window_records if r.get("source") == "polymarket_gamma"]
    validation_records = [r for r in window_records if r.get("event_type") == "ptb_validation"]
    latest_open = open_records[-1] if open_records else {}
    latest_validation = validation_records[-1] if validation_records else {}
    effective_open_price = latest_validation.get("platform_ptb") or latest_open.get("ptb")
    effective_open_source = latest_validation.get("platform_source") or latest_open.get("ptb_source")
    effective_open_quality = "platform" if latest_validation.get("platform_ptb") else latest_open.get("ptb_quality")

    platform_final = fetch_platform_price(start_ts, end_ts, max_retries=2, retry_interval=0.5)
    close_price = platform_final.get("closePrice") if platform_final else None
    completed = bool(platform_final.get("completed")) if platform_final else False

    reasons = []
    if len(official_price_seconds) < EXPECTED_5M_SECONDS:
        reasons.append("official_price_seconds_missing")
    if len(orderbook_snapshot_seconds) < EXPECTED_5M_SECONDS:
        reasons.append("orderbook_snapshot_seconds_missing")
    if not effective_open_price or latest_open.get("ptb_pending"):
        reasons.append("open_price_pending_or_missing")
    if latest_open.get("exclude_from_backtest"):
        reasons.append("open_price_validation_failed")
    if not completed or close_price is None:
        reasons.append("platform_close_price_missing")
    live_quality_reasons = []
    if quality_bad > 0:
        live_quality_reasons.append("data_quality_bad_or_stale")

    complete_for_backtest = not reasons
    practical_reasons = []
    if len(combined_price_seconds) < EXPECTED_5M_SECONDS:
        practical_reasons.append("combined_price_seconds_missing")
    if len(orderbook_snapshot_seconds) < EXPECTED_5M_SECONDS:
        practical_reasons.append("orderbook_snapshot_seconds_missing")
    if not effective_open_price or latest_open.get("ptb_pending"):
        practical_reasons.append("open_price_pending_or_missing")
    if latest_open.get("exclude_from_backtest"):
        practical_reasons.append("open_price_validation_failed")
    if not completed or close_price is None:
        practical_reasons.append("platform_close_price_missing")
    if quality_bad > 0:
        practical_reasons.append("data_quality_bad_or_stale")

    tail60_reasons = []
    if len(tail_combined_price_seconds) < TAIL_MIN_SECONDS:
        tail60_reasons.append("tail60_combined_price_seconds_missing")
    if len(tail_orderbook_snapshot_seconds) < TAIL_MIN_SECONDS:
        tail60_reasons.append("tail60_orderbook_snapshot_seconds_missing")
    if not effective_open_price or latest_open.get("ptb_pending"):
        tail60_reasons.append("open_price_pending_or_missing")
    if latest_open.get("exclude_from_backtest"):
        tail60_reasons.append("open_price_validation_failed")
    if not completed or close_price is None:
        tail60_reasons.append("platform_close_price_missing")

    summary = {
        "source": "collector_integrity",
        "event_type": "market_integrity",
        "reason": reason,
        "market_window_id": window_id,
        "slug": slug,
        "window_start_ts": int(start_ts),
        "window_end_ts": int(end_ts),
        "window_start_iso": iso_from_ts(start_ts),
        "window_end_iso": iso_from_ts(end_ts),
        "expected_seconds": EXPECTED_5M_SECONDS,
        "tail_seconds": TAIL_SECONDS,
        "tail_min_seconds": TAIL_MIN_SECONDS,
        "official_price_seconds": len(official_price_seconds),
        "fallback_price_seconds": len(fallback_price_seconds),
        "combined_price_seconds": len(combined_price_seconds),
        "tail60_official_price_seconds": len(tail_official_price_seconds),
        "tail60_fallback_price_seconds": len(tail_fallback_price_seconds),
        "tail60_combined_price_seconds": len(tail_combined_price_seconds),
        "tail60_orderbook_snapshot_seconds": len(tail_orderbook_snapshot_seconds),
        "orderbook_snapshot_seconds": len(orderbook_snapshot_seconds),
        "orderbook_event_seconds": len(orderbook_event_seconds),
        "missing_price_seconds": len(missing_price),
        "missing_orderbook_seconds": len(missing_orderbook),
        "missing_price_ranges": compact_missing_ranges(missing_price),
        "missing_orderbook_ranges": compact_missing_ranges(missing_orderbook),
        "missing_tail60_price_ranges": compact_missing_ranges(missing_tail_price),
        "missing_tail60_combined_price_ranges": compact_missing_ranges(missing_tail_combined_price),
        "missing_tail60_orderbook_ranges": compact_missing_ranges(missing_tail_orderbook),
        "open_price": effective_open_price,
        "open_price_source": effective_open_source,
        "open_price_quality": effective_open_quality,
        "open_price_validation_diff": latest_validation.get("diff"),
        "close_price": close_price,
        "platform_completed": completed,
        "quality_bad_count": quality_bad,
        "quality_degraded_count": quality_degraded,
        "live_usable": not live_quality_reasons,
        "live_quality_reasons": live_quality_reasons,
        "complete_for_backtest": complete_for_backtest,
        "complete_for_practical_backtest": not practical_reasons,
        "complete_tail60_for_practical_backtest": not tail60_reasons,
        "exclude_from_backtest": not complete_for_backtest,
        "exclude_reasons": reasons,
        "practical_exclude_reasons": practical_reasons,
        "tail60_exclude_reasons": tail60_reasons,
    }
    return summary


def finalize_market_integrity(slug, window_id, start_ts, end_ts, reason="market_closed"):
    if not slug or not window_id or not start_ts or not end_ts:
        return None
    if window_id in finalized_market_ids:
        return None
    summary = compute_market_integrity(slug, window_id, start_ts, end_ts, reason)
    write_jsonl(MARKET_INTEGRITY_FILE, summary)
    finalized_market_ids.add(window_id)
    if summary["complete_for_backtest"]:
        log(f"Complete market: {slug} price={summary['official_price_seconds']}/300 orderbook={summary['orderbook_snapshot_seconds']}/300")
    else:
        log(f"Incomplete market: {slug} reasons={','.join(summary['exclude_reasons'])}")
    return summary


def schedule_market_integrity_finalize(slug, window_id, start_ts, end_ts, reason="market_closed", delay_seconds=180):
    def worker():
        time.sleep(delay_seconds)
        finalize_market_integrity(slug, window_id, start_ts, end_ts, reason=reason)
    threading.Thread(target=worker, daemon=True).start()


def get_current_window(asset="btc", timeframe="5m"):
    """Get the current market window ID and slug"""
    now = datetime.datetime.now(datetime.timezone.utc)
    if timeframe == "5m":
        minutes = now.minute - (now.minute % 5)
        floored = now.replace(minute=minutes, second=0, microsecond=0)
        ts = int(floored.timestamp())
        slug = f"{asset}-updown-5m-{ts}"
        window_id = f"{asset}_5m_{ts}"
    elif timeframe == "15m":
        minutes = now.minute - (now.minute % 15)
        floored = now.replace(minute=minutes, second=0, microsecond=0)
        ts = int(floored.timestamp())
        slug = f"{asset}-updown-15m-{ts}"
        window_id = f"{asset}_15m_{ts}"
    else:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    return slug, window_id, ts


def get_window_for_event_ts(event_ts_s, asset="btc", timeframe="5m"):
    if timeframe == "5m":
        window_ts = int(event_ts_s) // 300 * 300
        return f"{asset}-updown-5m-{window_ts}", f"{asset}_5m_{window_ts}", window_ts
    if timeframe == "15m":
        window_ts = int(event_ts_s) // 900 * 900
        return f"{asset}-updown-15m-{window_ts}", f"{asset}_15m_{window_ts}", window_ts
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def fetch_market_meta(slug):
    """Fetch market metadata from the Gamma API"""
    def parse_market(m):
        tokens = json.loads(m.get("clobTokenIds", "[]"))
        if len(tokens) < 2:
            return None
        
        # Parse outcomes to map token_up / token_down correctly
        outcomes_raw = m.get("outcomes", "[]")
        try:
            outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
        except:
            outcomes = []
        
        # Map tokens from outcomes
        token_up = ""
        token_down = ""
        token_map_quality = "unknown"
        
        if len(outcomes) >= 2 and len(tokens) >= 2:
            # Find the positions of Up and Down in outcomes
            for i, outcome in enumerate(outcomes):
                outcome_lower = str(outcome).lower().strip()
                # Outcome strings preserved as escapes so the
                # source stays ASCII-only: \u9ad8\u4e8e = "higher than", \u4e0a\u6da8 = "rise".
                if outcome_lower in ("up", "yes", "\u9ad8\u4e8e", "\u4e0a\u6da8"):
                    token_up = tokens[i]
                # Escapes: \u4f4e\u4e8e = "lower than", \u4e0b\u8dcc = "fall".
                elif outcome_lower in ("down", "no", "\u4f4e\u4e8e", "\u4e0b\u8dcc"):
                    token_down = tokens[i]
            
            # If an explicit mapping was found
            if token_up and token_down:
                token_map_quality = "good"
            else:
                # If outcomes are not in Up/Down format, assume the order is [Up, Down]
                token_up = tokens[0]
                token_down = tokens[1]
                token_map_quality = "assumed_order"
        else:
            # No outcomes info; assume the order is [Up, Down]
            token_up = tokens[0]
            token_down = tokens[1]
            token_map_quality = "no_outcomes"
        
        # Parse outcome_prices to validate the mapping
        outcome_prices_raw = m.get("outcomePrices", "[]")
        try:
            outcome_prices = json.loads(outcome_prices_raw) if isinstance(outcome_prices_raw, str) else outcome_prices_raw
        except:
            outcome_prices = []
        
        # If outcome_prices[0] > 0.5, the first token is more likely to be Up
        # This can be used to check whether the mapping is correct
        if False and len(outcome_prices) >= 2:
            price0 = float(outcome_prices[0])
            price1 = float(outcome_prices[1])
            # If price0 > 0.5 but token_up is not tokens[0], the mapping may be reversed
            if price0 > 0.5 and token_up != tokens[0]:
                # Mapping may be reversed; swap
                token_up, token_down = tokens[0], tokens[1]
                token_map_quality = "price_verified_swap"
            elif price0 < 0.5 and token_up == tokens[0]:
                # Mapping may be reversed; swap
                token_up, token_down = tokens[1], tokens[0]
                token_map_quality = "price_verified_swap"
        if token_map_quality in ("assumed_order", "no_outcomes"):
            token_up = ""
            token_down = ""
            token_map_quality = "bad_unverified_outcomes"

        return {
            "market_id": m["id"],
            "condition_id": m.get("conditionId", ""),
            "question": m.get("question", ""),
            "outcomes": outcomes_raw,
            "token_up": token_up,
            "token_down": token_down,
            "token_map_quality": token_map_quality,
            "volume": float(m.get("volume24hr", 0) or 0),
            "liquidity": float(m.get("liquidityNum", 0) or 0),
            "outcome_prices": outcome_prices_raw,
            "best_bid": float(m.get("bestBid", 0) or 0),
            "best_ask": float(m.get("bestAsk", 0) or 0),
            "last_trade_price": float(m.get("lastTradePrice", 0) or 0),
            "order_min_size": int(m.get("orderMinSize", 5) or 5),
            "tick_size": float(m.get("orderPriceMinTickSize", 0.01) or 0.01),
            "fee_schedule": m.get("feeSchedule", {}),
            "resolution_source": m.get("resolutionSource", ""),
            "neg_risk": m.get("negRisk", False),
            "spread": float(m.get("spread", 0) or 0),
            "accepting_orders": m.get("acceptingOrders", False),
            "closed": m.get("closed", False),
            "start_date": m.get("startDate", ""),
            "end_date": m.get("endDate", ""),
            "event_start_time": m.get("eventStartTime", ""),
        }

    try:
        r = requests.get(f"{GAMMA_API}/markets", params={"slug": slug, "limit": 1}, timeout=10, verify=SSL_VERIFY)
        if r.status_code == 200 and r.json():
            parsed = parse_market(r.json()[0])
            if parsed:
                return parsed
    except Exception as e:
        log(f"Gamma API error: {e}")
    try:
        r = requests.get(f"{GAMMA_API}/events", params={"slug": slug, "limit": 1}, timeout=10, verify=SSL_VERIFY)
        if r.status_code == 200 and r.json():
            markets = (r.json()[0] or {}).get("markets") or []
            if markets:
                parsed = parse_market(markets[0])
                if parsed:
                    return parsed
    except Exception as e:
        log(f"Gamma events API error: {e}")
    return None


def select_ptb_from_rtds(window_start_ms):
    """Look up the tick closest to window_start_ms in the RTDS tick cache
    
    Prefer a tick before window_start (exact)
    If there is none, take the earliest tick within 5 minutes after window_start (estimate)
    """
    # Priority: find the most recent tick before window_start
    candidates_before = [
        t for t in rtds_tick_buffer
        if t["symbol"] == "btc/usd" and t["timestamp_ms"] <= window_start_ms
    ]
    if candidates_before:
        return max(candidates_before, key=lambda t: t["timestamp_ms"])
    
    # Fallback: find the earliest tick within 5 minutes after window_start (when the collector just started)
    candidates_after = [
        t for t in rtds_tick_buffer
        if t["symbol"] == "btc/usd" 
        and window_start_ms < t["timestamp_ms"] <= window_start_ms + 300000
    ]
    if candidates_after:
        return min(candidates_after, key=lambda t: t["timestamp_ms"])
    
    return None


def fetch_platform_ptb(slug):
    """Fetch the Price to Beat from the Polymarket platform page (for validation)"""
    try:
        r = requests.get(
            f"https://polymarket.com/event/{slug}",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=15,
            verify=SSL_VERIFY
        )
        if r.status_code != 200:
            return None

        m = re.search(r'__NEXT_DATA__.*?>(.*?)</script>', r.text, re.DOTALL)
        if not m:
            return None

        data = json.loads(m.group(1))
        queries = data.get("props", {}).get("pageProps", {}).get("dehydratedState", {}).get("queries", [])

        for q in queries:
            qk = q.get("queryKey", [])
            qd = q.get("state", {}).get("data", {})
            if not qd:
                continue

            # Get openPrice from crypto-prices (this is the platform's Price to Beat)
            if isinstance(qk, list) and "crypto-prices" in str(qk):
                if isinstance(qd, dict):
                    open_price = float(qd.get("openPrice") or 0)
                    if open_price > 0:
                        log(f"Platform PTB: ${open_price:,.2f}")
                        return open_price

    except Exception as e:
        log(f"Platform PTB error: {e}")
    return None


def fetch_clob_book(token_id):
    """Fetch the full order book from CLOB REST"""
    try:
        r = requests.get(f"{CLOB_API}/book", params={"token_id": token_id}, timeout=5, verify=SSL_VERIFY)
        if r.status_code == 200:
            data = r.json()
            bids = []
            asks = []
            for row in data.get("bids", []) or []:
                try:
                    bids.append({"price": float(row["price"]), "size": float(row["size"])})
                except:
                    pass
            for row in data.get("asks", []) or []:
                try:
                    asks.append({"price": float(row["price"]), "size": float(row["size"])})
                except:
                    pass
            return {
                "bids": sorted(bids, key=lambda x: -x["price"]),
                "asks": sorted(asks, key=lambda x: x["price"]),
                "timestamp": data.get("timestamp", ""),
                "hash": data.get("hash", ""),
            }
    except Exception as e:
        log(f"CLOB book error: {e}")
    return None


def normalize_book_rows(rows, reverse=False):
    normalized = []
    for row in rows or []:
        try:
            normalized.append({"price": float(row["price"]), "size": float(row["size"])})
        except Exception:
            continue
    return sorted(normalized, key=lambda x: x["price"], reverse=reverse)


def register_market_tokens(slug, window_id, token_up, token_down):
    if not token_up or not token_down:
        return
    orderbook_cache_by_window.setdefault(window_id, {
        "up": {"bids": [], "asks": []},
        "down": {"bids": [], "asks": []},
    })
    token_lookup[token_up] = {"slug": slug, "market_window_id": window_id, "side": "up"}
    token_lookup[token_down] = {"slug": slug, "market_window_id": window_id, "side": "down"}


def lookup_token(aid):
    meta = token_lookup.get(aid)
    if meta:
        return meta["side"], meta["market_window_id"], meta["slug"]
    side = "up" if aid == current_tokens[0] else "down" if len(current_tokens) > 1 and aid == current_tokens[1] else "unknown"
    return side, current_window_id, current_slug


def simulate_market_buy(orderbook_asks, amount_usd=1.0):
    """
    Simulate a $1 market buy and compute slippage
    Returns: avg_fill_price, shares, available_liquidity, fill_quality
    """
    if not orderbook_asks:
        return {
            "simulated_avg_fill_price": 0,
            "simulated_shares": 0,
            "available_liquidity_at_entry": 0,
            "fill_quality": "none",
        }

    remaining_usd = amount_usd
    total_shares = 0
    total_cost = 0
    available_liquidity = 0

    for ask in orderbook_asks:
        price = ask["price"]
        size = ask["size"]
        available_liquidity += size

        if price <= 0 or price >= 1:
            continue

        # How much can be bought at this price level
        max_shares_at_price = remaining_usd / price
        shares_to_buy = min(max_shares_at_price, size)

        if shares_to_buy <= 0:
            continue

        cost = shares_to_buy * price
        total_shares += shares_to_buy
        total_cost += cost
        remaining_usd -= cost

        if remaining_usd <= 0.001:  # precision threshold
            break

    if total_shares > 0:
        avg_fill_price = total_cost / total_shares
        best_ask_price = orderbook_asks[0]["price"] if orderbook_asks else 0
        slippage = avg_fill_price - best_ask_price if best_ask_price > 0 else 0

        if remaining_usd <= 0.001:
            fill_quality = "full"
        elif total_cost > 0.5:  # at least half was filled
            fill_quality = "partial"
        else:
            fill_quality = "none"

        return {
            "simulated_avg_fill_price": round(avg_fill_price, 6),
            "simulated_shares": round(total_shares, 6),
            "available_liquidity_at_entry": round(available_liquidity, 2),
            "fill_quality": fill_quality,
            "slippage_vs_best_ask": round(slippage, 6),
        }
    else:
        return {
            "simulated_avg_fill_price": 0,
            "simulated_shares": 0,
            "available_liquidity_at_entry": round(available_liquidity, 2),
            "fill_quality": "none",
        }


def save_market_meta_snapshot(slug, meta, reason="periodic"):
    """Save a market metadata snapshot"""
    entry = {
        "source": "polymarket_gamma",
        "server_ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "market_window_id": current_window_id,
        "slug": slug,
        "reason": reason,
        **meta,
    }
    write_jsonl(MARKET_META_FILE, entry)
    stats["market_meta"] += 1


def switch_market(slug, window_id, window_ts):
    """Switch to a new market -- driven by wall-clock time; PTB only affects the quality flag"""
    global current_slug, current_window_id, current_tokens, current_ptb
    global current_market_id, current_condition_id, window_start_ts, window_end_ts
    global last_up_bid, last_up_ask, last_down_bid, last_down_ask
    global last_good_orderbook, orderbook_cache
    global ptb_pending, last_successful_switch_at, market_switch_reason
    global last_orderbook_second_key, last_rest_orderbook_fetch_at

    if slug == current_slug and current_ptb > 0 and not ptb_pending:
        return True

    now = time.time()
    window_start_ms = int(window_ts * 1000)
    window_end = window_ts + (300 if current_timeframe == "5m" else 900)

    if current_slug and slug != current_slug:
        schedule_market_integrity_finalize(
            current_slug,
            current_window_id,
            window_start_ts,
            window_end_ts,
            reason="market_switch",
        )

    # ── Try to fetch Gamma metadata ──
    meta = prefetched_markets.pop(window_id, None) or fetch_market_meta(slug)
    if not meta:
        # Create a minimal window state when Gamma is unavailable
        log(f"Gamma unavailable; creating a temporary window: {slug}")
        meta = {
            "market_id": "",
            "condition_id": "",
            "token_up": "",
            "token_down": "",
            "question": slug,
            "volume": 0,
            "liquidity": 0,
        }

    # ── Try to fetch PTB (platform API first)──
    ptb = 0
    ptb_quality = "pending"
    ptb_source = "none"
    ptb_timestamp_ms = 0
    ptb_lag_ms = 0
    platform_open = None

    # Prefer openPrice from the platform API
    platform_price = fetch_platform_price(window_ts, window_end, max_retries=3, retry_interval=1.0)
    if platform_price and platform_price.get("openPrice"):
        platform_open = platform_price["openPrice"]
        ptb = platform_open
        ptb_source = "polymarket_crypto_price_api"
        ptb_quality = "platform"
        ptb_timestamp_ms = int(window_ts * 1000)
        ptb_lag_ms = 0
        log(f"Platform PTB: ${ptb:,.2f}")
    
    # If the platform API fails, use the RTDS cache
    if ptb <= 0:
        ptb_tick = select_ptb_from_rtds(window_start_ms)
        if ptb_tick:
            ptb = ptb_tick["value"]
            ptb_timestamp_ms = ptb_tick["timestamp_ms"]
            ptb_lag_ms = ptb_timestamp_ms - window_start_ms
            abs_lag = abs(ptb_lag_ms)
            if abs_lag <= 1000:
                ptb_quality = "exact"
            elif abs_lag <= 3000:
                ptb_quality = "close"
            elif abs_lag <= 30000:
                ptb_quality = "estimated"
            elif abs_lag <= 300000:
                ptb_quality = "estimated"
            else:
                ptb_quality = "bad"
            ptb_source = "polymarket_rtds_chainlink"
            log(f"RTDS PTB: ${ptb:,.2f} quality={ptb_quality} lag={ptb_lag_ms}ms")

    # ── Force-update global state (wall-clock driven)──
    old_slug = current_slug
    current_slug = slug
    current_window_id = window_id
    current_tokens = [meta.get("token_up", ""), meta.get("token_down", "")]
    current_ptb = ptb
    current_market_id = meta.get("market_id", "")
    current_condition_id = meta.get("condition_id", "")
    window_start_ts = window_ts
    window_end_ts = window_end
    ptb_pending = (ptb == 0)
    last_successful_switch_at = now
    market_switch_reason = "wall_clock_forced" if ptb == 0 else "normal"

    with price_lock:
        last_up_bid = last_up_ask = last_down_bid = last_down_ask = 0
    last_good_orderbook = {"up": None, "down": None}
    orderbook_cache = {"up": {"bids": [], "asks": []}, "down": {"bids": [], "asks": []}}
    if current_tokens[0] and current_tokens[1]:
        register_market_tokens(slug, window_id, current_tokens[0], current_tokens[1])
        cached = orderbook_cache_by_window.get(window_id)
        if cached:
            orderbook_cache = cached
    last_orderbook_second_key = ""
    last_rest_orderbook_fetch_at = 0

    # Keep the last_good_orderbook cache until new data refreshes it
    # Avoid losing all order-book data when Gamma fails
    # The new market has different tokens, but a degraded data-quality flag is enough; "no order book" is a bigger problem than "slightly stale order-book data"

    # Save window metadata
    window_entry = {
        "source": "polymarket_gamma",
        "server_ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "market_window_id": window_id,
        "slug": slug,
        "asset": current_asset,
        "timeframe": current_timeframe,
        "window_start_ts": window_start_ts,
        "window_end_ts": window_end_ts,
        "window_start_ms": window_start_ms,
        "ptb": ptb,
        "ptb_source": ptb_source,
        "ptb_timestamp_ms": ptb_timestamp_ms,
        "ptb_lag_ms": ptb_lag_ms,
        "ptb_quality": ptb_quality,
        "ptb_pending": ptb_pending,
        "platform_ptb": None,
        "ptb_mismatch": False,
        "exclude_from_backtest": ptb_quality == "bad",
        "switch_reason": market_switch_reason,
        **meta,
    }
    write_jsonl(WINDOWS_FILE, window_entry)
    stats["windows"] += 1

    if meta.get("token_up"):
        subscribe_clob_current(reason="market_switch")
        save_market_meta_snapshot(slug, meta, reason="market_open")
        save_orderbook_snapshot(slug, reason="rest_refresh")
        threading.Thread(target=warmup_current_orderbook, args=(slug, window_id), daemon=True).start()

    log(f"New market: {slug} | PTB=${ptb:,.2f} | quality={ptb_quality} | reason={market_switch_reason}")

    # Retry filling PTB in the background while PTB is pending
    if ptb_pending:
        threading.Thread(target=retry_ptb_fill, args=(slug, window_id, window_start_ms), daemon=True).start()
    else:
        threading.Thread(target=validate_ptb_async, args=(slug, window_id, ptb), daemon=True).start()

    return True


def warmup_current_orderbook(slug, window_id):
    """Right after switching to a new market, actively subscribe and pull the REST order book to reduce the gap at the start."""
    for _ in range(15):
        if current_window_id != window_id or current_slug != slug:
            return
        try:
            subscribe_clob_current(reason="market_warmup")
            save_orderbook_snapshot(slug, reason="rest_refresh")
        except Exception as e:
            log(f"Order-book warmup error: {e}")
        time.sleep(2)


def retry_ptb_fill(slug, window_id, window_start_ms):
    """Retry fetching PTB in the background (up to 60 seconds)"""
    global current_ptb, ptb_pending
    
    for attempt in range(60):
        time.sleep(1)
        # If already switched to another window, stop retrying
        if current_window_id != window_id:
            return
        
        ptb_tick = select_ptb_from_rtds(window_start_ms)
        if ptb_tick:
            ptb = ptb_tick["value"]
            ptb_timestamp_ms = ptb_tick["timestamp_ms"]
            ptb_lag_ms = ptb_timestamp_ms - window_start_ms
            abs_lag = abs(ptb_lag_ms)
            if abs_lag <= 1000:
                ptb_quality = "exact"
            elif abs_lag <= 3000:
                ptb_quality = "close"
            elif abs_lag <= 300000:
                ptb_quality = "estimated"
            else:
                ptb_quality = "bad"

            with price_lock:
                current_ptb = ptb
            ptb_pending = False
            
            # Append a PTB fill record
            write_jsonl(WINDOWS_FILE, {
                "source": "ptb_retry_fill",
                "server_ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "market_window_id": window_id,
                "slug": slug,
                "ptb": ptb,
                "ptb_quality": ptb_quality,
                "ptb_lag_ms": ptb_lag_ms,
                "ptb_timestamp_ms": ptb_timestamp_ms,
                "attempt": attempt + 1,
            })
            log(f"PTB retry succeeded: {slug} PTB=${ptb:,.2f} quality={ptb_quality} lag={ptb_lag_ms}ms")
            
            # Asynchronously validate the platform PTB
            threading.Thread(target=validate_ptb_async, args=(slug, window_id, ptb), daemon=True).start()
            return
    
    log(f"PTB retry timed out: {slug} (60s)")


def validate_ptb_async(slug, window_id, our_ptb):
    """Asynchronously validate the platform PTB in the background"""
    global current_ptb
    try:
        window_ts = int(slug.rsplit("-", 1)[-1])
        window_end = window_ts + (300 if current_timeframe == "5m" else 900)

        # When a new market just opens, the page and API may lag, so keep fetching the official openPrice in the background.
        platform_ptb = None
        platform_source = "polymarket_crypto_price_api"
        for attempt in range(30):
            time.sleep(2)
            platform_price = fetch_platform_price(window_ts, window_end, max_retries=1, retry_interval=0.2)
            if platform_price and platform_price.get("openPrice"):
                platform_ptb = platform_price["openPrice"]
                break

        if platform_ptb is None:
            platform_source = "polymarket_event_page"
            platform_ptb = fetch_platform_ptb(slug)

        if platform_ptb is None:
            log(f"Failed to fetch the platform PTB: {slug}")
            write_jsonl(WINDOWS_FILE, {
                "source": "platform_validation",
                "server_ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "market_window_id": window_id,
                "slug": slug,
                "event_type": "ptb_validation",
                "our_ptb": our_ptb,
                "platform_ptb": None,
                "platform_source": platform_source,
                "ptb_mismatch": True,
                "exclude_from_backtest": True,
                "validation_error": "platform_ptb_unavailable",
            })
            return

        diff = abs(our_ptb - platform_ptb)
        mismatch = diff > 1.0
        if current_window_id == window_id:
            with price_lock:
                current_ptb = platform_ptb

        if mismatch:
            log(f"⚠️ PTB mismatch! {slug} ours=${our_ptb:,.2f} platform=${platform_ptb:,.2f} diff=${diff:,.2f}")
        else:
            log(f"✅ PTB validation passed: {slug} diff=${diff:,.2f}")

        # Update the record in windows.jsonl (append an update record)
        update_entry = {
            "source": "platform_validation",
            "server_ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "market_window_id": window_id,
            "slug": slug,
            "event_type": "ptb_validation",
            "our_ptb": our_ptb,
            "platform_ptb": platform_ptb,
            "platform_source": platform_source,
            "diff": round(diff, 2),
            "ptb_mismatch": mismatch,
            "initial_ptb_mismatch": mismatch,
            "exclude_from_backtest": False,
        }
        write_jsonl(WINDOWS_FILE, update_entry)

    except Exception as e:
        log(f"PTB validation error: {e}")


def save_orderbook_snapshot(slug, reason="periodic"):
    """Save the current order-book snapshot to orderbook_ticks.jsonl"""
    global last_good_orderbook, last_orderbook_tick_at, last_rest_orderbook_fetch_at
    
    if len(current_tokens) < 2:
        return

    now = time.time()
    should_fetch_rest = reason == "rest_refresh"

    up_book = None
    down_book = None
    snapshot_source = "clob_ws_cache"
    if should_fetch_rest:
        up_book = fetch_clob_book(current_tokens[0])
        down_book = fetch_clob_book(current_tokens[1])
        last_rest_orderbook_fetch_at = now
        snapshot_source = "clob_rest_book"

    window_cache = orderbook_cache_by_window.get(current_window_id) or orderbook_cache
    if not up_book:
        up_book = {
            "bids": normalize_book_rows(window_cache["up"].get("bids"), reverse=True),
            "asks": normalize_book_rows(window_cache["up"].get("asks")),
        }
    if not down_book:
        down_book = {
            "bids": normalize_book_rows(window_cache["down"].get("bids"), reverse=True),
            "asks": normalize_book_rows(window_cache["down"].get("asks")),
        }

    # Prefer the REST book top-of-book price
    up_bid1 = up_book["bids"][0]["price"] if up_book and up_book["bids"] else 0
    up_ask1 = up_book["asks"][0]["price"] if up_book and up_book["asks"] else 0
    down_bid1 = down_book["bids"][0]["price"] if down_book and down_book["bids"] else 0
    down_ask1 = down_book["asks"][0]["price"] if down_book and down_book["asks"] else 0

    # If REST is empty, use the WebSocket cache
    with price_lock:
        if up_bid1 == 0:
            up_bid1 = last_up_bid
        if up_ask1 == 0:
            up_ask1 = last_up_ask
        if down_bid1 == 0:
            down_bid1 = last_down_bid
        if down_ask1 == 0:
            down_ask1 = last_down_ask

    # If still all 0, use the last good order book
    up_empty = (up_bid1 == 0 and up_ask1 == 0)
    down_empty = (down_bid1 == 0 and down_ask1 == 0)
    
    if False and up_empty and last_good_orderbook["up"]:
        cached = last_good_orderbook["up"]
        up_bid1 = cached.get("bid1_price", 0)
        up_ask1 = cached.get("ask1_price", 0)
    
    if False and down_empty and last_good_orderbook["down"]:
        cached = last_good_orderbook["down"]
        down_bid1 = cached.get("bid1_price", 0)
        down_ask1 = cached.get("ask1_price", 0)

    # If everything is 0, do not write (prevents an empty book from overwriting)
    if up_bid1 == 0 and up_ask1 == 0 and down_bid1 == 0 and down_ask1 == 0:
        return

    # Compute the spread
    up_spread = round(up_ask1 - up_bid1, 4) if up_ask1 > 0 and up_bid1 > 0 else 0
    down_spread = round(down_ask1 - down_bid1, 4) if down_ask1 > 0 and down_bid1 > 0 else 0

    # Simulate $1 market-buy slippage
    up_sim = simulate_market_buy(up_book["asks"] if up_book else [], 1.0)
    down_sim = simulate_market_buy(down_book["asks"] if down_book else [], 1.0)
    remaining_seconds = int(window_end_ts - now) if window_end_ts > 0 else None

    entry = {
        "source": "polymarket_clob_ws",
        "snapshot_source": snapshot_source,
        "server_ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "market_window_id": current_window_id,
        "slug": slug,
        "reason": reason,
        "remaining_seconds": remaining_seconds,
        "tail60": bool(remaining_seconds is not None and 0 < remaining_seconds <= TAIL_SECONDS),
        "up": {
            "bid1_price": up_bid1,
            "bid1_size": up_book["bids"][0]["size"] if up_book and up_book["bids"] else 0,
            "ask1_price": up_ask1,
            "ask1_size": up_book["asks"][0]["size"] if up_book and up_book["asks"] else 0,
            "spread": up_spread,
            "depth_bids": len(up_book["bids"]) if up_book else 0,
            "depth_asks": len(up_book["asks"]) if up_book else 0,
            "bids": up_book["bids"][:5] if up_book else [],
            "asks": up_book["asks"][:5] if up_book else [],
        },
        "down": {
            "bid1_price": down_bid1,
            "bid1_size": down_book["bids"][0]["size"] if down_book and down_book["bids"] else 0,
            "ask1_price": down_ask1,
            "ask1_size": down_book["asks"][0]["size"] if down_book and down_book["asks"] else 0,
            "spread": down_spread,
            "depth_bids": len(down_book["bids"]) if down_book else 0,
            "depth_asks": len(down_book["asks"]) if down_book else 0,
            "bids": down_book["bids"][:5] if down_book else [],
            "asks": down_book["asks"][:5] if down_book else [],
        },
        "up_sim": up_sim,
        "down_sim": down_sim,
    }
    write_jsonl(ORDERBOOK_TICKS_FILE, entry)
    stats["orderbook_ticks"] += 1
    last_orderbook_tick_at = time.time()
    
    # Update the last-good order-book cache
    if up_bid1 > 0 or up_ask1 > 0:
        last_good_orderbook["up"] = entry["up"]
    if down_bid1 > 0 or down_ask1 > 0:
        last_good_orderbook["down"] = entry["down"]


# ── CLOB WebSocket ──
def on_clob_message(ws, message):
    """Handle CLOB WebSocket messages"""
    global last_up_bid, last_up_ask, last_down_bid, last_down_ask, last_orderbook_tick_at
    
    if not message or message.isspace() or message == "pong":
        return

    try:
        data = json.loads(message)
        events = data if isinstance(data, list) else [data]
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

        for event in events:
            # price_change event -> write to price_change_ticks.jsonl (not a trade)
            if "price_changes" in event:
                for pc in event["price_changes"]:
                    aid = pc.get("asset_id") or pc.get("asset") or event.get("asset_id") or ""
                    side, event_window_id, event_slug = lookup_token(aid)
                    price = pc.get("price")
                    bid = pc.get("best_bid")
                    ask = pc.get("best_ask")

                    if event_window_id == current_window_id:
                        with price_lock:
                            if side == "up" and bid and ask:
                                last_up_bid = float(bid)
                                last_up_ask = float(ask)
                            elif side == "down" and bid and ask:
                                last_down_bid = float(bid)
                                last_down_ask = float(ask)

                    entry = {
                        "source": "polymarket_clob_ws",
                        "server_ts": pc.get("timestamp", now_iso),
                        "market_window_id": event_window_id,
                        "slug": event_slug,
                        "token_id": aid,
                        "side": side,
                        "event_type": "price_change",
                        "price": price,
                        "best_bid": bid,
                        "best_ask": ask,
                        "size": pc.get("size"),
                        "trade_side": pc.get("side"),
                    }
                    write_jsonl(PRICE_CHANGE_TICKS_FILE, entry)
                    stats["price_change_ticks"] += 1

            # orderbook event -> write to orderbook_ticks.jsonl
            if "bids" in event or "asks" in event:
                aid = event.get("asset_id", "")
                side, event_window_id, event_slug = lookup_token(aid)
                bids = event.get("bids", [])
                asks = event.get("asks", [])

                if side in ("up", "down"):
                    cache = orderbook_cache_by_window.setdefault(event_window_id, {
                        "up": {"bids": [], "asks": []},
                        "down": {"bids": [], "asks": []},
                    })
                    cache[side]["bids"] = bids
                    cache[side]["asks"] = asks
                    if event_window_id == current_window_id:
                        orderbook_cache[side]["bids"] = bids
                        orderbook_cache[side]["asks"] = asks

                entry = {
                    "source": "polymarket_clob_ws",
                    "server_ts": now_iso,
                    "market_window_id": event_window_id,
                    "slug": event_slug,
                    "token_id": aid,
                    "side": side,
                    "event_type": "orderbook_update",
                    "bids": bids[:5],
                    "asks": asks[:5],
                }
                write_jsonl(ORDERBOOK_TICKS_FILE, entry)
                stats["orderbook_ticks"] += 1
                if event_window_id == current_window_id:
                    last_orderbook_tick_at = time.time()

            # last_trade_price event -> write to trade_ticks.jsonl (a real trade)
            if event.get("event_type") == "last_trade_price":
                aid = event.get("asset_id", "")
                side, event_window_id, event_slug = lookup_token(aid)
                entry = {
                    "source": "polymarket_clob_ws",
                    "server_ts": now_iso,
                    "market_window_id": event_window_id,
                    "slug": event_slug,
                    "token_id": aid,
                    "side": side,
                    "event_type": "last_trade_price",
                    "price": event.get("price"),
                    "size": event.get("size"),
                    "trade_side": event.get("side"),
                }
                write_jsonl(TRADE_TICKS_FILE, entry)
                stats["trade_ticks"] += 1

            # market_resolved event -> keep only the current BTC market's resolution
            if event.get("event_type") == "market_resolved" or "resolution" in str(event).lower():
                # Check whether it relates to the current market
                event_condition_id = event.get("condition_id") or event.get("market", "")
                event_tokens = event.get("clob_token_ids") or event.get("assets_ids") or []

                is_current_market = (
                    event_condition_id == current_condition_id
                    or any(t in current_tokens for t in event_tokens)
                    or (event.get("asset_id") or "") in current_tokens
                )

                entry = {
                    "source": "polymarket_clob_ws",
                    "server_ts": now_iso,
                    "market_window_id": current_window_id,
                    "slug": current_slug,
                    "event_type": "market_resolved",
                    "condition_id": event_condition_id,
                    "is_current_market": is_current_market,
                    "raw": event,
                }

                # Write to the debug file (all resolution events)
                write_jsonl(RESOLUTIONS_DEBUG_FILE, entry)

                # Only the current market's resolution is written to the main file
                if is_current_market:
                    write_jsonl(RESOLUTIONS_FILE, entry)
                    stats["resolutions"] += 1
                    log(f"Current market resolved: {current_slug}")

    except Exception as e:
        log(f"CLOB WS message handling error: {e}")


def subscribe_clob_current(reason="subscribe"):
    global last_clob_subscribe_at
    if not clob_ws_conn or len(current_tokens) < 2 or not current_tokens[0] or not current_tokens[1]:
        return False
    try:
        clob_ws_conn.send(json.dumps({
            "assets_ids": current_tokens,
            "type": "market",
            "operation": "subscribe",
            "custom_feature_enabled": True,
        }))
        last_clob_subscribe_at = time.time()
        log(f"CLOB subscribed to the current market: {current_slug} reason={reason}")
        return True
    except Exception as e:
        log(f"CLOB subscription failed: {e}")
        return False


def subscribe_clob_tokens(tokens, slug, reason="prefetch"):
    global last_clob_subscribe_at
    if not clob_ws_conn or len(tokens) < 2 or not tokens[0] or not tokens[1]:
        return False
    try:
        clob_ws_conn.send(json.dumps({
            "assets_ids": tokens,
            "type": "market",
            "operation": "subscribe",
            "custom_feature_enabled": True,
        }))
        last_clob_subscribe_at = time.time()
        log(f"CLOB subscribed to market: {slug} reason={reason}")
        return True
    except Exception as e:
        log(f"CLOB subscription failed {slug}: {e}")
        return False


def on_clob_open(ws):
    global clob_ws_conn
    clob_ws_conn = ws
    stats["ws_connected"] = True
    log("CLOB WebSocket connected")
    subscribe_clob_current(reason="ws_open")
    for window_id, meta in list(prefetched_markets.items()):
        subscribe_clob_tokens([meta.get("token_up"), meta.get("token_down")], meta.get("question") or f"prefetched-{window_id}", reason="ws_open_prefetch")


def on_clob_error(ws, error):
    stats["ws_connected"] = False
    log(f"CLOB WebSocket error: {error}")


def on_clob_close(ws, close_status_code, close_msg):
    global clob_ws_conn
    clob_ws_conn = None
    stats["ws_connected"] = False
    log(f"CLOB WebSocket closed: {close_status_code} {close_msg}")


def clob_ws_loop():
    """CLOB WebSocket main loop"""
    while True:
        try:
            ws = websocket.WebSocketApp(
                CLOB_WS_URL,
                on_message=on_clob_message,
                on_open=on_clob_open,
                on_error=on_clob_error,
                on_close=on_clob_close,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            log(f"CLOB WS exception: {e}")
        time.sleep(5)


# ── Polymarket RTDS Chainlink WebSocket ──
def on_rtds_message(ws, message):
    """Handle the RTDS Chainlink price stream"""
    global last_rtds_price, last_rtds_ts, rtds_debug_count, last_price_tick_at, last_rtds_message_at

    if not message or message.isspace():
        return
    last_rtds_message_at = time.time()

    # Debug: save the first 20 raw messages
    if rtds_debug_count < 20:
        try:
            write_jsonl(RTDS_DEBUG_FILE, {
                "source": "rtds_raw",
                "server_ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "raw_message": message[:1000],
                "debug_index": rtds_debug_count,
            })
        except:
            pass
        rtds_debug_count += 1

    try:
        data = json.loads(message)

        # Handle the Chainlink topic format
        if isinstance(data, dict):
            topic = data.get("topic", "")
            msg_type = data.get("type", "")
            payload = data.get("payload", {})
            payload_ts = data.get("timestamp", 0)

            # Determine the message type: snapshot vs update
            rtds_message_kind = "snapshot" if msg_type == "snapshot" else "update"

            # Handle the payload.data array format (RTDS returns a historical data snapshot)
            if isinstance(payload, dict) and "data" in payload:
                for item in payload["data"]:
                    value = item.get("value")
                    ts = item.get("timestamp")

                    if value and ts:
                        value = float(value)
                        ts_ms = float(ts)
                        ts_s = ts_ms / 1000 if ts_ms > 1e12 else ts_ms

                        with price_lock:
                            last_rtds_price = value
                            last_rtds_ts = ts_s
                            stats["rtds_degraded"] = False

                        # Write to the RTDS tick ring buffer
                        if "btc" in "btc/usd":  # Currently only btc/usd is tracked
                            rtds_tick_buffer.append({
                                "symbol": "btc/usd",
                                "timestamp_ms": int(ts_ms),
                                "value": value,
                                "source": "polymarket_rtds_chainlink",
                                "message_type": rtds_message_kind,
                            })

                        received_at = datetime.datetime.now(datetime.timezone.utc)
                        received_at_ms = int(received_at.timestamp() * 1000)
                        received_lag_ms = received_at_ms - ts_ms if ts_ms > 0 else 0
                        event_slug, event_window_id, event_window_ts = get_window_for_event_ts(ts_s, current_asset, current_timeframe)

                        entry = {
                            "source": "polymarket_rtds_chainlink",
                            "server_ts": datetime.datetime.fromtimestamp(ts_s, tz=datetime.timezone.utc).isoformat(),
                            "received_at": received_at.isoformat(),
                            "received_lag_ms": received_lag_ms,
                            "market_window_id": event_window_id,
                            "slug": event_slug,
                            "event_window_start_ts": event_window_ts,
                            "symbol": "btc/usd",
                            "value": value,
                            "rtds_timestamp_ms": ts_ms,
                            "rtds_message_kind": rtds_message_kind,
                            "topic": topic,
                            "type": msg_type,
                            "ts": int(ts_ms / 1000),
                        }
                        write_jsonl(PRICE_TICKS_FILE, entry)
                        stats["price_ticks"] += 1
                    last_price_tick_at = time.time()

            # Handle the single-payload format
            elif topic == "crypto_prices_chainlink" and payload:
                value = payload.get("value")
                ts = payload.get("timestamp")
                symbol = payload.get("symbol", "").lower()

                if value and symbol:
                    value = float(value)
                    ts_ms = float(ts) if ts else 0
                    ts_s = ts_ms / 1000 if ts_ms > 1e12 else ts_ms

                    with price_lock:
                        last_rtds_price = value
                        last_rtds_ts = ts_s
                        stats["rtds_degraded"] = False

                    # Write to the RTDS tick ring buffer
                    if "btc" in symbol:
                        rtds_tick_buffer.append({
                            "symbol": symbol,
                            "timestamp_ms": int(ts_ms),
                            "value": value,
                            "source": "polymarket_rtds_chainlink",
                            "message_type": rtds_message_kind,
                        })

                    received_at = datetime.datetime.now(datetime.timezone.utc)
                    received_at_ms = int(received_at.timestamp() * 1000)
                    received_lag_ms = received_at_ms - ts_ms if ts_ms > 0 else 0
                    event_slug, event_window_id, event_window_ts = get_window_for_event_ts(ts_s, current_asset, current_timeframe)

                    entry = {
                        "source": "polymarket_rtds_chainlink",
                        "server_ts": datetime.datetime.fromtimestamp(ts_s, tz=datetime.timezone.utc).isoformat(),
                        "received_at": received_at.isoformat(),
                        "received_lag_ms": received_lag_ms,
                        "market_window_id": event_window_id,
                        "slug": event_slug,
                        "event_window_start_ts": event_window_ts,
                        "symbol": symbol,
                        "value": value,
                        "rtds_timestamp_ms": ts_ms,
                        "rtds_message_kind": rtds_message_kind,
                        "topic": topic,
                        "type": msg_type,
                        "ts": int(ts_ms / 1000),
                    }
                    write_jsonl(PRICE_TICKS_FILE, entry)
                    stats["price_ticks"] += 1
                    last_price_tick_at = time.time()

            # Handle the Binance topic format (backup)
            elif topic == "crypto_prices" and payload:
                value = payload.get("p") or payload.get("price")
                symbol = payload.get("s", "").lower()

                if value and symbol:
                    entry = {
                        "source": "binance_fallback",
                        "server_ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                        "market_window_id": current_window_id,
                        "slug": current_slug,
                        "symbol": symbol,
                        "value": float(value),
                        "topic": topic,
                        "type": msg_type,
                        "strict_usable": False,
                    }
                    write_jsonl(FALLBACK_PRICE_TICKS_FILE, entry)
                    stats["fallback_price_ticks"] += 1

        # May be in array format
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    value = item.get("value") or item.get("price")
                    if value:
                        event_ts = int(time.time())
                        event_slug, event_window_id, event_window_ts = get_window_for_event_ts(event_ts, current_asset, current_timeframe)
                        entry = {
                            "source": "polymarket_rtds_chainlink",
                            "server_ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                            "market_window_id": event_window_id,
                            "slug": event_slug,
                            "event_window_start_ts": event_window_ts,
                            "symbol": item.get("symbol", "unknown"),
                            "value": float(value),
                        }
                        write_jsonl(PRICE_TICKS_FILE, entry)
                        stats["price_ticks"] += 1
                    last_price_tick_at = time.time()

    except Exception as e:
        log(f"RTDS message handling error: {e}")


def on_rtds_open(ws):
    global last_rtds_message_at
    stats["rtds_connected"] = True
    stats["rtds_reconnects"] += 1
    last_rtds_message_at = time.time()
    log("RTDS Chainlink WebSocket connected")

    # Correct subscription format: filters is a string, not an object
    subscribe_msg = json.dumps({
        "action": "subscribe",
        "subscriptions": [
            {
                "topic": "crypto_prices_chainlink",
                "type": "update",
                "filters": json.dumps({"symbol": "btc/usd"})
            }
        ]
    })
    ws.send(subscribe_msg)
    log(f"Subscribed to Chainlink: {subscribe_msg[:100]}")

    # If there is no data after 10 seconds, subscribe to the Binance verification channel
    def check_and_subscribe_binance():
        time.sleep(10)
        if stats["price_ticks"] == 0:
            log("Chainlink had no data for 10 seconds; subscribing to the Binance verification channel")
            binance_msg = json.dumps({
                "action": "subscribe",
                "subscriptions": [
                    {
                        "topic": "crypto_prices",
                        "type": "update",
                        "filters": "btcusdt,ethusdt,solusdt,xrpusdt"
                    }
                ]
            })
            try:
                ws.send(binance_msg)
                stats["rtds_degraded"] = True
            except:
                pass

    threading.Thread(target=check_and_subscribe_binance, daemon=True).start()


def on_rtds_error(ws, error):
    stats["rtds_connected"] = False
    log(f"RTDS WebSocket error: {error}")


def on_rtds_close(ws, close_status_code, close_msg):
    stats["rtds_connected"] = False
    log(f"RTDS WebSocket closed: {close_status_code} {close_msg}")


def rtds_watchdog_loop(ws):
    """Close and reconnect RTDS when official Chainlink ticks stop."""
    while True:
        time.sleep(2)
        try:
            if not stats["rtds_connected"]:
                continue
            if last_price_tick_at <= 0:
                stale_for = time.time() - last_rtds_message_at if last_rtds_message_at > 0 else 0
            else:
                stale_for = time.time() - last_price_tick_at
            if stale_for > PRICE_STALE_SECONDS:
                stats["rtds_degraded"] = True
                stats["rtds_stale_events"] += 1
                log(f"RTDS official price stale for {stale_for:.1f}s, reconnecting")
                try:
                    ws.close()
                except Exception:
                    pass
                return
        except Exception as e:
            log(f"RTDS watchdog error: {e}")
            return


def rtds_ping_loop(ws):
    """Send a text PING every 5 seconds to keep alive"""
    while True:
        try:
            ws.send("PING")
            time.sleep(5)
        except:
            break


def fallback_price_loop():
    """Record the executable bot-side BTC signal during the last 60 seconds."""
    global last_fallback_price_tick_at, last_fallback_price, last_fallback_price_ts
    global last_fallback_price_source, last_fallback_price_error
    while True:
        loop_started = time.time()
        try:
            now = loop_started
            remaining = window_end_ts - now if window_end_ts > 0 else 9999
            if current_slug and 0 < remaining <= TAIL_SECONDS:
                quote = fetch_tail60_signal_price()
                if quote.get("price"):
                    observed_at = datetime.datetime.now(datetime.timezone.utc)
                    updated_at = quote.get("updated_at") or 0
                    updated_age_ms = int((observed_at.timestamp() - updated_at) * 1000) if updated_at else None
                    source = quote.get("source") or "tail60_price_fallback"
                    entry = {
                        "source": source,
                        "server_ts": observed_at.isoformat(),
                        "market_window_id": current_window_id,
                        "slug": current_slug,
                        "event_window_start_ts": window_start_ts,
                        "symbol": "btc/usd",
                        "value": quote["price"],
                        "fallback_updated_at": iso_from_ts(updated_at) if updated_at else None,
                        "fallback_updated_at_ts": updated_at or None,
                        "fallback_updated_age_ms": updated_age_ms,
                        "remaining_seconds": int(remaining),
                        "strict_usable": False,
                        "signal_usable": not bool(quote.get("stale_cache")),
                        "tail60": True,
                    }
                    if quote.get("stale_cache"):
                        entry["stale_cache"] = True
                    if source == "chainlink_rpc_latest_round_data":
                        entry["chainlink_updated_at"] = entry["fallback_updated_at"]
                        entry["chainlink_updated_at_ts"] = entry["fallback_updated_at_ts"]
                        entry["chainlink_updated_age_ms"] = entry["fallback_updated_age_ms"]
                    if quote.get("primary_error"):
                        entry["primary_error"] = quote.get("primary_error")
                    if quote.get("secondary_error"):
                        entry["secondary_error"] = quote.get("secondary_error")
                    write_jsonl(FALLBACK_PRICE_TICKS_FILE, entry)
                    last_fallback_price_tick_at = time.time()
                    last_fallback_price = quote["price"]
                    last_fallback_price_ts = updated_at or int(last_fallback_price_tick_at)
                    last_fallback_price_source = source
                    last_fallback_price_error = ""
                    stats["fallback_price_ticks"] += 1
                else:
                    last_fallback_price_error = quote.get("error") or "tail60_price_unavailable"
            time.sleep(max(0.02, 1.0 - (time.time() - loop_started)))
        except Exception as e:
            log(f"fallback price loop error: {e}")
            time.sleep(1)


def rtds_ws_loop():
    """RTDS Chainlink WebSocket main loop"""
    global rtds_debug_count
    while True:
        try:
            rtds_debug_count = 0  # Reset the debug counter
            ws = websocket.WebSocketApp(
                RTDS_WS_URL,
                on_message=on_rtds_message,
                on_open=on_rtds_open,
                on_error=on_rtds_error,
                on_close=on_rtds_close,
            )
            # Start the ping thread
            threading.Thread(target=rtds_ping_loop, args=(ws,), daemon=True).start()
            threading.Thread(target=rtds_watchdog_loop, args=(ws,), daemon=True).start()
            ws.run_forever(ping_interval=5, ping_timeout=3)
        except Exception as e:
            log(f"RTDS WS exception: {e}")
        time.sleep(5)


# ── Market check loop ──
def market_check_loop():
    """Check market state every second; wall-clock time drives switching"""
    global negative_seconds_seen, clob_ws_conn
    
    while True:
        try:
            now = time.time()
            slug, window_id, window_ts = get_current_window(current_asset, current_timeframe)
            window_end = window_ts + (300 if current_timeframe == "5m" else 900)
            
            # ── Core: wall-clock time decides whether a switch is needed ──
            need_switch = False
            
            if current_slug == "":
                # First start
                need_switch = True
                reason = "first_start"
            elif slug != current_slug:
                # The wall-clock-computed window differs from the current one
                need_switch = True
                need_switch = True
                reason = "wall_clock_advance"
            elif len(current_tokens) < 2 or not current_tokens[0] or not current_tokens[1]:
                need_switch = True
                reason = "token_retry"
            elif now >= window_end_ts and window_end_ts > 0:
                # The current window has expired
                need_switch = True
                need_switch = True
                reason = "window_expired"
            
            if need_switch:
                remaining = window_end_ts - now if window_end_ts > 0 else 0
                if remaining < 0:
                    negative_seconds_seen = True
                    log(f"Window expired {int(-remaining)}s，forcing switch -> {slug}")
                
                switch_market(slug, window_id, window_ts)

            # Save an order-book snapshot every second to support backtesting the final-moment reversal; the last 10 seconds are flagged separately.
            remaining = window_end_ts - now
            if current_slug:
                if stats["ws_connected"] and (last_orderbook_tick_at <= 0 or now - last_orderbook_tick_at > 5) and now - last_clob_subscribe_at > 10:
                    subscribe_clob_current(reason="orderbook_watchdog")
                if stats["ws_connected"] and last_orderbook_tick_at > 0 and now - last_orderbook_tick_at > 20:
                    log(f"CLOB order book {now - last_orderbook_tick_at:.1f}s not updated; forcing reconnect")
                    try:
                        if clob_ws_conn:
                            clob_ws_conn.close()
                    except Exception:
                        pass
                    stats["ws_connected"] = False
                save_orderbook_snapshot(current_slug, reason="pre_close_1s" if 0 < remaining <= 10 else "periodic_1s")
                if 0 < remaining <= 30:
                    prefetch_next_market(window_ts + (300 if current_timeframe == "5m" else 900))

        except Exception as e:
            log(f"Market check error: {e}")

        time.sleep(1)


def prefetch_next_market(next_window_ts):
    next_slug, next_window_id, _ = get_window_for_event_ts(next_window_ts, current_asset, current_timeframe)
    if next_window_id in prefetched_markets:
        return
    meta = fetch_market_meta(next_slug)
    if not meta or not meta.get("token_up") or not meta.get("token_down"):
        return
    prefetched_markets[next_window_id] = meta
    register_market_tokens(next_slug, next_window_id, meta.get("token_up"), meta.get("token_down"))
    subscribe_clob_tokens([meta.get("token_up"), meta.get("token_down")], next_slug, reason="prefetch_next")
    log(f"Prefetching the next market: {next_slug}")


# ── Gamma API resolution polling ──
def resolution_poll_loop():
    """After a market ends, poll the Gamma API every 10 seconds for at least 10 minutes"""
    last_window_id = ""
    poll_start = 0
    poll_duration = 600  # 10 minutes

    while True:
        try:
            # Check whether the window was just switched
            if current_window_id != last_window_id:
                last_window_id = current_window_id
                poll_start = time.time()

            # If within the polling period
            if poll_start > 0 and (time.time() - poll_start) < poll_duration:
                # Check whether the market is closed
                meta = fetch_market_meta(current_slug)
                if meta and meta.get("closed"):
                    # Try to fetch the resolution result
                    entry = {
                        "source": "polymarket_gamma",
                        "server_ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                        "market_window_id": current_window_id,
                        "slug": current_slug,
                        "event_type": "gamma_resolution_poll",
                        "closed": meta.get("closed", False),
                        "condition_id": current_condition_id,
                        "raw_meta": meta,
                    }
                    write_jsonl(RESOLUTIONS_FILE, entry)
                    stats["resolutions"] += 1
                    log(f"Gamma resolution poll: {current_slug} closed={meta.get('closed')}")

        except Exception as e:
            log(f"Resolution polling error: {e}")

        time.sleep(10)


# ── data quality status ──
def data_quality_loop():
    """Output data quality status every 5 seconds (including health fields)"""
    while True:
        try:
            now = time.time()
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            remaining = window_end_ts - now if window_end_ts > 0 else 0
            if not current_slug or not current_window_id or window_start_ts <= 0:
                time.sleep(1)
                continue
            
            # Compute the expected window
            expected_ts = int(now) // 300 * 300
            expected_end = expected_ts + 300
            switch_lag = (now - last_successful_switch_at) if last_successful_switch_at > 0 else -1

            # Count ticks within the window
            window_tick_count = 0
            try:
                with open(ORDERBOOK_TICKS_FILE, "r") as f:
                    for line in f:
                        if current_window_id in line:
                            window_tick_count += 1
            except:
                pass

            # Compute tick age
            last_price_age = max(0, int((now - last_price_tick_at) * 1000)) if last_price_tick_at > 0 else -1
            last_fallback_price_age = max(0, int((now - last_fallback_price_tick_at) * 1000)) if last_fallback_price_tick_at > 0 else -1
            last_ob_age = max(0, int((now - last_orderbook_tick_at) * 1000)) if last_orderbook_tick_at > 0 else -1
            official_price_data_lag_ms = max(0, int((now - last_rtds_ts) * 1000)) if last_rtds_ts > 0 else -1
            fallback_price_data_lag_ms = max(0, int((now - last_fallback_price_ts) * 1000)) if last_fallback_price_ts > 0 else -1

            # Judge data quality
            quality = "good"
            if remaining < 0:
                quality = "stale"  # negative seconds must be stale
            elif len(current_tokens) < 2 or not current_tokens[0] or not current_tokens[1]:
                quality = "bad"
            elif last_price_age < 0 or last_price_age > PRICE_STALE_SECONDS * 1000:
                quality = "bad"
                stats["rtds_degraded"] = True
            elif last_ob_age < 0 or last_ob_age > ORDERBOOK_STALE_SECONDS * 1000:
                quality = "bad"
            elif last_price_age > PRICE_DEGRADED_SECONDS * 1000:
                quality = "degraded"
            elif ptb_pending:
                quality = "degraded"  # degrade while PTB is pending
            elif not stats["ws_connected"]:
                quality = "bad"
            elif stats["rtds_degraded"]:
                quality = "degraded"
            elif window_tick_count < 10:
                quality = "degraded"

            entry = {
                "source": "data_quality",
                "server_ts": now_utc.isoformat(),
                "market_window_id": current_window_id,
                "slug": current_slug,
                "collector_running": True,
                "current_market_slug": current_slug,
                "token_ids_ready": len(current_tokens) >= 2,
                "clob_ws_online": stats["ws_connected"],
                "rtds_chainlink_online": stats["rtds_connected"],
                "rtds_degraded": stats["rtds_degraded"],
                "last_orderbook_tick_age_ms": last_ob_age,
                "last_price_tick_age_ms": last_price_age,
                "last_fallback_price_tick_age_ms": last_fallback_price_age,
                "official_price_data_lag_ms": official_price_data_lag_ms,
                "fallback_price_data_lag_ms": fallback_price_data_lag_ms,
                "last_price_tick_at": datetime.datetime.fromtimestamp(last_price_tick_at, tz=datetime.timezone.utc).isoformat() if last_price_tick_at > 0 else None,
                "last_fallback_price_tick_at": datetime.datetime.fromtimestamp(last_fallback_price_tick_at, tz=datetime.timezone.utc).isoformat() if last_fallback_price_tick_at > 0 else None,
                "last_official_price_timestamp": datetime.datetime.fromtimestamp(last_rtds_ts, tz=datetime.timezone.utc).isoformat() if last_rtds_ts > 0 else None,
                "last_fallback_price_timestamp": datetime.datetime.fromtimestamp(last_fallback_price_ts, tz=datetime.timezone.utc).isoformat() if last_fallback_price_ts > 0 else None,
                "last_fallback_price": last_fallback_price,
                "last_fallback_price_source": last_fallback_price_source,
                "last_fallback_price_error": last_fallback_price_error,
                "last_orderbook_tick_at": datetime.datetime.fromtimestamp(last_orderbook_tick_at, tz=datetime.timezone.utc).isoformat() if last_orderbook_tick_at > 0 else None,
                "current_window_tick_count": window_tick_count,
                "current_window_quality": quality,
                "ptb_pending": ptb_pending,
                "remaining_seconds": int(remaining),
                "expected_window_start_ts": expected_ts,
                "current_window_start_ts": window_start_ts,
                "last_successful_market_switch_at": datetime.datetime.fromtimestamp(last_successful_switch_at, tz=datetime.timezone.utc).isoformat() if last_successful_switch_at > 0 else None,
                "switch_lag_seconds": round(switch_lag, 1),
                "negative_seconds_seen": negative_seconds_seen,
                "market_switch_reason": market_switch_reason,
                "stats": {
                    "windows": stats["windows"],
                    "price_ticks": stats["price_ticks"],
                    "orderbook_ticks": stats["orderbook_ticks"],
                    "price_change_ticks": stats["price_change_ticks"],
                    "trade_ticks": stats["trade_ticks"],
                    "market_meta": stats["market_meta"],
                    "resolutions": stats["resolutions"],
                    "fallback_price_ticks": stats["fallback_price_ticks"],
                    "rtds_reconnects": stats["rtds_reconnects"],
                    "rtds_stale_events": stats["rtds_stale_events"],
                },
            }
            write_jsonl(DATA_QUALITY_FILE, entry)

        except Exception as e:
            log(f"Data quality error: {e}")

        time.sleep(1)


# ── Statistics report ──
def stats_loop():
    """Output statistics every 60 seconds"""
    while True:
        time.sleep(60)
        elapsed = time.time() - stats["start_time"]
        log(f"[Statistics] Running {elapsed/60:.1f}min | "
            f"Markets:{stats['windows']} | "
            f"Prices:{stats['price_ticks']} | "
            f"Order book:{stats['orderbook_ticks']} | "
            f"Price changes:{stats['price_change_ticks']} | "
            f"Trades:{stats['trade_ticks']} | "
            f"Metadata:{stats['market_meta']} | "
            f"Resolutions:{stats['resolutions']} | "
            f"CLOB WS:{'✓' if stats['ws_connected'] else '✗'} | "
            f"RTDS WS:{'✓' if stats['rtds_connected'] else '✗'} | "
            f"RTDS degraded:{stats['rtds_degraded']}")


# ── Main entry point ──
def main():
    log("=" * 60)
    log("Polymarket Real-Market Data Collector v3")
    log(f"Asset: {current_asset.upper()} | Timeframe: {current_timeframe}")
    log(f"Output directory: {DATA_DIR}")
    log("=" * 60)

    # Start the threads
    threads = [
        threading.Thread(target=clob_ws_loop, daemon=True, name="clob-ws"),
        threading.Thread(target=rtds_ws_loop, daemon=True, name="rtds-ws"),
        threading.Thread(target=fallback_price_loop, daemon=True, name="fallback-price"),
        threading.Thread(target=market_check_loop, daemon=True, name="market-check"),
        threading.Thread(target=resolution_poll_loop, daemon=True, name="resolution-poll"),
        threading.Thread(target=data_quality_loop, daemon=True, name="data-quality"),
        threading.Thread(target=stats_loop, daemon=True, name="stats"),
    ]

    for t in threads:
        t.start()
        log(f"Starting thread: {t.name}")

    # Main thread waits
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log("Stop signal received; exiting...")
        sys.exit(0)


if __name__ == "__main__":
    main()
