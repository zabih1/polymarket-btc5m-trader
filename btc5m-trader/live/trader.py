#!/usr/bin/env python3
"""
BTC 5M Live Trader — CLOB SDK execution
Fully isolated: separate config / state / trades / logs
"""
import os, sys, json, time, threading, re
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dotenv import load_dotenv

from shared.utils import log, log_trade, CN
from shared.config import Config
from shared.btc_price import get_btc, get_btc_fresh, btc_price_loop, fetch_ptb, extract_tokens, find_market, latest_market_snapshot, fetch_platform_crypto_price
import clob_executor

load_dotenv()

# === Paths ===
MODE = "live"
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

# Root trades.jsonl (shared trade log)
ROOT_TRADES = DATA_ROOT / "trades.jsonl"

STATE_FILE = DATA_DIR / "state.json"
NOTIFY_FILE = DATA_DIR / "notify.txt"

# === Wallet (read from the shared .env) ===
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
FUNDER_ADDRESS = os.getenv("FUNDER_ADDRESS", "")

# === Data directory (shared, read-only) ===
COLLECTOR_DATA = DATA_ROOT / "shared"
MARKETS_FILE = COLLECTOR_DATA / "markets.jsonl"
BTC_PRICE_FILE = COLLECTOR_DATA / "btc_price.jsonl"

# === Runtime state ===
config = Config(str(TRADER_DIR))
cfg = config.data  # Shorthand reference
bankroll = 1.01
trade_count = 0
win_count = 0
loss_count = 0
total_withdrawn = 0.0
consecutive_losses = 0
cooldown_until = 0
btc_price = 0.0

executor_ready = False


def load_state():
    global bankroll, trade_count, win_count, loss_count, total_withdrawn, consecutive_losses, cooldown_until
    state = config.load_state()
    bankroll = state.get("bankroll", 1.01)
    trade_count = state.get("trade_count", 0)
    win_count = state.get("win_count", 0)
    loss_count = state.get("loss_count", 0)
    total_withdrawn = state.get("total_withdrawn", 0.0)
    consecutive_losses = state.get("consecutive_losses", 0)
    cooldown_until = state.get("cooldown_until", 0)
    log(f"State loaded: ${bankroll:.2f}/{trade_count} trades ({win_count}W/{loss_count}L)", MODE)


def record_skip(slug, reason, remaining=0):
    """Record a skip event to trades.jsonl (visible in the shared trade log)."""
    record = {
        "slug": slug,
        "time": datetime.now(CN).isoformat(),
        "mode": "live",
        "direction": "none",
        "gap": 0,
        "btc_entry": round(btc_price, 2) if btc_price else 0,
        "ptb": 0,
        "buy_price": 0,
        "buy_amount": 0,
        "seconds_left": remaining,
        "won": False,
        "net_profit": 0,
        "status": "skipped",
        "skip_reason": reason,
    }
    try:
        with open(ROOT_TRADES, "a") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except:
        pass


def save_state():
    state = {
        "bankroll": bankroll, "trade_count": trade_count,
        "win_count": win_count, "loss_count": loss_count,
        "total_withdrawn": total_withdrawn,
        "consecutive_losses": consecutive_losses,
        "cooldown_until": cooldown_until,
        "executor_ready": executor_ready,
    }
    config.save_state(state)
    # Also write the root trader_state.json
    try:
        root_state = DATA_ROOT / "trader_state.json"
        with open(root_state, "w") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
    except:
        pass


def parse_float(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except:
        return default


def calc_bet_size(current_bankroll: float) -> float:
    """Live path mirrors sim staking, then caps by max_live_amount."""
    min_order_amount = 1.0
    mode = str(cfg.get("bet_mode", "fraction") or "fraction")
    if mode in ("amount", "fixed_amount"):
        target = parse_float(cfg.get("fixed_bet_amount", 1.0), 1.0)
    else:
        target = current_bankroll * parse_float(cfg.get("bet_fraction", 1.0), 1.0)
    max_live_amount = parse_float(cfg.get("max_live_amount", 1.0), 1.0)
    target = max(min_order_amount, target)
    return round(min(target, max_live_amount, max(min_order_amount, current_bankroll)), 6)


# === BTC price ===
def btc_update_loop():
    global btc_price
    while True:
        try:
            p = get_btc_fresh(retries=1)
            if p > 0:
                btc_price = p
        except:
            pass
        time.sleep(1)


def load_markets() -> list:
    try:
        if MARKETS_FILE.exists():
            with open(MARKETS_FILE) as f:
                return [json.loads(l) for l in f if l.strip()]
    except:
        pass
    return []


def markets_reader():
    """Load market data every 5 seconds."""
    while True:
        yield load_markets()
        time.sleep(5)


# === Notifications ===
def notify(msg):
    with open(NOTIFY_FILE, "a") as f:
        f.write(f"[{datetime.now(CN).strftime('%H:%M:%S')}] {msg}\n")
    log(f"📢 {msg}", MODE)


# === Signal detection ===
def check_signal(markets, slug, now, current_5m):
    """Detect a trading signal. Returns (direction, buy_price, token_id, gap) or None."""
    global btc_price

    ptb = fetch_ptb(slug)
    if ptb <= 0:
        return None

    snap = latest_market_snapshot(slug)
    snap_ts = snap.get("timestamp", "")
    try:
        snap_time = datetime.fromisoformat(snap_ts.replace("Z", "+00:00"))
        snap_age = time.time() - snap_time.timestamp()
    except:
        snap_age = 999
    if snap_age > 15:
        r = current_5m + 300 - now
        record_skip(slug, f"data too old: {snap_age:.0f}s", r)
        return None

    if snap.get("chainlink_price", 0) > 0:
        btc_price = float(snap["chainlink_price"])
    elif btc_price <= 0:
        btc_price = get_btc_fresh()
    if btc_price <= 0:
        return None

    gap = btc_price - ptb

    # Check the gap threshold
    if abs(gap) < cfg["gap_threshold"]:
        return None

    direction = "Up" if gap > 0 else "Down"
    m = find_market(markets, slug)
    tid_up, tid_down = extract_tokens(m)
    buy_price = 0
    token_id = ""

    remaining = current_5m + 300 - now
    if remaining < 5:
        return None  # Too late

    up_p = float(snap.get("up_ask") or snap.get("up_price") or 0)
    down_p = float(snap.get("down_ask") or snap.get("down_price") or 0)
    if direction == "Up":
        buy_price = up_p if 0 < up_p < 1 else 0
        token_id = tid_up
    else:
        buy_price = down_p if 0 < down_p < 1 else 0
        token_id = tid_down

    if buy_price <= 0 or buy_price > 0.99:
        return None
    if buy_price < cfg["min_buy_price"]:
        return None

    return {
        "direction": direction,
        "buy_price": buy_price,
        "token_id": token_id,
        "gap": gap,
        "ptb": ptb,
        "btc_price": btc_price,
        "remaining": remaining,
        "data_age": snap_age,
        "market_data_source": snap.get("market_data_source", ""),
    }


def live_ready_to_trade() -> tuple[bool, str]:
    """Hard gate for real-money execution."""
    global executor_ready
    if cfg.get("paused", True):
        return False, "live config is paused"
    if not cfg.get("armed", False):
        return False, "live not manually armed"
    health = clob_executor.health(check_auth=True)
    balance_payload = health.get("balance_allowance", {}) if isinstance(health, dict) else {}
    try:
        collateral_balance = float(balance_payload.get("balance", 0) or 0) / 1_000_000
    except Exception:
        collateral_balance = 0.0
    executor_ready = bool(health.get("ok") and collateral_balance >= 1.0)
    if not executor_ready:
        return False, f"CLOB SDK not ready or pUSD balance below $1 (balance=${collateral_balance:.2f})"
    return True, "ready"


# ===== Main loop =====
def main():
    global executor_ready, btc_price, bankroll, trade_count, win_count, loss_count
    global consecutive_losses, cooldown_until

    log("=" * 50, MODE)
    log("BTC 5M LIVE TRADER", MODE)
    log(f"Wallet: {FUNDER_ADDRESS[:10]}...{FUNDER_ADDRESS[-6:]}", MODE)
    log("Execution layer: backend CLOB SDK", MODE)
    log("=" * 50, MODE)

    # Load config and state
    config.load()
    load_state()

    # Check the backend CLOB executor; real orders are still gated by paused/armed/balance.
    executor_ready = clob_executor.check_ready()
    if not executor_ready:
        log("⚠️ CLOB SDK executor unavailable! Continuing to run, but no real orders will be placed", MODE)

    # Start the price thread
    threading.Thread(target=btc_update_loop, daemon=True).start()

    # Wait for the first BTC price
    time.sleep(3)
    btc_price = get_btc()

    # Main loop
    processed = set()
    market_gen = markets_reader()

    while True:
        try:
            config.load()
            now = int(time.time())
            current_5m = (now // 300) * 300
            slug = f"btc-updown-5m-{current_5m}"
            remaining = current_5m + 300 - now

            # Check paused
            if cfg.get("paused", False):
                time.sleep(5)
                continue

            if not cfg.get("armed", False):
                time.sleep(5)
                continue

            # Check cooldown
            if cooldown_until > now:
                time.sleep(3)
                continue

            # Skip already-processed markets
            if slug in processed:
                time.sleep(3)
                continue

            # Strategy 1 window: keep listening from T-entry_second to T-5s.
            if remaining > cfg["entry_second"]:
                time.sleep(min(remaining - cfg["entry_second"], 5))
                continue

            if remaining <= 5:
                if slug not in processed:
                    record_skip(slug, "No strategy 1 signal before T-5", remaining)
                    processed.add(slug)
                time.sleep(3)
                continue

            # Load market data
            markets = load_markets()

            # Check the signal
            sig = check_signal(markets, slug, now, current_5m)
            if not sig:
                if remaining % 5 == 0:
                    snap = latest_market_snapshot(slug)
                    ptb = fetch_ptb(slug)
                    cur = float(snap.get("chainlink_price") or btc_price or 0)
                    gap = cur - ptb if ptb > 0 and cur > 0 else 0
                    log(f"Listening {slug} T-{remaining}s gap=${gap:+,.0f} Up={snap.get('up_ask') or snap.get('up_price', 0)} Down={snap.get('down_ask') or snap.get('down_price', 0)}", MODE, tag="LIVE")
                time.sleep(1)
                continue

            direction = sig["direction"]
            buy_price = sig["buy_price"]
            gap = sig["gap"]

            # Calculate the bet
            min_order_amount = 1.0  # Market order minimum on the Polymarket UI.
            max_live_amount = float(cfg.get("max_live_amount", 1.0))
            bet_size = calc_bet_size(bankroll)
            if bet_size < min_order_amount:
                log(f"⏭ Minimum order amount ${min_order_amount:.2f} > per-trade limit ${max_live_amount:.2f}", MODE, tag="LIVE")
                record_skip(slug, f"minimum order amount ${min_order_amount:.2f} > limit ${max_live_amount:.2f}", remaining)
                processed.add(slug)
                time.sleep(3)
                continue
            size = round(bet_size / buy_price, 2)

            log(f"🎯 Signal triggered! {slug} gap=${gap:+,.0f} {direction} @{buy_price:.3f} bet ${bet_size:.2f}", MODE, tag="LIVE")

            # ===== Execute the order =====
            ready, ready_reason = live_ready_to_trade()
            result = {}
            if ready:
                order_type = str(cfg.get("live_order_type", "FAK") or "FAK").upper()
                log(f"[CLOB] Market order: {direction} ${bet_size:.2f} token={sig['token_id'][:10]}...", MODE)
                result = clob_executor.place_market_order(
                    direction=direction,
                    amount=bet_size,
                    slug=slug,
                    token_id=sig["token_id"],
                    max_order_amount=max_live_amount,
                    order_type=order_type,
                    dry_run=False,
                )
                if result.get("success"):
                    log(f"[CLOB] Order placed successfully: {result.get('status', '')} {result.get('orderID', '')}", MODE)
                else:
                    log(f"[CLOB] Order failed: {result.get('error', 'unknown')}", MODE)
            else:
                log(f"⏭ {ready_reason}, skipping real order", MODE)
                record_skip(slug, ready_reason, remaining)

            # Record the trade
            record = {
                "slug": slug,
                "time": datetime.now(CN).isoformat(),
                "mode": "live",
                "direction": direction,
                "gap": round(gap, 2),
                "btc_entry": round(btc_price, 2),
                "ptb": round(sig["ptb"], 2),
                "buy_price": buy_price,
                "buy_amount": round(bet_size, 2),
                "bet_mode": cfg.get("bet_mode", "fraction"),
                "fixed_bet_amount": cfg.get("fixed_bet_amount", 1.0),
                "bet_fraction": cfg.get("bet_fraction", 1.0),
                "size": size,
                "data_age": round(sig.get("data_age", 0), 3),
                "market_data_source": sig.get("market_data_source", ""),
                "orderID": result.get("orderID", ""),
                "status": (result.get("status") or "placed") if result.get("success") else ("failed" if ready else "skipped"),
                "executor": result.get("executor", "clob_sdk") if ready else "none",
                "skip_reason": "" if result.get("success") else (result.get("error") or result.get("errorMsg") or ready_reason),
                "executor_result": result,
            }
            log_trade(record, MODE)
            # Also write the root trades.jsonl
            try:
                with open(ROOT_TRADES, "a") as f:
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
            except:
                pass
            processed.add(slug)
            if not result.get("success"):
                save_state()
                time.sleep(3)
                continue

            # Wait for settlement
            settle_time = current_5m + 300 + 5
            wait = settle_time - time.time()
            if wait > 0:
                log(f"⏳ Waiting for settlement ({wait:.0f}s)...", MODE)
                time.sleep(wait)

            # ===== Settlement =====
            window_end_ts = current_5m + 300
            platform_price = fetch_platform_crypto_price(current_5m, window_end_ts, max_retries=300, retry_interval=2.0)
            
            btc_final = platform_price.get("closePrice")
            open_price = platform_price.get("openPrice") or sig["ptb"]
            settle_source = platform_price.get("source", "unknown")
            
            if btc_final is None or btc_final <= 0:
                # Platform settlement price not returned; mark as pending settlement
                log(f"⚠️ Platform settlement price not returned, marked as pending: {slug}", MODE, tag="LIVE")
                # Update the trade record to pending status
                record["settlement_status"] = "pending"
                record["settle_source"] = settle_source
                record["platform_close_price"] = None
                record["btc_final"] = None
                record["exclude_from_backtest"] = True
                try:
                    with open(ROOT_TRADES, "a") as f:
                        f.write(json.dumps(record, ensure_ascii=False) + "\n")
                except:
                    pass
                processed.add(slug)
                continue
            
            # Correct win/loss rule: final price >= open price means Up wins
            actual_winner = "Up" if btc_final >= open_price else "Down"
            won = (direction == actual_winner)
            
            # Update the trade record
            record["btc_final"] = round(btc_final, 2)
            record["platform_close_price"] = round(btc_final, 2)
            record["settlement_gap"] = round(btc_final - open_price, 2)
            record["actual_winner"] = actual_winner
            record["settlement_status"] = "confirmed"
            record["settle_source"] = settle_source
            record["settle_confirmed_at"] = datetime.now(CN).isoformat()
            record["exclude_from_backtest"] = False
            
            if won:
                profit = bet_size * (1 - buy_price) / buy_price
                bankroll += profit
                win_count += 1
                consecutive_losses = 0
                log(f"✅ Win! Profit +${profit:.2f} balance=${bankroll:.2f}", MODE, tag="LIVE")
            else:
                loss = bet_size
                bankroll -= loss
                loss_count += 1
                consecutive_losses += 1
                log(f"❌ Loss! -${loss:.2f} balance=${bankroll:.2f}", MODE, tag="LIVE")

            trade_count += 1
            save_state()

        except KeyboardInterrupt:
            log("🛑 Stopped", MODE)
            break
        except Exception as e:
            log(f"⚠️ Error: {e}", MODE)
            time.sleep(5)


if __name__ == "__main__":
    main()
