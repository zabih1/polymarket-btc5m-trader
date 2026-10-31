# Polymarket BTC 5-Minute Trader

An automated trading system for Polymarket's BTC Up or Down 5-minute prediction markets: real-time data collection, a strategy engine, backtesting, and simulation/live trading.

> **Warning: experimental software for educational purposes. Trading involves risk of loss, past performance does not guarantee future results, and live trading uses real money. Never trade with money you cannot afford to lose. Start in simulation mode.**

## How the market works

Every 5 minutes Polymarket opens a market named like `btc-updown-5m-<start_unix_timestamp>`, where the timestamp is the window start rounded down to a multiple of 300 seconds.

- **Price to beat (PTB)** is the BTC price at the start of the window.
- At settlement, if the final BTC price is >= PTB, **Up** wins; otherwise **Down** wins.
- Each side has a token priced between $0.01 and $0.99. That price is roughly the market's implied probability. A winning token pays $1.
- Example: buying an Up token at $0.95 wins about $0.05 if Up wins, and loses $1 if Down wins. A strategy that only buys at $0.95 or above therefore needs a very high win rate just to break even.

## How the system works

The system is three separate processes that share data through JSONL files under one data directory, not through a database or internal API:

```
Collector  ->  data/  ->  Trader (sim or live)  ->  Polymarket CLOB API (live only)
                |
                ->  Backtests read the same data files
```

1. **Collect.** `btc5m-collector/collect.py` streams Chainlink BTC/USD prices from Polygon RPC every second, market metadata from the Polymarket Gamma API, and Up/Down order-book prices from the Polymarket CLOB WebSocket (with a CLOB REST snapshot as backup). `btc5m-trader/collectors/true_market_collector.py` is a stricter collector that also writes `true_market/` files: window metadata, price ticks, order-book ticks, real trades, resolutions, and data-quality checks.
2. **Wait for the entry window.** Each trader loops over the current 5-minute window and only evaluates trades in the last `entry_second` seconds before settlement (25 seconds for the default strategy), polling roughly every 0.1 seconds. A window is processed at most once.
3. **Check the signal.** For the current window it fetches the PTB, gets the current BTC price, and calculates `gap = BTC price - PTB`. Direction is Up if gap > 0, otherwise Down. It then reads the latest order-book tick (no older than about 3 seconds in simulation) and requires a fully fillable $1 order at a price that passes the configured filters.
4. **Execute or skip.** If every check passes, the sim trader records a simulated trade or the live trader places a real market order. Otherwise it records a skip reason (for example: gap too small, probability too low, no liquidity, or stale data) in `trades.jsonl`.
5. **Settle.** After the window ends, the trader fetches Polymarket's official close price, decides the winner, calculates profit/loss, and updates `state.json` and `trades.jsonl`.

## The default strategy

The active strategy in `btc5m-trader/config.json`, `btc5m-trader/sim/config.json`, and `btc5m-trader/live/config.json` is strategy 3, "Recommended: T25 P95 gap20 fixed $1":

| Setting | Value |
| --- | --- |
| Entry window | Last 25 seconds of the window |
| Gap threshold | \|gap\| >= $20 |
| Minimum buy price | 0.95 |
| Bet size | Fixed $1 |
| Starting simulated capital | $10 |

In other words: only trade late in the window, only when BTC is already at least $20 above or below the start price, and only when the market already prices that side at 95% or more. The config notes describe this strategy as 269 trades, 267 wins and 2 losses in its filtered simulation sample, turning $10 into about $13.17. Treat that as a small, filtered sample, not a guarantee. A broader backtest claim in the same notes was 133 markets at 55-65% signal accuracy, which shows how much the result depends on which trades are filtered in.

Other strategies (1, 2, 4, 5) are kept in the same config files for comparison, including earlier, wider, and more aggressive variants. Changing `active_strategy` in the relevant `config.json` switches strategy; the trader reloads config each loop.

## Simulation vs live

- **Simulation** (`btc5m-trader/sim/trader.py`): uses a fake bankroll, never touches a wallet or browser, and settles against Polymarket's official close price. Its config and state are separate from live.
- **Live** (`btc5m-trader/live/trader.py`): uses the same signal logic, but real orders are blocked unless all three gates pass: the config is not paused, it has been manually armed, and the CLOB executor is healthy with at least $1 of pUSD balance. Orders are placed through `btc5m-trader/clob_executor/` using Polymarket's official Python CLOB SDK on Polygon (chain 137), signed with the private key from `.env`.

## Project layout

```
btc5m-collector/
  collect.py                 # Real-time data collector
btc5m-trader/
  sim/trader.py              # Simulation trader
  live/trader.py             # Live trader (CLOB SDK)
  shared/
    config.py                # Sim/live config and strategy slots
    btc_price.py             # Chainlink price, PTB, market snapshots
    utils.py                 # Logging and trade records
  collectors/
    true_market_collector.py # Stricter true-market collector
  clob_executor/
    __init__.py              # Signed CLOB market orders
  backtest/
    btc5m_backtest.py        # Strict backtest on collected data
    btc5m_backtest_deep.py   # Deeper backtest variant
    pragmatic_research.py    # Research/backtest helper
  test_pipeline.py           # Quick signal-pipeline check
  config.json                # Root strategy config
  sim/config.json            # Simulation config
  live/config.json           # Live config
  .env.example               # Wallet and mode template (no secrets)
deployment/systemd/
  btc5m-collector.service    # Service template (collector)
  btc5m-sim.service          # Service template (simulation)
  btc5m-live.service         # Service template (live)
```

## Quick start

Requirements: Python 3.11+.

```bash
# Install dependencies
pip install requests websocket-client python-dotenv py_clob_client_v2

# Optional: point all processes at one data directory.
# Defaults to <repo root>/data if unset.
export BTC5M_DATA_DIR=/path/to/data

# 1. Start the collector (in one terminal)
python btc5m-collector/collect.py

# 2. Start the simulation trader (in another terminal, simulation first!)
python btc5m-trader/sim/trader.py

# 3. Live trading only after simulation looks sane, and only with
#    paused=false and armed=true in btc5m-trader/live/config.json,
#    plus a funded .env (see below).
python btc5m-trader/live/trader.py
```

Backtests read from the data directory:

```bash
python btc5m-trader/backtest/btc5m_backtest.py --help
```

## Configuration

Copy the template and fill in only your own values:

```bash
cp btc5m-trader/.env.example btc5m-trader/.env
```

`.env` holds `PRIVATE_KEY` (without the `0x` prefix) and `FUNDER_ADDRESS`. **Never commit `.env`, never share a private key, and never paste one into chat.** The live trader will not place real orders while paused, unarmed, or underfunded, even if a signal fires.

Strategy settings live in the three `config.json` files: `entry_second`, `gap_threshold`, `min_buy_price`, `bet_mode` (`fixed_amount` or `fraction`), `fixed_bet_amount`, `bet_fraction`, `max_live_amount`, `max_consecutive_losses`, `cooldown_seconds`, and `initial_capital`. Note that `.env.example` contains older example values (for example gap 10 and entry 5) that differ from `config.json` (gap 20 and entry 25); `config.json` is what the traders actually use.

For systemd, edit the `Environment` lines in `deployment/systemd/` to match your install path and data directory before enabling a unit. The sim and live units assume the repo is checked out at `~/polymarket-btc5m-trader`.

## Notes

- Data paths are set with `BTC5M_DATA_DIR`, defaulting to `<repo root>/data`. Collector, traders, backtests, and the CLOB executor all derive their paths from it.
- Strategy numbers, thresholds, settlement rules, token order (tokens[0] = Up, tokens[1] = Down), Chainlink contract, and Polymarket API endpoints are as configured in `config.json` and the code.
- The live path is the CLOB SDK only. `test_pipeline.py` is a signal check.

## License

MIT, see `LICENSE`.
