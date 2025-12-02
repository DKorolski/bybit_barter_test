# Scalping orderbook backtest (v8-style)

This note explains how to run and interpret the `scalping_orderbook_backtest` example that mirrors the Python v8 scalper. The example replays historic orderbook snapshots and trades to simulate maker-only entries and exits with queue-aware fills, cluster-driven take-profits, time-stop repricing, and end-of-day forced closures.

## How to run

From the repository root:

```bash
cargo run --example scalping_orderbook_backtest
```

The example reads bundled sample data and writes a CSV report to `barter/examples/data/scalping_backtest.csv`.

### Inputs
- **Orderbook snapshots:** `barter/examples/data/2025-11-16_BTCUSDT_ob200.data` (JSON lines with bids/asks).
- **Trades:** `barter/examples/data/BTCUSDT2025-11-16.csv` (timestamp, side, size, price).

You can swap the files by editing the constants at the top of `barter/examples/scalping_orderbook_backtest.rs`.

## Configuration knobs

The strategy parameters live in `StrategyConfig` (see the example source). Defaults align with the Python v8 baseline:

| Field | Meaning | Default |
| --- | --- | --- |
| `maker_fee` | Exchange maker fee fraction per side | `0.0` |
| `taker_fee` | Exchange taker fee fraction per side | `0.0003` |
| `broker_fee_abs` | Fixed broker fee per side | `1.0` |
| `tp_ticks` | Take-profit distance in ticks | `20` |
| `entry_cluster_q` | Entry cluster quantile threshold | `0.995` |
| `exit_cluster_q` | Exit cluster quantile threshold | `0.3` |
| `exit_cluster_start_ms` | Earliest time to look for exit clusters after entry | `800` ms |
| `exit_cluster_max_diff_ticks` | Max tick gap between TP and cluster price | `17` ticks |
| `max_hold_ms` | Time before switching to time-stop | `30000` ms |
| `time_stop_reprice_ms` | Reprice interval for time-stop limit orders | `2000` ms |
| `tick_size` | Instrument tick size | `0.1` |
| `entry_timeout_ms` | Cancel pending entry after this time | `2500` ms |
| `exit_order_timeout_ms` | Cancel cluster exit if unfilled by this time | `2500` ms |
| `max_cluster_depth_entry` | Depth (levels) scanned for entry clusters | `12` |
| `max_cluster_depth_exit` | Depth (levels) scanned for exit clusters | `12` |

Adjust values in `StrategyConfig::default` or construct a custom config in `main`.

## Execution flow

1. **Cluster history building:** For each orderbook snapshot, the code keeps the largest bid/ask sizes within `max_cluster_depth_*` levels and appends them to rolling histories for quantile calculations.
2. **Entry signal:** After at least `MIN_HISTORY` (50) snapshots, a long (Buy) signal triggers when the bid cluster exceeds the `entry_cluster_q` threshold and lies within the allowed depth; a short (Sell) mirrors this on the ask side.
3. **Pending maker entry:** Instead of immediate fills, the strategy posts a maker limit at the best bid/ask with a queue-ahead volume estimate. Trades reduce the queue until the order fills or the `entry_timeout_ms` elapses.
4. **Position & TP:** On fill, a position records the entry time/price and a plain TP (`tp_plain`) offset by `tp_ticks * tick_size` in the trade direction.
5. **Exit logic:**
   - **Cluster exit:** After `exit_cluster_start_ms`, the strategy searches for a qualifying cluster on the opposite side, placing a maker limit one tick in front if it is close enough to `tp_plain`. A cluster exit order expires after `exit_order_timeout_ms` and can be replaced later.
   - **Time-stop:** When holding time exceeds `max_hold_ms`, any cluster order is removed and a maker limit is (re)posted at the best bid/ask, repricing every `time_stop_reprice_ms` until filled.
6. **Fees & PnL:** Both entry and exit are treated as maker liquidity. Exchange maker fees and a fixed broker fee apply per side when computing gross/net ticks and returns.
7. **End-of-day close:** If events finish while still in a position, the strategy force-closes using the last trade (or mid-price) to emit a final log entry with reason `eod_force`.

## Output CSV

The run produces `scalping_backtest.csv` with per-trade records:

```
entry_time,exit_time,direction,side,entry_price,exit_price,gross_ticks,net_ticks,gross_ret,net_ret,reason,exit_liquidity,tp_plain,exit_from_cluster,entry_fee,exit_fee,total_fee,hold_ms
```

- `direction`/`side` reflect Buy (+1) or Sell (-1) positions.
- `reason` is one of `cluster_exit`, `time_stop_limit`, or `eod_force`.
- `exit_from_cluster` marks whether the exit came from a cluster order (vs. time-stop or EOD).
- `hold_ms` captures position duration.

Use the CSV to compare against the Python v8 journal for the same day.