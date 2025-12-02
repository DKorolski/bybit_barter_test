# v8 Orderbook-Based Scalping Strategy — Developer Documentation

This document explains the **v8** version of the orderbook-based scalping strategy, as implemented in the Python prototype. It is written for an engineer who needs to **re‑implement or audit** the logic (e.g., in Rust) as faithfully as possible.

The goal is **tick‑level backtesting** of a strategy that:
- Enters using **limit orders at the best prices** when a large liquidity **cluster** appears.
- Exits primarily using **limit orders at “whale” clusters** near a predefined take‑profit (TP) level.
- Falls back to a **time‑based exit mode** (“time‑stop”) if price does not reach a suitable cluster in time.
- Applies both **exchange maker/taker fees** and a **fixed broker fee** per side.
- Writes a detailed **trade log** for analysis and matching across implementations.

---

## 1. Data Inputs

### 1.1 Orderbook (L2 snapshots)

- Each snapshot contains:
  - `timestamp` in **milliseconds** since epoch.
  - `bids`: list of levels `[(price, size), ...]` sorted **best bid first** (highest price).
  - `asks`: list of levels `[(price, size), ...]` sorted **best ask first** (lowest price).
- Only the **top N levels** are used (`MAX_LEVELS`, e.g., 20). Lower levels can be ignored for cluster calculations.

### 1.2 Trades

- Each trade row contains:
  - `timestamp` (seconds or ms; in v8 we convert to **milliseconds**).
  - `side`: `"Buy"` or `"Sell"`.
  - `price`.
  - Additional columns may exist but are not required for the strategy.

### 1.3 Event Stream

1. **Merge** orderbook snapshots and trades into a single event stream, sorted by timestamp.
2. On each event:
   - If it is an **orderbook snapshot** → update best bid/ask, cluster metrics, and possibly perform **entry/exit decisions**.
   - If it is a **trade event** → used only for the **time‑stop limit execution** (fill simulation).

Time resolution: all timestamps are treated as **milliseconds**.

---

## 2. Configuration Parameters (v8)

Below are the key parameters in v8. The exact numbers can differ per experiment — what matters is the **semantics**.

```python
ROWS_TO_PROCESS: Optional[int] = 800_000  # for backtest truncation (optional)
MAX_LEVELS     = 20

TICK_SIZE      = 0.1           # price tick size
ORDER_SIZE     = 1.0           # order size, in base asset

# Exchange fees as fraction of notional per side
FEE_RATE_MAKER = 0.0
FEE_RATE_TAKER = 0.0066/100  # ~0.0066%

# Fixed broker fee per side in quote currency (e.g. USDT)
BROKER_FEE_ABS = 1.0

# Ideal take-profit distance in ticks
TP_TICKS       = 200

# Entry clusters
ENTRY_CLUSTER_Q         = 0.895  # quantile of cluster strength for signal
MAX_CLUSTER_DEPTH_ENTRY = 12     # how many levels to consider for entry cluster

# Exit clusters
EXIT_CLUSTER_Q          = 0.3    # lower quantile; exit clusters are more frequent
MAX_CLUSTER_DEPTH_EXIT  = 12

# After entering, when do we start looking for exit clusters
EXIT_CLUSTER_START_MS   = 800

# Order timeouts
EXIT_ORDER_TIMEOUT_MS   = 2_500  # lifetime of cluster-exit limit & entry limit
TIME_STOP_ORDER_TIMEOUT_MS = 800 # lifetime of time-stop limit before re-price

# How far a cluster can be from the ideal TP (in ticks)
EXIT_CLUSTER_MAX_DIFF_TICKS = 197

# When to switch to time-stop mode
MAX_HOLD_MS = 60_000  # e.g., 60 seconds
```

In the Rust code these map to a config struct, e.g.:

```rust
struct StrategyConfig {
    maker_fee: Decimal,                  // FEE_RATE_MAKER
    taker_fee: Decimal,                  // FEE_RATE_TAKER
    broker_fee_abs: Decimal,             // BROKER_FEE_ABS
    tp_ticks: Decimal,                   // TP_TICKS

    entry_cluster_q: f64,                // ENTRY_CLUSTER_Q
    exit_cluster_q: f64,                 // EXIT_CLUSTER_Q
    exit_cluster_start_ms: u64,          // EXIT_CLUSTER_START_MS
    exit_cluster_max_diff_ticks: Decimal,// EXIT_CLUSTER_MAX_DIFF_TICKS

    max_hold_ms: u64,                    // MAX_HOLD_MS
    time_stop_reprice_ms: u64,           // TIME_STOP_ORDER_TIMEOUT_MS

    tick_size: Decimal,                  // TICK_SIZE
    entry_timeout_ms: u64,               // EXIT_ORDER_TIMEOUT_MS (for entry)
    exit_order_timeout_ms: u64,          // EXIT_ORDER_TIMEOUT_MS (for exit)
    max_cluster_depth_entry: usize,      // MAX_CLUSTER_DEPTH_ENTRY
    max_cluster_depth_exit: usize,       // MAX_CLUSTER_DEPTH_EXIT
}
```

---

## 3. Cluster Definition and Quantiles

### 3.1 Cluster Strength

For each side of the book (bids/asks), we compute a **cluster strength** as a simple sum of sizes over the first `K` levels:

```text
cluster_strength(side) = sum(size[i] for i in 0..K-1)
```

Where:
- For **entry** we use `K = MAX_CLUSTER_DEPTH_ENTRY` or a subset (often 5–12 levels in practice).
- For **exit** we use `K = MAX_CLUSTER_DEPTH_EXIT` similarly.

In the reference Python/Rust code we often use something like “top 5 levels”, but the exact `K` is configurable.

### 3.2 Rolling History and Quantiles

We maintain two histories:
- `bid_history`: sequence of bid cluster strengths over time.
- `ask_history`: sequence of ask cluster strengths over time.

From these histories we compute **quantiles** to determine thresholds:
- `entry_bid_threshold = quantile(bid_history, ENTRY_CLUSTER_Q)`
- `entry_ask_threshold = quantile(ask_history, ENTRY_CLUSTER_Q)`
- `exit_bid_threshold = quantile(bid_history, EXIT_CLUSTER_Q)`
- `exit_ask_threshold = quantile(ask_history, EXIT_CLUSTER_Q)`

We require a **minimum length** of history (e.g. > 50 samples) before using quantiles, to avoid noisy early signals.

Quantile implementation must be consistent between Python and Rust:
- Sort the array ascending.
- Use index `idx = round((len(data) - 1) * q)`.
- Take `sorted[idx]` as the quantile.

---

## 4. Position Model

A position is characterized by:

```text
direction: long (buy) or short (sell)
entry_time: timestamp (ms)
entry_price: price at which the entry limit is filled
target_price (tp_plain): entry_price ± TP_TICKS * TICK_SIZE
last_reprice: last time we updated time-stop limit (ms)
time_stop_mode: bool, indicates whether we entered time-stop regime
```

- For **long**: `tp_plain = entry_price + TP_TICKS * TICK_SIZE`
- For **short**: `tp_plain = entry_price - TP_TICKS * TICK_SIZE`

We assume **only one position at a time** (no scaling in/out).

---

## 5. Entry Logic (Limit Orders at Best Prices)

### 5.1 Signal Conditions

At each orderbook snapshot:

1. Compute current cluster strengths:
   - `cluster_bid`, `cluster_ask`.
2. Compute best prices:
   - `best_bid = bids[0].price`
   - `best_ask = asks[0].price`
3. If we already have a position → **skip entry logic** (we only manage exits).
4. Check that history is warmed up (e.g., `len(bid_history) >= 50` and `len(ask_history) >= 50`).
5. Compute thresholds:
   - `entry_bid_threshold  = quantile(bid_history, ENTRY_CLUSTER_Q)`
   - `entry_ask_threshold  = quantile(ask_history, ENTRY_CLUSTER_Q)`

Then:

- **Long entry signal** if:
  - `cluster_bid >= entry_bid_threshold`
  - `cluster_bid > cluster_ask` (bid side is stronger)
- **Short entry signal** if:
  - `cluster_ask >= entry_ask_threshold`
  - `cluster_ask > cluster_bid` (ask side is stronger)

### 5.2 Entry Execution (Limit + Timeout)

When a signal is fired:

1. We place a **limit order** at **best bid** (for long) or **best ask** (for short).
2. Entry is considered **maker liquidity**.
3. The order has a lifetime of `entry_timeout_ms` milliseconds (in v8: same as `EXIT_ORDER_TIMEOUT_MS`, often 2500 ms).
4. Fill simulation:
   - We simulate fill when:
     - For **long**: trades or orderbook best ask moves **down to** our limit price (or crosses it).
     - For **short**: best bid moves **up to** our limit price (or crosses it).
   - In practice, for the prototype we often approximate: if at any subsequent OB snapshot within timeout `best_ask <= entry_price` (for long) or `best_bid >= entry_price` (for short), we treat it as filled.
5. If not filled within `entry_timeout_ms`, we **cancel** the order and ignore that signal.

Once filled:
- We create a `Position` with:
  - `entry_time` = fill timestamp.
  - `entry_price` = limit price (best bid / best ask at entry time).
  - `target_price (tp_plain)` = entry price ± `TP_TICKS * TICK_SIZE`.
  - `time_stop_mode = False`.

---

## 6. Exit Logic — Cluster-Based Limit Exit

Exits are split into two modes:

1. **Cluster Exit (main path)** — limit order at “whale” cluster near TP.
2. **Time-Stop Mode** — activated after we’ve held the position too long or price didn’t cooperate.

### 6.1 Cluster Exit Conditions

While holding a position, at each orderbook snapshot `ts`:

1. Compute `elapsed = ts - position.entry_time`.
2. Compute current best bid/ask: `best_bid`, `best_ask`.
3. Compute whether the **ideal TP zone is hit** by mid/market:

   - For **long**:
     ```text
     target_hit = best_ask >= tp_plain
     ```
   - For **short**:
     ```text
     target_hit = best_bid <= tp_plain
     ```

4. If `elapsed < EXIT_CLUSTER_START_MS` → do *not* look for exit cluster yet.
5. Otherwise, determine the relevant side and cluster history:

   - For **long exit**: we look at **asks**:
     - `exit_cluster = cluster_ask`.
     - `history = ask_history`.
   - For **short exit**: we look at **bids**:
     - `exit_cluster = cluster_bid`.
     - `history = bid_history`.

6. If `len(history) < some_min_len` (e.g., 50) → skip cluster exit for now.

7. Compute exit threshold:

   ```text
   exit_threshold = quantile(history, EXIT_CLUSTER_Q)
   ```

8. Compute price distance between `tp_plain` and the current best price on the exit side, in ticks:

   ```text
   cluster_price = best_ask (for long) or best_bid (for short)
   diff_ticks = abs(cluster_price - tp_plain) / TICK_SIZE
   within_ticks = diff_ticks <= EXIT_CLUSTER_MAX_DIFF_TICKS
   ```

9. Condition to place/keep a cluster exit:

   ```text
   if target_hit and exit_cluster >= exit_threshold and within_ticks:
       place/refresh exit limit order
   ```

### 6.2 Exit Limit Order at Cluster

When cluster exit conditions are satisfied:

- For **long**:
  - Exit limit price typically equals `best_bid` or `cluster_price` (one tick “in front” of the whale cluster, depending on implementation detail). In v8 we often use the **current best price** on the opposite side.
- For **short**:
  - Exit limit price equals `best_ask` or `cluster_price` accordingly.

Order properties:

- Liquidity type: **maker** (we assume we provide liquidity at that level).
- Order lifetime: `EXIT_ORDER_TIMEOUT_MS` (e.g. 2500 ms).
- If not filled within the timeout, we **cancel** it and can place a new cluster exit when conditions next hold.

Fill simulation:

- As with entry, we consider the order filled when prices move through or to our limit price (on the opposite side of the book). The timing is determined by the orderbook/trade events in the backtest.

When filled, we record:
- `ExitReason = "cluster_exit"`
- `exit_liquidity = "maker"`
- Fees: **maker** on exit side.

---

## 7. Time-Stop Mode

If cluster exit doesn’t occur fast enough, we switch to a **time-based exit regime** that uses a **rolling limit order** at favorable prices.

### 7.1 Entering Time-Stop Mode

At each orderbook snapshot while in position:

1. Compute `elapsed = ts - entry_time`.
2. If `elapsed >= MAX_HOLD_MS` → we **force** time-stop mode.
3. In v8 there is an additional condition that can put us into `time_stop_mode` earlier, such as “price already hit TP zone but we didn’t get cluster exit in time”; but the core rule is:
   - **After `MAX_HOLD_MS`** we definitely enter time-stop.

When entering time-stop mode:

- Set `time_stop_mode = True`.
- Set `last_reprice = ts`.
- Place an initial **time-stop limit order**.

### 7.2 Time-Stop Limit Mechanics

For **time-stop exits**, we use a limit order that we periodically re-price:

- For **long**:
  - Place a limit **sell** near the **best bid** (exit as maker if price bounces up).
- For **short**:
  - Place a limit **buy** near the **best ask**.

The rules:

1. A time-stop limit has lifetime `TIME_STOP_ORDER_TIMEOUT_MS` (e.g. 800 ms).
2. On every orderbook snapshot:
   - If `ts - last_reprice >= TIME_STOP_ORDER_TIMEOUT_MS`, we **cancel** the old time-stop limit and **re-price** at the current best price on the relevant side.
3. Fill simulation:
   - If the market moves at least **1 tick in our favor** past the limit price, we consider the order filled.

In code terms, a typical condition for a favorable move could be:

- For **long time-stop**:
  ```text
  favourable_move = best_bid >= entry_price + TICK_SIZE
  ```

- For **short time-stop**:
  ```text
  favourable_move = best_ask <= entry_price - TICK_SIZE
  ```

If filled, we record:
- `ExitReason = "time_stop_limit"` (or equivalent label).
- `exit_liquidity = "maker"` (we assume our limit rested in the book).
- Exit side fee: **maker**.

> Note: In some variants, time-stop exit might be treated as **taker** (market) instead of **maker**. The important part is to keep it consistent between Python and Rust. The v8 spec you’re matching should explicitly state which fee is used for time-stop exits.

---

## 8. Fees and Returns

### 8.1 Exchange Fees

For each side (entry or exit) we apply:

```text
exchange_fee = price * FEE_RATE_side * ORDER_SIZE
```

Where `side` is either `maker` or `taker`. In v8:

- Entry is **always maker** (limit at best bid/ask).
- Cluster exit is **maker**.
- Time-stop exit: in Python v8 spec it can be **maker or taker**, depending on the version; choose one and make it consistent across implementations.

### 8.2 Broker Fixed Fee

On each side, we also add a **fixed broker fee**:

```text
broker_fee = BROKER_FEE_ABS   # in quote currency, e.g. 1 USDT
```

Total fee per side:

```text
fee_side = exchange_fee + broker_fee
```

Total fee per trade:

```text
total_fee = entry_fee + exit_fee
```

### 8.3 PnL in Ticks and Returns

- Tick distance between two prices:

```text
ticks = abs(exit_price - entry_price) / TICK_SIZE
```

- Signed gross ticks:

  - For **long**:
    ```text
    gross_ticks = (exit_price - entry_price) / TICK_SIZE
    ```
  - For **short**:
    ```text
    gross_ticks = (entry_price - exit_price) / TICK_SIZE
    ```

- Net ticks:
  
```text
net_ticks = gross_ticks - total_fee / TICK_SIZE
```

- Gross return (relative):

```text
gross_ret = (exit_price - entry_price) / entry_price  * direction_sign
```

Where `direction_sign = +1` for long, `-1` for short.

- Net return:

```text
net_ret = gross_ret - total_fee / entry_price
```

These values are written to the trade log.

---

## 9. Trade Log Schema (CSV)

Each filled position (trade) is logged as one row with the following columns:

```text
entry_time           # timestamp (human-readable or ms, but consistent)
exit_time            # timestamp
direction            # "long" or "short" (or "buy"/"sell")
side                 # +1 for long, -1 for short
entry_price
exit_price
gross_ticks
net_ticks
gross_ret
net_ret
reason               # "cluster_exit" or "time_stop_limit"
exit_liquidity       # "maker" or "taker"
tp_plain             # ideal TP level from entry
exit_from_cluster    # bool, True if exit is via cluster logic
entry_fee            # absolute fee paid on entry
exit_fee             # absolute fee paid on exit
total_fee            # entry_fee + exit_fee
```

In Rust, this corresponds to a struct like:

```rust
struct TradeLog {
    entry_time: u64,
    exit_time: u64,
    direction: Side,          // Buy / Sell (or Long / Short)
    entry_price: Decimal,
    exit_price: Decimal,
    gross_ticks: Decimal,
    net_ticks: Decimal,
    gross_ret: Decimal,
    net_ret: Decimal,
    reason: ExitReason,       // Cluster / TimeStop
    exit_liquidity: &'static str, // "maker" / "taker"
    tp_plain: Decimal,
    exit_from_cluster: bool,
    entry_fee: Decimal,
    exit_fee: Decimal,
    total_fee: Decimal,
}
```

CSV writer converts enums to lower‑case strings, etc.

---

## 10. Implementation Notes and Gotchas

1. **Warm-Up Period**  
   - Both entry and exit logic rely on quantiles of `bid_history` and `ask_history`.
   - Must enforce a **minimum length** (e.g. 50 samples) before quantiles become valid; otherwise early behaviour between Python and Rust will diverge.

2. **Quantile Calculation**  
   - Use the same sorting and indexing scheme in both implementations:
     - sort ascending,
     - `idx = round((len(data) - 1) * q)`,
     - return `sorted[idx]`.

3. **Time Units**  
   - Make sure all timestamps are consistently in **milliseconds**.
   - `elapsed`, `time_stop_reprice_ms`, `EXIT_CLUSTER_START_MS`, `MAX_HOLD_MS` all operate on ms.

4. **Single Position Only**  
   - v8 assumes at most **one open position** at a time.
   - Do not open a new position until the current one is fully closed.

5. **Entry and Exit Timeouts**  
   - Entry limit and cluster-exit limit use the same timeout in v8 (`EXIT_ORDER_TIMEOUT_MS`).
   - Time‑stop limit uses its own shorter timeout (`TIME_STOP_ORDER_TIMEOUT_MS`) for more frequent re-pricing.

6. **Matching Python vs Rust**  
   - To validate your implementation, compare:
     - number of trades,
     - average net_ticks,
     - PnL distribution by direction,
     - and per‑trade logs for a short period (e.g. 1 hour).
   - Small differences (1–2 ticks in price, a few ms in timestamps) are acceptable. Larger discrepancies often stem from:
     - different warm‑up behavior,
     - slightly different quantile implementation,
     - or different assumptions on when a limit order is considered filled.

---

## 11. Summary

The **v8 strategy** is a realistic orderbook-driven scalper that:

- Enters with **limit orders at best bid/ask** when a strong liquidity cluster appears.
- Exits preferentially with **limit orders at “whale” clusters** near the ideal TP.
- Falls back to a **time-based limit exit** if the position lives too long or the cluster exit is not achievable.
- Applies both **exchange percentage fees** and a **fixed broker fee** per side.
- Produces a detailed **trade log** enabling 1‑to‑1 comparison between different implementations (Python, Rust, etc.).

Re-implementing v8 in another language is mainly about:
- faithfully reproducing the **cluster/quantile logic**,
- respecting **timeouts and modes** (entry, cluster exit, time‑stop),
- and reproducing the **fee and PnL math** exactly.
