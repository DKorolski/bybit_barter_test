//! Example back-test for a simple orderbook-driven scalping strategy.
//!
//! This example mirrors the Python "v8" prototype described in the task:
//! - Limit-maker entry with a simple queue model.
//! - Quantile-based cluster detection for entries and exits.
//! - Time-stop exits via re-priced maker limits.
//! - Exchange maker/taker fees plus a fixed broker fee per side.
//!
//! **Important:** this example is intentionally self contained and aimed at
//! illustrating the strategy mechanics rather than maximising performance.
//! For production usage prefer streaming the data and instrument specific
//! configuration via the engine builder APIs demonstrated in other examples.

use barter::logging::init_logging;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;
use std::{cmp::Ordering, error::Error, path::Path, str::FromStr, time::Instant};
use tokio::{
    fs::File,
    io::{self, AsyncBufReadExt, BufReader},
};
use tracing::{info, warn};

const ORDERBOOK_PATH: &str = "barter/examples/data/2025-11-16_BTCUSDT_ob200.data";
const TRADES_PATH: &str = "barter/examples/data/BTCUSDT2025-11-16.csv";
const OUTPUT_PATH: &str = "barter/examples/data/scalping_backtest.csv";

const MIN_HISTORY: usize = 50;
const ORDER_SIZE: Decimal = dec!(1);

/// Core configuration for the strategy. Values mirror the Python reference
/// with small rounding allowances for decimal arithmetic.
#[derive(Debug, Clone, Copy)]
struct StrategyConfig {
    maker_fee: Decimal,
    taker_fee: Decimal,
    broker_fee_abs: Decimal,
    tp_ticks: Decimal,
    entry_cluster_q: f64,
    exit_cluster_q: f64,
    exit_cluster_start_ms: u64,
    exit_cluster_max_diff_ticks: Decimal,
    max_hold_ms: u64,
    time_stop_reprice_ms: u64,
    tick_size: Decimal,
    entry_timeout_ms: u64,
    exit_order_timeout_ms: u64,
    max_cluster_depth_entry: usize,
    max_cluster_depth_exit: usize,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            maker_fee: dec!(0.0),
            taker_fee: dec!(0.0003),
            broker_fee_abs: dec!(1.0),
            tp_ticks: dec!(200),
            entry_cluster_q: 0.895,
            exit_cluster_q: 0.3,
            exit_cluster_start_ms: 800,
            exit_cluster_max_diff_ticks: dec!(197),
            max_hold_ms: 60_000,
            time_stop_reprice_ms: 800,
            tick_size: dec!(0.1),
            entry_timeout_ms: 2500,
            exit_order_timeout_ms: 2_500,
            max_cluster_depth_entry: 12,
            max_cluster_depth_exit: 12,
        }
    }
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Side {
    #[serde(alias = "Buy", alias = "BUY")]
    Buy,
    #[serde(alias = "Sell", alias = "SELL")]
    Sell,
}

impl Side {
    fn direction(self) -> i32 {
        match self {
            Side::Buy => 1,
            Side::Sell => -1,
        }
    }

    fn opposite(self) -> Self {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Level {
    price: Decimal,
    size: Decimal,
}

#[derive(Debug, Deserialize, Clone)]
struct OrderbookMessage {
    ts: u64,
    #[serde(default)]
    data: OrderbookData,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct OrderbookData {
    #[serde(default, deserialize_with = "deserialize_levels")]
    b: Vec<Level>,
    #[serde(default, deserialize_with = "deserialize_levels")]
    a: Vec<Level>,
}

#[derive(Debug, Deserialize)]
struct TradeRow {
    timestamp: f64,
    #[serde(rename = "side")]
    trade_side: Side,
    #[serde(deserialize_with = "decimal_from_string")]
    size: Decimal,
    #[serde(deserialize_with = "decimal_from_string")]
    price: Decimal,
}

#[derive(Debug, Clone)]
enum MarketEvent {
    Orderbook(OrderbookMessage),
    Trade(TradeEvent),
}

#[derive(Debug, Clone)]
struct TradeEvent {
    ts: u64,
    side: Side,
    price: Decimal,
    size: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct PendingOrder {
    side: Side,
    price: Decimal,
    placed_ts: u64,
    queue_ahead: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct Position {
    direction: Side,
    entry_time: u64,
    entry_price: Decimal,
    tp_plain: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExitOrderKind {
    Cluster,
    TimeStop,
}

#[derive(Debug, Clone, Copy)]
struct ExitOrder {
    kind: ExitOrderKind,
    price: Decimal,
    placed_ts: u64,
    queue_ahead: Decimal,
}

#[derive(Debug, Clone)]
enum TradeState {
    Flat,
    WaitingEntry(PendingOrder),
    InPosition {
        position: Position,
        exit_order: Option<ExitOrder>,
    },
}

#[derive(Debug, Clone, Copy)]
struct ClusterSideInfo {
    max_price: Decimal,
    max_size: Decimal,
    max_depth: usize,
    best_price: Decimal,
    best_size: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct ClusterInfo {
    bid: Option<ClusterSideInfo>,
    ask: Option<ClusterSideInfo>,
}

#[derive(Debug, Clone, Copy)]
enum ExitReason {
    Cluster,
    TimeStopLimit,
    EodForce,
}

#[derive(Debug, Clone)]
struct TradeLog {
    entry_time: u64,
    exit_time: u64,
    direction: Side,
    entry_price: Decimal,
    exit_price: Decimal,
    gross_ticks: Decimal,
    net_ticks: Decimal,
    gross_ret: Decimal,
    net_ret: Decimal,
    reason: ExitReason,
    exit_liquidity: &'static str,
    tp_plain: Decimal,
    exit_from_cluster: bool,
    entry_fee: Decimal,
    exit_fee: Decimal,
    total_fee: Decimal,
    hold_ms: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    init_logging();

    let config = StrategyConfig::default();
    info!(?config, "running scalping back-test");

    let (orderbooks, trades) =
        tokio::join!(load_orderbooks(ORDERBOOK_PATH), load_trades(TRADES_PATH),);

    let mut orderbooks = orderbooks?;
    let mut trades = trades?;

    orderbooks.sort_by_key(|o| o.ts);
    trades.sort_by_key(|t| t.ts);

    let mut events = merge_events(orderbooks, trades);
    events.sort_by(|a, b| match (a, b) {
        (MarketEvent::Orderbook(a), MarketEvent::Orderbook(b)) => a.ts.cmp(&b.ts),
        (MarketEvent::Trade(a), MarketEvent::Trade(b)) => a.ts.cmp(&b.ts),
        (MarketEvent::Orderbook(a), MarketEvent::Trade(b)) => a.ts.cmp(&b.ts),
        (MarketEvent::Trade(a), MarketEvent::Orderbook(b)) => a.ts.cmp(&b.ts),
    });

    let mut ctx = StrategyContext::new(config);

    let started = Instant::now();
    for event in events {
        ctx.on_event(event);
    }

    ctx.finalize();

    write_results(&ctx.trades).await?;
    info!(
        elapsed_ms = started.elapsed().as_millis(),
        count = ctx.trades.len(),
        "back-test finished"
    );
    Ok(())
}

struct StrategyContext {
    config: StrategyConfig,
    bid_history_entry: Vec<f64>,
    ask_history_entry: Vec<f64>,
    bid_history_exit: Vec<f64>,
    ask_history_exit: Vec<f64>,
    state: TradeState,
    last_best_bid: Option<Level>,
    last_best_ask: Option<Level>,
    last_trade_price: Option<Decimal>,
    last_ts: Option<u64>,
    trades: Vec<TradeLog>,
}

impl StrategyContext {
    fn new(config: StrategyConfig) -> Self {
        Self {
            config,
            bid_history_entry: Vec::with_capacity(2048),
            ask_history_entry: Vec::with_capacity(2048),
            bid_history_exit: Vec::with_capacity(2048),
            ask_history_exit: Vec::with_capacity(2048),
            state: TradeState::Flat,
            last_best_bid: None,
            last_best_ask: None,
            last_trade_price: None,
            last_ts: None,
            trades: Vec::new(),
        }
    }

    fn on_event(&mut self, event: MarketEvent) {
        match event {
            MarketEvent::Orderbook(mut ob) => {
                self.last_ts = Some(ob.ts);
                sort_levels(&mut ob.data);
                self.on_orderbook(ob);
            }
            MarketEvent::Trade(trade) => {
                self.last_ts = Some(trade.ts);
                self.on_trade(trade)
            }
        }
    }

    fn on_orderbook(&mut self, ob: OrderbookMessage) {
        let entry_clusters = compute_cluster_info(&ob.data, self.config.max_cluster_depth_entry);
        let exit_clusters = compute_cluster_info(&ob.data, self.config.max_cluster_depth_exit);

        if let Some(bid) = entry_clusters.bid {
            self.last_best_bid = Some(Level {
                price: bid.best_price,
                size: bid.best_size,
            });
            self.bid_history_entry.push(bid.max_size.to_f64());
        }

        if let Some(ask) = entry_clusters.ask {
            self.last_best_ask = Some(Level {
                price: ask.best_price,
                size: ask.best_size,
            });
            self.ask_history_entry.push(ask.max_size.to_f64());
        }

        if let Some(bid) = exit_clusters.bid {
            self.bid_history_exit.push(bid.max_size.to_f64());
        }

        if let Some(ask) = exit_clusters.ask {
            self.ask_history_exit.push(ask.max_size.to_f64());
        }

        let state_snapshot = std::mem::replace(&mut self.state, TradeState::Flat);

        match state_snapshot {
            TradeState::Flat => {
                self.evaluate_entry(ob.ts, &entry_clusters);
            }
            TradeState::WaitingEntry(pending) => {
                let waited = ob.ts.saturating_sub(pending.placed_ts);
                if waited >= self.config.entry_timeout_ms {
                    info!(ts = ob.ts, "entry order timed out");
                    self.state = TradeState::Flat;
                } else {
                    self.state = TradeState::WaitingEntry(pending);
                }
            }
            TradeState::InPosition {
                mut position,
                mut exit_order,
            } => {
                self.evaluate_exit(ob.ts, &mut position, &mut exit_order, &exit_clusters);
                self.state = TradeState::InPosition {
                    position,
                    exit_order,
                };
            }
        }
    }

    fn on_trade(&mut self, trade: TradeEvent) {
        self.last_trade_price = Some(trade.price);

        let state_snapshot = std::mem::replace(&mut self.state, TradeState::Flat);

        match state_snapshot {
            TradeState::Flat => {}
            TradeState::WaitingEntry(mut pending) => {
                if self.try_fill_entry(&mut pending, &trade) {
                    let entry_price = pending.price;
                    let tp_plain = match pending.side {
                        Side::Buy => entry_price + self.config.tp_ticks * self.config.tick_size,
                        Side::Sell => entry_price - self.config.tp_ticks * self.config.tick_size,
                    };

                    let position = Position {
                        direction: pending.side,
                        entry_time: trade.ts,
                        entry_price,
                        tp_plain,
                    };

                    info!(ts = trade.ts, %entry_price, ?pending.side, "entry filled");
                    self.state = TradeState::InPosition {
                        position,
                        exit_order: None,
                    };
                } else {
                    self.state = TradeState::WaitingEntry(pending);
                }
            }
            TradeState::InPosition {
                position,
                mut exit_order,
            } => {
                if let Some(order) = exit_order.as_mut() {
                    if self.try_fill_exit(order, &position, &trade) {
                        let reason = match order.kind {
                            ExitOrderKind::Cluster => ExitReason::Cluster,
                            ExitOrderKind::TimeStop => ExitReason::TimeStopLimit,
                        };
                        self.log_close(
                            &position,
                            trade.ts,
                            order.price,
                            reason,
                            order.kind == ExitOrderKind::Cluster,
                        );
                        self.state = TradeState::Flat;
                        return;
                    }
                }

                self.state = TradeState::InPosition {
                    position,
                    exit_order,
                };
            }
        }
    }

    fn evaluate_entry(&mut self, ts: u64, clusters: &ClusterInfo) {
        let (Some(bid), Some(ask)) = (clusters.bid, clusters.ask) else {
            return;
        };

        if self.bid_history_entry.len() < MIN_HISTORY || self.ask_history_entry.len() < MIN_HISTORY
        {
            return;
        }

        let bid_thr = quantile(&self.bid_history_entry, self.config.entry_cluster_q);
        let ask_thr = quantile(&self.ask_history_entry, self.config.entry_cluster_q);

        if bid.max_size.to_f64() >= bid_thr && bid.max_depth <= self.config.max_cluster_depth_entry
        {
            let queue_ahead = (bid.best_size - ORDER_SIZE).max(Decimal::ZERO);
            self.state = TradeState::WaitingEntry(PendingOrder {
                side: Side::Buy,
                price: bid.best_price,
                placed_ts: ts,
                queue_ahead,
            });
            info!(ts, price = %bid.best_price, "pending long entry");
        } else if ask.max_size.to_f64() >= ask_thr
            && ask.max_depth <= self.config.max_cluster_depth_entry
        {
            let queue_ahead = (ask.best_size - ORDER_SIZE).max(Decimal::ZERO);
            self.state = TradeState::WaitingEntry(PendingOrder {
                side: Side::Sell,
                price: ask.best_price,
                placed_ts: ts,
                queue_ahead,
            });
            info!(ts, price = %ask.best_price, "pending short entry");
        }
    }

    fn evaluate_exit(
        &mut self,
        ts: u64,
        position: &mut Position,
        exit_order: &mut Option<ExitOrder>,
        clusters: &ClusterInfo,
    ) {
        let Some(best_bid) = self.last_best_bid else {
            return;
        };
        let Some(best_ask) = self.last_best_ask else {
            return;
        };

        // Handle exit order timeouts / repricing.
        if let Some(order) = exit_order.as_mut() {
            match order.kind {
                ExitOrderKind::Cluster => {
                    if ts.saturating_sub(order.placed_ts) >= self.config.exit_order_timeout_ms {
                        info!(ts, price = %order.price, "cluster exit expired");
                        *exit_order = None;
                    }
                }
                ExitOrderKind::TimeStop => {
                    if ts.saturating_sub(order.placed_ts) >= self.config.time_stop_reprice_ms {
                        let (price, queue) = match position.direction {
                            Side::Buy => (
                                best_ask.price,
                                (best_ask.size - ORDER_SIZE).max(Decimal::ZERO),
                            ),
                            Side::Sell => (
                                best_bid.price,
                                (best_bid.size - ORDER_SIZE).max(Decimal::ZERO),
                            ),
                        };
                        *order = ExitOrder {
                            kind: ExitOrderKind::TimeStop,
                            price,
                            placed_ts: ts,
                            queue_ahead: queue,
                        };
                        info!(ts, %price, "repriced time-stop exit");
                    }
                }
            }
        }

        let elapsed = ts.saturating_sub(position.entry_time);

        // Force transition to time-stop after max_hold_ms.
        if elapsed >= self.config.max_hold_ms {
            if exit_order.map(|o| o.kind) != Some(ExitOrderKind::TimeStop) {
                let (price, queue) = match position.direction {
                    Side::Buy => (
                        best_ask.price,
                        (best_ask.size - ORDER_SIZE).max(Decimal::ZERO),
                    ),
                    Side::Sell => (
                        best_bid.price,
                        (best_bid.size - ORDER_SIZE).max(Decimal::ZERO),
                    ),
                };
                *exit_order = Some(ExitOrder {
                    kind: ExitOrderKind::TimeStop,
                    price,
                    placed_ts: ts,
                    queue_ahead: queue,
                });
                info!(ts, %price, "entered time-stop mode");
            }
            return;
        }

        // Try to place cluster exit if allowed.
        if exit_order.is_none() && elapsed >= self.config.exit_cluster_start_ms {
            let (cluster, history, candidate_price) = match position.direction {
                Side::Buy => {
                    let Some(ask) = clusters.ask else {
                        return;
                    };
                    (
                        ask,
                        &self.ask_history_exit,
                        ask.max_price - self.config.tick_size,
                    )
                }
                Side::Sell => {
                    let Some(bid) = clusters.bid else {
                        return;
                    };
                    (
                        bid,
                        &self.bid_history_exit,
                        bid.max_price + self.config.tick_size,
                    )
                }
            };

            if history.len() >= MIN_HISTORY {
                let threshold = quantile(history, self.config.exit_cluster_q);
                let diff_ticks =
                    price_diff_in_ticks(position.tp_plain, candidate_price, self.config.tick_size);
                let within_diff = diff_ticks <= self.config.exit_cluster_max_diff_ticks;

                if cluster.max_size.to_f64() >= threshold
                    && cluster.max_depth <= self.config.max_cluster_depth_exit
                    && within_diff
                {
                    *exit_order = Some(ExitOrder {
                        kind: ExitOrderKind::Cluster,
                        price: candidate_price,
                        placed_ts: ts,
                        queue_ahead: Decimal::ZERO,
                    });
                    info!(ts, %candidate_price, "placed cluster exit");
                }
            }
        }
    }

    fn try_fill_entry(&self, pending: &mut PendingOrder, trade: &TradeEvent) -> bool {
        match pending.side {
            Side::Buy => {
                if trade.side == Side::Sell && trade.price <= pending.price {
                    pending.queue_ahead -= trade.size;
                }
            }
            Side::Sell => {
                if trade.side == Side::Buy && trade.price >= pending.price {
                    pending.queue_ahead -= trade.size;
                }
            }
        }

        pending.queue_ahead <= Decimal::ZERO
    }

    fn try_fill_exit(
        &self,
        order: &mut ExitOrder,
        position: &Position,
        trade: &TradeEvent,
    ) -> bool {
        match position.direction {
            Side::Buy => {
                if trade.side == Side::Buy && trade.price >= order.price {
                    order.queue_ahead -= trade.size;
                }
            }
            Side::Sell => {
                if trade.side == Side::Sell && trade.price <= order.price {
                    order.queue_ahead -= trade.size;
                }
            }
        }

        order.queue_ahead <= Decimal::ZERO
    }

    fn log_close(
        &mut self,
        position: &Position,
        exit_time: u64,
        exit_price: Decimal,
        reason: ExitReason,
        exit_from_cluster: bool,
    ) {
        let side_dir = Decimal::from(position.direction.direction());
        let notional_entry = position.entry_price * ORDER_SIZE;
        let gross_pnl_px = side_dir * (exit_price - position.entry_price) * ORDER_SIZE;

        let entry_fee = self.exchange_fee(position.entry_price, self.config.maker_fee);
        let exit_fee = self.exchange_fee(exit_price, self.config.maker_fee);
        let total_fee = entry_fee + exit_fee;

        let gross_ret = gross_pnl_px / notional_entry;
        let net_ret = (gross_pnl_px - total_fee) / notional_entry;

        let rel_tick = self.config.tick_size / position.entry_price;
        let gross_ticks =
            side_dir * price_diff_in_ticks(position.entry_price, exit_price, self.config.tick_size);
        let net_ticks = net_ret / rel_tick;

        let hold_ms = exit_time.saturating_sub(position.entry_time);

        self.trades.push(TradeLog {
            entry_time: position.entry_time,
            exit_time,
            direction: position.direction,
            entry_price: position.entry_price,
            exit_price,
            gross_ticks,
            net_ticks,
            gross_ret,
            net_ret,
            reason,
            exit_liquidity: "maker",
            tp_plain: position.tp_plain,
            exit_from_cluster,
            entry_fee,
            exit_fee,
            total_fee,
            hold_ms,
        });

        info!(exit_time, %exit_price, ?reason, "position closed");
    }

    fn force_close(&mut self, ts: u64) {
        let position = match &self.state {
            TradeState::InPosition { position, .. } => *position,
            TradeState::WaitingEntry(_) => {
                self.state = TradeState::Flat;
                return;
            }
            TradeState::Flat => return,
        };

        let exit_price = if let Some(price) = self.last_trade_price {
            price
        } else if let (Some(bid), Some(ask)) = (self.last_best_bid, self.last_best_ask) {
            (bid.price + ask.price) / dec!(2)
        } else {
            position.entry_price
        };

        self.log_close(&position, ts, exit_price, ExitReason::EodForce, false);

        self.state = TradeState::Flat;
    }

    fn finalize(&mut self) {
        if !matches!(self.state, TradeState::Flat) {
            warn!("forcing end-of-day exit");
            let ts = self.last_ts.unwrap_or(0);
            self.force_close(ts);
        }
    }

    fn exchange_fee(&self, price: Decimal, rate: Decimal) -> Decimal {
        (price * rate * ORDER_SIZE) + self.config.broker_fee_abs
    }
}

fn price_diff_in_ticks(a: Decimal, b: Decimal, tick: Decimal) -> Decimal {
    let diff = if a > b { a - b } else { b - a };
    diff / tick
}

fn sort_levels(data: &mut OrderbookData) {
    data.b.sort_by(|a, b| b.price.cmp(&a.price));
    data.a.sort_by(|a, b| a.price.cmp(&b.price));
}

fn compute_cluster_info(data: &OrderbookData, depth: usize) -> ClusterInfo {
    let bid = compute_side_info(&data.b, depth);
    let ask = compute_side_info(&data.a, depth);
    ClusterInfo { bid, ask }
}

fn compute_side_info(levels: &[Level], depth: usize) -> Option<ClusterSideInfo> {
    let best = levels.first()?;
    let mut max = *best;
    let mut max_depth = 0;

    for (idx, level) in levels.iter().take(depth).enumerate() {
        if level.size > max.size {
            max = *level;
            max_depth = idx;
        }
    }

    Some(ClusterSideInfo {
        max_price: max.price,
        max_size: max.size,
        max_depth,
        best_price: best.price,
        best_size: best.size,
    })
}

fn quantile(data: &[f64], q: f64) -> f64 {
    if data.is_empty() {
        return 0.0;
    }

    let mut sorted = data.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let idx = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted[idx]
}

async fn load_orderbooks(path: &str) -> io::Result<Vec<OrderbookMessage>> {
    let file = File::open(path).await?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let mut out = Vec::new();

    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<OrderbookMessage>(&line) {
            Ok(ob) => out.push(ob),
            Err(err) => warn!(%err, "failed to parse orderbook line"),
        }
    }

    Ok(out)
}

async fn write_results(trades: &[TradeLog]) -> Result<(), Box<dyn Error>> {
    let mut writer = csv::Writer::from_path(Path::new(OUTPUT_PATH))?;

    writer.write_record([
        "entry_time",
        "exit_time",
        "direction",
        "side",
        "entry_price",
        "exit_price",
        "gross_ticks",
        "net_ticks",
        "gross_ret",
        "net_ret",
        "reason",
        "exit_liquidity",
        "tp_plain",
        "exit_from_cluster",
        "entry_fee",
        "exit_fee",
        "total_fee",
        "hold_ms",
    ])?;

    for trade in trades {
        writer.write_record([
            trade.entry_time.to_string(),
            trade.exit_time.to_string(),
            format!("{:?}", trade.direction).to_lowercase(),
            trade.direction.direction().to_string(),
            trade.entry_price.to_string(),
            trade.exit_price.to_string(),
            trade.gross_ticks.to_string(),
            trade.net_ticks.to_string(),
            trade.gross_ret.to_string(),
            trade.net_ret.to_string(),
            match trade.reason {
                ExitReason::Cluster => "cluster_exit",
                ExitReason::TimeStopLimit => "time_stop_limit",
                ExitReason::EodForce => "eod_force",
            }
            .to_string(),
            trade.exit_liquidity.to_string(),
            trade.tp_plain.to_string(),
            trade.exit_from_cluster.to_string(),
            trade.entry_fee.to_string(),
            trade.exit_fee.to_string(),
            trade.total_fee.to_string(),
            trade.hold_ms.to_string(),
        ])?;
    }

    writer.flush()?;
    info!(path = OUTPUT_PATH, count = trades.len(), "wrote trade log");
    Ok(())
}

fn merge_events(orderbooks: Vec<OrderbookMessage>, trades: Vec<TradeEvent>) -> Vec<MarketEvent> {
    let mut events = Vec::with_capacity(orderbooks.len() + trades.len());
    events.extend(orderbooks.into_iter().map(MarketEvent::Orderbook));
    events.extend(trades.into_iter().map(MarketEvent::Trade));
    events
}

async fn load_trades(path: &str) -> Result<Vec<TradeEvent>, Box<dyn Error>> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(Path::new(path))?;

    let mut trades = Vec::new();
    for result in reader.deserialize::<TradeRow>() {
        let row = result?;
        let ts_ms = (row.timestamp * 1_000.0) as u64;
        trades.push(TradeEvent {
            ts: ts_ms,
            side: row.trade_side,
            price: row.price,
            size: row.size,
        });
    }

    Ok(trades)
}

fn deserialize_levels<'de, D>(deserializer: D) -> Result<Vec<Level>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw: Vec<(String, String)> = Vec::deserialize(deserializer)?;
    Ok(raw
        .into_iter()
        .filter_map(
            |(price, size)| match (parse_decimal(&price), parse_decimal(&size)) {
                (Some(price), Some(size)) if size > dec!(0) => Some(Level { price, size }),
                _ => None,
            },
        )
        .collect())
}

fn decimal_from_string<'de, D>(s: D) -> Result<Decimal, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw: String = String::deserialize(s)?;
    Decimal::from_str_exact(raw.trim())
        .map_err(|err| serde::de::Error::custom(format!("decimal parse error: {err}")))
}

fn parse_decimal(value: &str) -> Option<Decimal> {
    Decimal::from_str(value.trim()).ok()
}

trait ToF64Decimal {
    fn to_f64(&self) -> f64;
}

impl ToF64Decimal for Decimal {
    fn to_f64(&self) -> f64 {
        self.to_string().parse::<f64>().unwrap_or(0.0)
    }
}
