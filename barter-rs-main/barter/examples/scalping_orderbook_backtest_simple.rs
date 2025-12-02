//! Example back-test for a simple orderbook-driven scalping strategy.
//!
//! The goal is to emulate the behaviour of the referenced Python "v8"
//! prototype while staying within the Barter ecosystem. The example shows how
//! to:
//! - Parse historic orderbook (L2) snapshots and trades from disk.
//! - Build lightweight cluster metrics and quantile based entry / exit logic.
//! - Simulate limit-order based execution with re-pricing and time-stop exits.
//! - Account for both exchange maker / taker fees and fixed broker fees.
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
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            maker_fee: dec!(0.0),
            taker_fee: dec!(0.0003),
            broker_fee_abs: dec!(1.0),
            tp_ticks: dec!(200),
            entry_cluster_q: 0.9,
            exit_cluster_q: 0.3,
            exit_cluster_start_ms: 800,
            exit_cluster_max_diff_ticks: dec!(197),
            max_hold_ms: 50_000,
            time_stop_reprice_ms: 2_000,
            tick_size: dec!(0.1),
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
}

#[derive(Debug, Clone, Copy)]
struct Position {
    direction: Side,
    entry_time: u64,
    entry_price: Decimal,
    target_price: Decimal,
    last_reprice: u64,
    time_stop_mode: bool,
}

#[derive(Debug, Clone, Copy)]
enum ExitReason {
    Cluster,
    TimeStop,
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

    if let Some(position) = ctx.position.take() {
        warn!(?position, "leaving open position without exit");
    }

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
    bid_history: Vec<f64>,
    ask_history: Vec<f64>,
    best_bid: Option<Decimal>,
    best_ask: Option<Decimal>,
    position: Option<Position>,
    trades: Vec<TradeLog>,
}

impl StrategyContext {
    fn new(config: StrategyConfig) -> Self {
        Self {
            config,
            bid_history: Vec::with_capacity(1024),
            ask_history: Vec::with_capacity(1024),
            best_bid: None,
            best_ask: None,
            position: None,
            trades: Vec::new(),
        }
    }

    fn on_event(&mut self, event: MarketEvent) {
        match event {
            MarketEvent::Orderbook(ob) => self.on_orderbook(ob),
            MarketEvent::Trade(trade) => self.on_trade(trade),
        }
    }

    fn on_orderbook(&mut self, ob: OrderbookMessage) {
        let cluster_bid = cluster_strength(&ob.data.b);
        let cluster_ask = cluster_strength(&ob.data.a);

        self.best_bid = ob.data.b.first().map(|l| l.price);
        self.best_ask = ob.data.a.first().map(|l| l.price);

        if cluster_bid > dec!(0) {
            self.bid_history.push(cluster_bid.to_f64());
        }
        if cluster_ask > dec!(0) {
            self.ask_history.push(cluster_ask.to_f64());
        }

        if let Some(position) = self.position {
            self.evaluate_exit(&position, ob.ts, cluster_bid, cluster_ask);
        } else {
            self.evaluate_entry(ob.ts, cluster_bid, cluster_ask);
        }
    }

    fn on_trade(&mut self, trade: TradeEvent) {
        if let Some(mut position) = self.position {
            if position.time_stop_mode {
                if self.fill_time_stop(trade, &mut position) {
                    self.position = None;
                } else {
                    self.position = Some(position);
                }
            }
        }
    }

    fn evaluate_entry(&mut self, ts: u64, cluster_bid: Decimal, cluster_ask: Decimal) {
        let Some(best_bid) = self.best_bid else {
            return;
        };
        let Some(best_ask) = self.best_ask else {
            return;
        };

        if self.bid_history.len() < 50 || self.ask_history.len() < 50 {
            return;
        }

        let bid_threshold = quantile(&self.bid_history, self.config.entry_cluster_q);
        let ask_threshold = quantile(&self.ask_history, self.config.entry_cluster_q);

        if cluster_bid.to_f64() >= bid_threshold && cluster_bid > cluster_ask {
            self.open_position(Side::Buy, ts, best_bid);
        } else if cluster_ask.to_f64() >= ask_threshold && cluster_ask > cluster_bid {
            self.open_position(Side::Sell, ts, best_ask);
        }
    }

    fn open_position(&mut self, direction: Side, ts: u64, price: Decimal) {
        let target_price = match direction {
            Side::Buy => price + self.config.tp_ticks * self.config.tick_size,
            Side::Sell => price - self.config.tp_ticks * self.config.tick_size,
        };

        info!(ts, %price, %target_price, ?direction, "opening position");

        self.position = Some(Position {
            direction,
            entry_time: ts,
            entry_price: price,
            target_price,
            last_reprice: ts,
            time_stop_mode: false,
        });
    }

    fn evaluate_exit(
        &mut self,
        position: &Position,
        ts: u64,
        cluster_bid: Decimal,
        cluster_ask: Decimal,
    ) {
        if let Some((best_bid, best_ask)) = self.current_prices() {
            let elapsed = ts.saturating_sub(position.entry_time);
            let target_hit = match position.direction {
                Side::Buy => best_ask >= position.target_price,
                Side::Sell => best_bid <= position.target_price,
            };

            if elapsed >= self.config.exit_cluster_start_ms {
                let exit_cluster = match position.direction {
                    Side::Buy => cluster_ask,
                    Side::Sell => cluster_bid,
                };
                let history = match position.direction {
                    Side::Buy => &self.ask_history,
                    Side::Sell => &self.bid_history,
                };

                if history.len() > 50 {
                    let threshold = quantile(history, self.config.exit_cluster_q);
                    let within_ticks = price_diff_in_ticks(
                        position.target_price,
                        match position.direction {
                            Side::Buy => best_ask,
                            Side::Sell => best_bid,
                        },
                        self.config.tick_size,
                    ) <= self.config.exit_cluster_max_diff_ticks;

                    if target_hit && exit_cluster.to_f64() >= threshold && within_ticks {
                        self.close_position(
                            position,
                            ts,
                            match position.direction {
                                Side::Buy => best_ask,
                                Side::Sell => best_bid,
                            },
                            ExitReason::Cluster,
                            true,
                        );
                        self.position = None;
                        return;
                    }
                }
            }

            let exit_cluster_window = self
                .config
                .exit_cluster_start_ms
                .saturating_add(self.config.time_stop_reprice_ms);

            let max_hold_reached = elapsed >= self.config.max_hold_ms
                || (elapsed >= exit_cluster_window && target_hit);

            if max_hold_reached {
                let mut position = *position;
                if !position.time_stop_mode {
                    position.time_stop_mode = true;
                    position.last_reprice = ts;
                    info!(ts, ?position.direction, "entering time-stop mode");
                }

                if position.time_stop_mode {
                    let (price, should_reprice) = match position.direction {
                        Side::Buy => (
                            best_bid,
                            ts.saturating_sub(position.last_reprice)
                                >= self.config.time_stop_reprice_ms,
                        ),
                        Side::Sell => (
                            best_ask,
                            ts.saturating_sub(position.last_reprice)
                                >= self.config.time_stop_reprice_ms,
                        ),
                    };

                    if should_reprice {
                        position.last_reprice = ts;
                    }

                    // Fill immediately when the book has moved in our favour at least one tick.
                    let favourable_move = match position.direction {
                        Side::Buy => position.entry_price <= price - self.config.tick_size,
                        Side::Sell => position.entry_price >= price + self.config.tick_size,
                    };

                    if favourable_move {
                        self.close_position(&position, ts, price, ExitReason::TimeStop, false);
                        self.position = None;
                    } else {
                        self.position = Some(position);
                    }
                }
            }
        }
    }

    fn fill_time_stop(&mut self, trade: TradeEvent, position: &mut Position) -> bool {
        let price_crossed = match position.direction {
            Side::Buy => {
                trade.side == Side::Buy && trade.price >= self.best_ask.unwrap_or(trade.price)
            }
            Side::Sell => {
                trade.side == Side::Sell && trade.price <= self.best_bid.unwrap_or(trade.price)
            }
        };

        if price_crossed {
            let exit_price = match position.direction {
                Side::Buy => self.best_ask.unwrap_or(trade.price),
                Side::Sell => self.best_bid.unwrap_or(trade.price),
            };

            self.close_position(position, trade.ts, exit_price, ExitReason::TimeStop, false);
            true
        } else {
            false
        }
    }

    fn current_prices(&self) -> Option<(Decimal, Decimal)> {
        match (self.best_bid, self.best_ask) {
            (Some(bid), Some(ask)) => Some((bid, ask)),
            _ => None,
        }
    }

    fn close_position(
        &mut self,
        position: &Position,
        exit_time: u64,
        exit_price: Decimal,
        reason: ExitReason,
        exit_from_cluster: bool,
    ) {
        let direction = position.direction;
        let gross_ticks =
            price_diff_in_ticks(position.entry_price, exit_price, self.config.tick_size)
                * Decimal::from(direction.direction());

        let entry_fee = self.exchange_fee(position.entry_price, self.config.maker_fee);
        let exit_fee = match reason {
            ExitReason::Cluster => self.exchange_fee(exit_price, self.config.maker_fee),
            ExitReason::TimeStop => self.exchange_fee(exit_price, self.config.taker_fee),
        };
        let total_fee = entry_fee + exit_fee;

        let gross_ret = (exit_price - position.entry_price) / position.entry_price
            * Decimal::from(direction.direction());
        let net_ticks = gross_ticks - (total_fee / self.config.tick_size);
        let net_ret = gross_ret - (total_fee / position.entry_price);

        self.trades.push(TradeLog {
            entry_time: position.entry_time,
            exit_time,
            direction,
            entry_price: position.entry_price,
            exit_price,
            gross_ticks,
            net_ticks,
            gross_ret,
            net_ret,
            reason,
            exit_liquidity: match reason {
                ExitReason::Cluster => "maker",
                ExitReason::TimeStop => "taker",
            },
            tp_plain: position.target_price,
            exit_from_cluster,
            entry_fee,
            exit_fee,
            total_fee,
        });

        info!(exit_time, %exit_price, ?reason, "position closed");
    }

    fn exchange_fee(&self, price: Decimal, rate: Decimal) -> Decimal {
        (price * rate) + self.config.broker_fee_abs
    }
}

fn price_diff_in_ticks(a: Decimal, b: Decimal, tick: Decimal) -> Decimal {
    let diff = if a > b { a - b } else { b - a };
    diff / tick
}

fn cluster_strength(levels: &[Level]) -> Decimal {
    levels.iter().take(5).map(|l| l.size).sum()
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
        match serde_json::from_str::<OrderbookMessage>(&line) {
            Ok(mut msg) => {
                // Defensive: ensure bid / ask vectors are sorted by price best first.
                msg.data
                    .b
                    .sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap_or(Ordering::Equal));
                msg.data
                    .a
                    .sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(Ordering::Equal));
                out.push(msg);
            }
            Err(err) => warn!(?err, "failed to parse orderbook line"),
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
                ExitReason::TimeStop => "time_stop",
            }
            .to_string(),
            trade.exit_liquidity.to_string(),
            trade.tp_plain.to_string(),
            trade.exit_from_cluster.to_string(),
            trade.entry_fee.to_string(),
            trade.exit_fee.to_string(),
            trade.total_fee.to_string(),
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