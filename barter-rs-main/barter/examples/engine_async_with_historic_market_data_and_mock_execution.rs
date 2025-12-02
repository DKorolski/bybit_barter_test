use barter::{
    EngineEvent,
    engine::{
        clock::HistoricalClock,
        state::{
            global::DefaultGlobalData,
            instrument::{data::DefaultInstrumentMarketData, filter::InstrumentFilter},
            trading::TradingState,
        },
    },
    logging::init_logging,
    risk::DefaultRiskManager,
    statistic::time::Daily,
    strategy::DefaultStrategy,
    system::{
        builder::{AuditMode, EngineFeedMode, SystemArgs, SystemBuilder},
        config::SystemConfig,
    },
};
use barter_data::{
    books::{Level, OrderBook},
    event::{DataKind, MarketEvent},
    exchange::bybit::{book::BybitOrderBookInner, message::BybitPayloadKind},
    streams::{
        consumer::{MarketStreamEvent, MarketStreamResult},
        reconnect::{Event, stream::ReconnectingStream},
    },
    subscription::{book::OrderBookEvent, trade::PublicTrade},
};
use barter_instrument::{
    Side, exchange::ExchangeId, index::IndexedInstruments, instrument::InstrumentIndex,
};
use barter_integration::de::de_u64_epoch_ms_as_datetime_utc;
use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use futures::{Stream, StreamExt, stream};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use std::{
    fs::{File, create_dir_all},
    io::{BufRead, BufReader, Write as IoWrite},
    time::Duration,
};
use tracing::{info, warn};

// ----- Пути к файлам -----

const FILE_PATH_SYSTEM_CONFIG: &str = "barter/examples/config/system_config.json";
const FILE_PATH_TRADES: &str = "barter/examples/data/BTCUSDT2025-11-16.csv";
const FILE_PATH_ORDERBOOK: &str = "barter/examples/data/2025-11-16_BTCUSDT_ob200.data";
const DOM_REPORT_PATH: &str = "barter/examples/data/dom_backtest1.txt";

// ----- Параметры риска (для встроенного tear-sheet движка Barter, не DOM-логики) -----

const RISK_FREE_RETURN: Decimal = dec!(0.05);

// ----- Параметры DOM-скальпинга -----

const MAX_LEVELS: usize = 20;
const TICK_SIZE: f64 = 0.1; // BTCUSDT perp
const ORDER_SIZE: f64 = 1.0;
const ENTRY_TIMEOUT_MS: i64 = 1_000;
const MAX_HOLD_MS: i64 = 30_000;
const TAKE_PROFIT_TICKS: i64 = 10;
const MAX_CLUSTER_DEPTH: usize = 10;
const WHALE_THRESHOLD: f64 = 12.0;
const WHALE_MULTIPLIER: f64 = 3.0;
const FEE_RATE_MAKER: f64 = 0.0001 / 100.0;

// ограничение на количество строк OB (как ROWS_TO_PROCESS в Python)
const ROWS_TO_PROCESS: Option<usize> = Some(800_000);

#[derive(Debug, Clone)]
struct DomConfig {
    price_tick: f64,
    order_size: f64,
    entry_timeout: ChronoDuration,
    max_hold: ChronoDuration,
    take_profit_ticks: i64,
    max_levels: usize,
    cluster_depth: usize,
    whale_threshold: f64,
    whale_multiplier: f64,
    fee_rate_maker: f64,
}

#[derive(Debug)]
struct PendingEntry {
    side: Side,
    price: f64,
    placed_at: DateTime<Utc>,
    size_remaining: f64,
}

#[derive(Debug)]
struct OpenPosition {
    side: Side,
    entry_price: f64,
    tp_price: f64,
    opened_at: DateTime<Utc>,
    tp_remaining: f64,
}

#[derive(Debug)]
enum ExitReason {
    TakeProfit,
    HoldTimeout,
}

#[derive(Debug)]
struct TradeResult {
    side: Side,
    entry_time: DateTime<Utc>,
    exit_time: DateTime<Utc>,
    entry_price: f64,
    exit_price: f64,
    pnl_net_price: f64,
    pnl_net_ticks: f64,
    reason: ExitReason,
}

#[derive(Debug)]
struct WhaleSignal {
    side: Side,
    whale_price: f64,
    whale_volume: f64,
    entry_price: f64,
    tp_price: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();

    let SystemConfig {
        instruments,
        executions,
    } = load_config()?;
    let instruments = IndexedInstruments::new(instruments);

    let (clock, market_stream) = init_bybit_historic_clock_and_market_stream();

    let args = SystemArgs::new(
        &instruments,
        executions,
        clock,
        DefaultStrategy::default(),
        DefaultRiskManager::default(),
        market_stream,
        DefaultGlobalData::default(),
        |_| DefaultInstrumentMarketData::default(),
    );

    let system = SystemBuilder::new(args)
        .engine_feed_mode(EngineFeedMode::Stream)
        .audit_mode(AuditMode::Disabled)
        .trading_state(TradingState::Enabled)
        .build::<EngineEvent, _>()?
        .init_with_runtime(tokio::runtime::Handle::current())
        .await?;

    tokio::time::sleep(Duration::from_secs(5)).await;

    system.cancel_orders(InstrumentFilter::None);
    system.close_positions(InstrumentFilter::None);

    let (engine, _shutdown_audit) = system.shutdown().await?;

    let trading_summary = engine
        .trading_summary_generator(RISK_FREE_RETURN)
        .generate(Daily);

    trading_summary.print_summary();

    Ok(())
}

fn load_config() -> Result<SystemConfig, Box<dyn std::error::Error>> {
    let file = File::open(FILE_PATH_SYSTEM_CONFIG)?;
    let reader = BufReader::new(file);
    let config: SystemConfig = serde_json::from_reader(reader)?;
    Ok(config)
}

// ======================================================================
// DOM whale scalping: поиск сигналов и симуляция очереди с MarketStream
// ======================================================================

fn detect_whale_signal(book: &OrderBook, cfg: &DomConfig, last_price: f64) -> Option<WhaleSignal> {
    let bids = book.bids().levels();
    let asks = book.asks().levels();

    if bids.is_empty() || asks.is_empty() {
        return None;
    }

    let depth = cfg
        .cluster_depth
        .min(cfg.max_levels)
        .min(bids.len())
        .min(asks.len());

    if depth == 0 {
        return None;
    }

    let avg_bid = average_volume(&bids[..depth]);
    let avg_ask = average_volume(&asks[..depth]);

    let best_bid = bids[0].price.to_f64().unwrap_or(last_price);
    let best_ask = asks[0].price.to_f64().unwrap_or(last_price);

    let mut best_long: Option<WhaleSignal> = None;
    let mut best_short: Option<WhaleSignal> = None;

    for lvl in &bids[..depth] {
        let vol = lvl.amount.to_f64().unwrap_or(0.0);
        if vol < cfg.whale_threshold || vol < avg_bid * cfg.whale_multiplier {
            continue;
        }

        let whale_price = lvl.price.to_f64().unwrap_or(best_bid);
        if (last_price - whale_price).abs() > cfg.price_tick * cfg.cluster_depth as f64 {
            continue;
        }

        let entry_price = best_bid + cfg.price_tick;
        let tp_price = entry_price + cfg.price_tick * cfg.take_profit_ticks as f64;

        let candidate = WhaleSignal {
            side: Side::Buy,
            whale_price,
            whale_volume: vol,
            entry_price,
            tp_price,
        };

        best_long = select_better(best_long, candidate);
    }

    for lvl in &asks[..depth] {
        let vol = lvl.amount.to_f64().unwrap_or(0.0);
        if vol < cfg.whale_threshold || vol < avg_ask * cfg.whale_multiplier {
            continue;
        }

        let whale_price = lvl.price.to_f64().unwrap_or(best_ask);
        if (last_price - whale_price).abs() > cfg.price_tick * cfg.cluster_depth as f64 {
            continue;
        }

        let entry_price = best_ask - cfg.price_tick;
        let tp_price = entry_price - cfg.price_tick * cfg.take_profit_ticks as f64;

        let candidate = WhaleSignal {
            side: Side::Sell,
            whale_price,
            whale_volume: vol,
            entry_price,
            tp_price,
        };

        best_short = select_better(best_short, candidate);
    }

    match (best_long, best_short) {
        (Some(long), Some(short)) => {
            if long.whale_volume >= short.whale_volume {
                Some(long)
            } else {
                Some(short)
            }
        }
        (Some(long), None) => Some(long),
        (None, Some(short)) => Some(short),
        (None, None) => None,
    }
}

fn average_volume(levels: &[Level]) -> f64 {
    if levels.is_empty() {
        return 0.0;
    }

    let total: f64 = levels
        .iter()
        .map(|lvl| lvl.amount.to_f64().unwrap_or(0.0).max(0.0))
        .sum();

    total / levels.len() as f64
}

fn select_better(existing: Option<WhaleSignal>, candidate: WhaleSignal) -> Option<WhaleSignal> {
    match existing {
        Some(current) if current.whale_volume >= candidate.whale_volume => Some(current),
        _ => Some(candidate),
    }
}

fn trade_matches_entry(pending: &PendingEntry, trade: &PublicTrade) -> bool {
    match pending.side {
        Side::Buy => trade.side == Side::Sell && trade.price <= pending.price,
        Side::Sell => trade.side == Side::Buy && trade.price >= pending.price,
    }
}

fn trade_matches_take_profit(position: &OpenPosition, trade: &PublicTrade) -> bool {
    match position.side {
        Side::Buy => trade.side == Side::Buy && trade.price >= position.tp_price,
        Side::Sell => trade.side == Side::Sell && trade.price <= position.tp_price,
    }
}

fn run_dom_whale_scan(events: &[MarketEvent<InstrumentIndex, DataKind>]) {
    let cfg = DomConfig {
        price_tick: TICK_SIZE,
        order_size: ORDER_SIZE,
        entry_timeout: ChronoDuration::milliseconds(ENTRY_TIMEOUT_MS),
        max_hold: ChronoDuration::milliseconds(MAX_HOLD_MS),
        take_profit_ticks: TAKE_PROFIT_TICKS,
        max_levels: MAX_LEVELS,
        cluster_depth: MAX_CLUSTER_DEPTH,
        whale_threshold: WHALE_THRESHOLD,
        whale_multiplier: WHALE_MULTIPLIER,
        fee_rate_maker: FEE_RATE_MAKER,
    };

    let mut last_book: Option<OrderBook> = None;
    let mut last_trade_price: Option<f64> = None;

    let mut pending_entry: Option<PendingEntry> = None;
    let mut open_position: Option<OpenPosition> = None;
    let mut trades: Vec<TradeResult> = Vec::new();

    let mut signals = 0usize;
    let mut orders_placed = 0usize;
    let mut entry_timeouts = 0usize;

    for event in events {
        match &event.kind {
            DataKind::OrderBook(order_book_event) => {
                if let Some(entry) = pending_entry.as_ref() {
                    if event.time_exchange.signed_duration_since(entry.placed_at)
                        > cfg.entry_timeout
                    {
                        entry_timeouts += 1;
                        pending_entry = None;
                    }
                }

                if let OrderBookEvent::Snapshot(book) | OrderBookEvent::Update(book) =
                    order_book_event
                {
                    last_book = Some(book.clone());
                    if pending_entry.is_none() && open_position.is_none() {
                        if let Some(last_price) = last_trade_price {
                            if let Some(signal) = detect_whale_signal(book, &cfg, last_price) {
                                signals += 1;
                                orders_placed += 1;

                                pending_entry = Some(PendingEntry {
                                    side: signal.side,
                                    price: signal.entry_price,
                                    placed_at: event.time_exchange,
                                    size_remaining: cfg.order_size,
                                });

                                info!(
                                    ?signal.side,
                                    entry_price = signal.entry_price,
                                    tp_price = signal.tp_price,
                                    whale_price = signal.whale_price,
                                    whale_volume = signal.whale_volume,
                                    time = %event.time_exchange,
                                    "whale signal detected"
                                );
                            }
                        }
                    }
                }
            }
            DataKind::Trade(trade) => {
                last_trade_price = Some(trade.price);

                if let Some(entry) = pending_entry.as_mut() {
                    if trade_matches_entry(entry, trade) {
                        let fill = entry.size_remaining.min(trade.amount);
                        entry.size_remaining -= fill;
                        if entry.size_remaining <= f64::EPSILON {
                            let filled_side = entry.side;
                            let entry_price = entry.price;
                            let opened_at = event.time_exchange;
                            let tp_price = match filled_side {
                                Side::Buy => {
                                    entry_price + cfg.price_tick * cfg.take_profit_ticks as f64
                                }
                                Side::Sell => {
                                    entry_price - cfg.price_tick * cfg.take_profit_ticks as f64
                                }
                            };

                            open_position = Some(OpenPosition {
                                side: filled_side,
                                entry_price,
                                tp_price,
                                opened_at,
                                tp_remaining: cfg.order_size,
                            });
                            pending_entry = None;
                        }
                    } else if event.time_exchange.signed_duration_since(entry.placed_at)
                        > cfg.entry_timeout
                    {
                        entry_timeouts += 1;
                        pending_entry = None;
                    }
                }

                if let Some(position) = open_position.as_mut() {
                    if trade_matches_take_profit(position, trade) {
                        let fill = position.tp_remaining.min(trade.amount);
                        position.tp_remaining -= fill;
                        if position.tp_remaining <= f64::EPSILON {
                            let exit_time = event.time_exchange;
                            let exit_price = trade.price;
                            let entry_price = position.entry_price;
                            let side = position.side;

                            let gross = match side {
                                Side::Buy => (exit_price - entry_price) * cfg.order_size,
                                Side::Sell => (entry_price - exit_price) * cfg.order_size,
                            };

                            let fee_entry = entry_price * cfg.order_size * cfg.fee_rate_maker;
                            let fee_exit = exit_price * cfg.order_size * cfg.fee_rate_maker;
                            let pnl_net_price = gross - fee_entry - fee_exit;
                            let pnl_net_ticks = pnl_net_price / (cfg.price_tick * cfg.order_size);

                            trades.push(TradeResult {
                                side,
                                entry_time: position.opened_at,
                                exit_time,
                                entry_price,
                                exit_price,
                                pnl_net_price,
                                pnl_net_ticks,
                                reason: ExitReason::TakeProfit,
                            });

                            open_position = None;
                        }
                    }

                    if let Some(pos) = open_position.as_ref() {
                        if event.time_exchange.signed_duration_since(pos.opened_at) > cfg.max_hold {
                            let exit_price = trade.price;
                            let side = pos.side;
                            let gross = match side {
                                Side::Buy => (exit_price - pos.entry_price) * cfg.order_size,
                                Side::Sell => (pos.entry_price - exit_price) * cfg.order_size,
                            };

                            let fee_entry = pos.entry_price * cfg.order_size * cfg.fee_rate_maker;
                            let fee_exit = exit_price * cfg.order_size * cfg.fee_rate_maker;
                            let pnl_net_price = gross - fee_entry - fee_exit;
                            let pnl_net_ticks = pnl_net_price / (cfg.price_tick * cfg.order_size);

                            trades.push(TradeResult {
                                side,
                                entry_time: pos.opened_at,
                                exit_time: event.time_exchange,
                                entry_price: pos.entry_price,
                                exit_price,
                                pnl_net_price,
                                pnl_net_ticks,
                                reason: ExitReason::HoldTimeout,
                            });

                            open_position = None;
                        }
                    }
                }
            }
            _ => {}
        }
    }

    if let (Some(pos), Some(last_price)) = (open_position.take(), last_trade_price) {
        let gross = match pos.side {
            Side::Buy => (last_price - pos.entry_price) * cfg.order_size,
            Side::Sell => (pos.entry_price - last_price) * cfg.order_size,
        };

        let fee_entry = pos.entry_price * cfg.order_size * cfg.fee_rate_maker;
        let fee_exit = last_price * cfg.order_size * cfg.fee_rate_maker;
        let pnl_net_price = gross - fee_entry - fee_exit;
        let pnl_net_ticks = pnl_net_price / (cfg.price_tick * cfg.order_size);

        trades.push(TradeResult {
            side: pos.side,
            entry_time: pos.opened_at,
            exit_time: events
                .last()
                .map(|e| e.time_exchange)
                .unwrap_or(pos.opened_at),
            entry_price: pos.entry_price,
            exit_price: last_price,
            pnl_net_price,
            pnl_net_ticks,
            reason: ExitReason::HoldTimeout,
        });
    }

    write_report(trades, signals, orders_placed, entry_timeouts);
}

fn write_report(
    trades: Vec<TradeResult>,
    signals: usize,
    orders_placed: usize,
    entry_timeouts: usize,
) {
    if trades.is_empty() {
        println!("--- DOM whale scalping backtest ---");
        println!("No trades executed.");
        println!("[BT] signals=0, orders_placed=0, filled=0, order_timeouts=0");

        if let Err(e) = create_dir_all("barter/examples/output") {
            eprintln!("Failed to create output dir: {e}");
        } else if let Ok(mut f) = File::create(DOM_REPORT_PATH) {
            let _ = f.write_all(b"No trades executed.\n");
        }

        println!("DOM backtest report saved to: {DOM_REPORT_PATH}");
        return;
    }

    let mut wins = 0usize;
    let mut total_net_ticks = 0.0_f64;
    let mut tp_count = 0usize;
    let mut timeout_count = 0usize;

    for trade in &trades {
        if trade.pnl_net_ticks > 0.0 {
            wins += 1;
        }
        total_net_ticks += trade.pnl_net_ticks;

        match trade.reason {
            ExitReason::TakeProfit => tp_count += 1,
            ExitReason::HoldTimeout => timeout_count += 1,
        }
    }

    let filled = trades.len();
    let hit_rate = wins as f64 * 100.0 / filled as f64;
    let avg_net_ticks = total_net_ticks / filled as f64;

    let mut report = String::new();
    report.push_str("--- Trades detail (mini journal) ---\n");

    for (i, trade) in trades.iter().enumerate() {
        report.push_str(&format!(
            "#{:05} {:?} | entry={:.2} at {} | exit={:.2} at {} | pnl_net_price={:.6} | pnl_net_ticks={:.3} | {:?}\n",
            i + 1,
            trade.side,
            trade.entry_price,
            trade.entry_time,
            trade.exit_price,
            trade.exit_time,
            trade.pnl_net_price,
            trade.pnl_net_ticks,
            trade.reason,
        ));
    }

    report.push_str("\n===== РЕЗУЛЬТАТЫ БЭКТЕСТА DOM (net_ticks) =====\n");
    report.push_str(&format!(
        "[BT] signals={signals}, orders_placed={orders_placed}, filled={filled}, order_timeouts={entry_timeouts}\n"
    ));
    report.push_str(&format!("[BT] trades={} (filled)\n", filled));
    report.push_str(&format!("[BT] Winrate: {:.2}%\n", hit_rate));
    report.push_str(&format!(
        "[BT] Средний PnL (net_ticks): {:.3}\n",
        avg_net_ticks
    ));
    report.push_str(&format!(
        "[BT] Суммарный PnL (net_ticks): {:.3}\n",
        total_net_ticks
    ));

    report.push_str("\nПо причинам выхода:\n");
    report.push_str(&format!("tp      {:>8}\n", tp_count));
    report.push_str(&format!("timeout {:>8}\n", timeout_count));

    if let Err(e) = create_dir_all("barter/examples/output") {
        eprintln!("Failed to create output dir: {e}");
    } else if let Ok(mut f) = File::create(DOM_REPORT_PATH) {
        if let Err(e) = f.write_all(report.as_bytes()) {
            eprintln!("Failed to write report to file: {e}");
        }
    }

    println!("DOM backtest report saved to: {DOM_REPORT_PATH}");
}

// ======================================================================
// Clock + historical stream (trades + orderbook), запуск DOM-сканера
// ======================================================================

fn init_bybit_historic_clock_and_market_stream() -> (
    HistoricalClock,
    impl Stream<Item = MarketStreamEvent<InstrumentIndex, DataKind>>,
) {
    let mut events = Vec::<MarketEvent<InstrumentIndex, DataKind>>::new();

    let trade_events = load_bybit_events_from_trades(FILE_PATH_TRADES);
    let ob_events = load_bybit_orderbook_events(FILE_PATH_ORDERBOOK);

    events.extend(trade_events);
    events.extend(ob_events);

    events.sort_by_key(|event| event.time_exchange);

    let time_exchange_first = events
        .first()
        .expect("no events loaded from trades & orderbook")
        .time_exchange;

    let clock = HistoricalClock::new(time_exchange_first);

    run_dom_whale_scan(&events);

    let results: Vec<MarketStreamResult<InstrumentIndex, DataKind>> = events
        .into_iter()
        .map(|event| Event::Item(Ok(event)))
        .collect();

    let stream = stream::iter(results)
        .with_error_handler(|error| warn!(?error, "MarketStream generated error"))
        .inspect(|event| match event {
            Event::Reconnecting(exchange) => {
                info!(%exchange, "sending historical disconnection to Engine")
            }
            Event::Item(event) => {
                info!(
                    exchange = %event.exchange,
                    instrument = %event.instrument,
                    kind = event.kind.kind_name(),
                    "sending historical event to Engine",
                )
            }
        });

    (clock, stream)
}

// ======================================================================
// Загрузка orderbook из Bybit .data
// ======================================================================

#[derive(Debug, serde::Deserialize)]
struct HistoricBybitOrderBookMessage {
    #[serde(rename = "type")]
    kind: BybitPayloadKind,

    #[serde(rename = "ts", deserialize_with = "de_u64_epoch_ms_as_datetime_utc")]
    time: DateTime<Utc>,

    data: BybitOrderBookInner,
}

fn load_bybit_orderbook_events(path: &str) -> Vec<MarketEvent<InstrumentIndex, DataKind>> {
    let file = File::open(path).expect("cannot open orderbook data file");
    let reader = BufReader::new(file);

    let mut events = Vec::new();
    let mut count: usize = 0;

    for line in reader.lines() {
        if let Some(limit) = ROWS_TO_PROCESS {
            if count >= limit {
                break;
            }
        }
        count += 1;

        let line = match line {
            Ok(value) => value,
            Err(e) => {
                eprintln!("skip orderbook line (io error): {e}");
                continue;
            }
        };

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let msg: HistoricBybitOrderBookMessage = match serde_json::from_str(line) {
            Ok(message) => message,
            Err(e) => {
                eprintln!("skip orderbook line (json error): {e}");
                continue;
            }
        };

        let orderbook = OrderBook::new(
            msg.data.sequence,
            Some(msg.time),
            msg.data.bids,
            msg.data.asks,
        );

        let ob_event = match msg.kind {
            BybitPayloadKind::Snapshot => OrderBookEvent::Snapshot(orderbook),
            BybitPayloadKind::Delta => OrderBookEvent::Update(orderbook),
        };

        events.push(MarketEvent {
            time_exchange: msg.time,
            time_received: msg.time,
            exchange: ExchangeId::BybitSpot,
            instrument: InstrumentIndex(0),
            kind: DataKind::OrderBook(ob_event),
        });
    }

    events
}

// ======================================================================
// Загрузка трейдов из CSV Bybit
// ======================================================================

fn load_bybit_events_from_trades(path: &str) -> Vec<MarketEvent<InstrumentIndex, DataKind>> {
    let file = File::open(path).expect("cannot open trades csv");
    let reader = BufReader::new(file);

    let mut lines = reader.lines();

    if let Some(Ok(_header)) = lines.next() {}

    let mut events = Vec::new();

    for line in lines {
        let line = match line {
            Ok(value) => value,
            Err(e) => {
                eprintln!("skip trade line (io error): {e}");
                continue;
            }
        };

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() < 5 {
            eprintln!("skip trade line (bad format): {line}");
            continue;
        }

        let ts_str = parts[0].trim();
        let _symbol = parts[1].trim();
        let side_str = parts[2].trim();
        let size_str = parts[3].trim();
        let price_str = parts[4].trim();

        let time: DateTime<Utc> = if let Ok(ms) = ts_str.parse::<i64>() {
            let secs = ms / 1000;
            let nanos = ((ms % 1000) * 1_000_000) as u32;
            Utc.timestamp_opt(secs, nanos)
                .single()
                .unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().unwrap())
        } else if let Ok(secs_f) = ts_str.parse::<f64>() {
            let secs = secs_f.trunc() as i64;
            let nanos = ((secs_f - secs as f64) * 1_000_000_000.0).round() as u32;
            Utc.timestamp_opt(secs, nanos)
                .single()
                .unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().unwrap())
        } else if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(ts_str) {
            dt.with_timezone(&Utc)
        } else if let Ok(naive) =
            chrono::NaiveDateTime::parse_from_str(ts_str, "%Y-%m-%d %H:%M:%S%.f")
        {
            chrono::DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc)
        } else {
            eprintln!("skip trade line (bad ts string): {}", ts_str);
            continue;
        };

        let price: f64 = match price_str.parse() {
            Ok(value) => value,
            Err(e) => {
                eprintln!("skip trade line (bad price): {e} | line={line}");
                continue;
            }
        };

        let size: f64 = match size_str.parse() {
            Ok(value) => value,
            Err(e) => {
                eprintln!("skip trade line (bad size): {e} | line={line}");
                continue;
            }
        };

        let side = match side_str {
            "Buy" | "buy" | "BUY" => Side::Buy,
            "Sell" | "sell" | "SELL" => Side::Sell,
            other => {
                eprintln!("skip trade line (bad side): {other} | line={line}");
                continue;
            }
        };

        let trade_id = parts.get(6).map(|s| s.to_string()).unwrap_or_default();

        let trade = PublicTrade {
            id: trade_id,
            price,
            amount: size,
            side,
        };

        events.push(MarketEvent {
            time_exchange: time,
            time_received: time,
            exchange: ExchangeId::BybitSpot,
            instrument: InstrumentIndex(0),
            kind: DataKind::Trade(trade),
        });
    }

    events
}
