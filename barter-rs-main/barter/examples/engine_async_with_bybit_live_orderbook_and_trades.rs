use barter::{
    EngineEvent,
    engine::{
        clock::LiveClock,
        state::{
            global::DefaultGlobalData,
            instrument::{data::DefaultInstrumentMarketData, filter::InstrumentFilter},
            trading::TradingState,
        },
    },
    logging::init_logging,
    risk::DefaultRiskManager,
    strategy::DefaultStrategy,
    system::{
        builder::{AuditMode, EngineFeedMode, SystemArgs, SystemBuilder},
        config::{InstrumentConfig, SystemConfig},
    },
};
use barter_data::{
    streams::{
        builder::dynamic::indexed::init_indexed_multi_exchange_market_stream,
        reconnect::Event as StreamEvent,
    },
    subscription::SubKind,
};
use barter_instrument::{
    Underlying,
    asset::name::AssetNameExchange,
    exchange::ExchangeId,
    index::IndexedInstruments,
    instrument::{
        kind::{InstrumentKind, perpetual::PerpetualContract},
        quote::InstrumentQuoteAsset,
    },
};
use futures::StreamExt;
use rust_decimal_macros::dec;
use std::time::Duration;
use tracing::{info, warn};

const BYBIT_MARKET: &str = "BTCUSDT";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();

    // Configure the system with a single Bybit perpetual instrument and no execution layer so we
    // can observe raw market data without placing orders.
    let SystemConfig {
        instruments,
        executions,
    } = system_config();
    let instruments = IndexedInstruments::new(instruments);

    // Build an indexed MarketStream combining Bybit public trades & L2 order book deltas.
    let market_stream = init_indexed_multi_exchange_market_stream(
        &instruments,
        &[SubKind::PublicTrades, SubKind::OrderBooksL2],
    )
    .await?
    .inspect(|event| match event {
        StreamEvent::Reconnecting(exchange) => warn!(%exchange, "market stream reconnecting"),
        StreamEvent::Item(event) => info!(
            exchange = %event.exchange,
            instrument = %event.instrument,
            kind = event.kind.kind_name(),
            "forwarding event to Engine"
        ),
    });

    // Construct SystemArgs for the Engine running with live market data input.
    let args = SystemArgs::new(
        &instruments,
        executions,
        LiveClock,
        DefaultStrategy::default(),
        DefaultRiskManager::default(),
        market_stream,
        DefaultGlobalData::default(),
        |_| DefaultInstrumentMarketData::default(),
    );

    // Build & start the system using an async Engine feed.
    let system = SystemBuilder::new(args)
        .engine_feed_mode(EngineFeedMode::Stream)
        .audit_mode(AuditMode::Disabled)
        .trading_state(TradingState::Disabled)
        .build::<EngineEvent, _>()?
        .init_with_runtime(tokio::runtime::Handle::current())
        .await?;

    // Run the Engine briefly then perform a graceful shutdown.
    tokio::time::sleep(Duration::from_secs(15)).await;
    system.cancel_orders(InstrumentFilter::None);
    system.close_positions(InstrumentFilter::None);
    let _ = system.shutdown().await?;

    Ok(())
}

fn system_config() -> SystemConfig {
    let instrument = InstrumentConfig {
        exchange: ExchangeId::BybitPerpetualsUsd,
        name_exchange: BYBIT_MARKET.into(),
        underlying: Underlying::new("btc", "usdt"),
        quote: InstrumentQuoteAsset::UnderlyingQuote,
        kind: InstrumentKind::Perpetual(PerpetualContract {
            contract_size: dec!(1),
            settlement_asset: AssetNameExchange::from("usdt"),
        }),
        spec: None,
    };

    SystemConfig {
        instruments: vec![instrument],
        executions: vec![],
    }
}