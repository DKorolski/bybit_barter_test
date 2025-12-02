use barter_data::{
    event::DataKind,
    exchange::bybit::{futures::BybitPerpetualsUsd, spot::BybitSpot},
    streams::{Streams, consumer::MarketStreamResult, reconnect::stream::ReconnectingStream},
    subscription::{book::OrderBooksL2, trade::PublicTrades},
};
use barter_instrument::instrument::market_data::{
    MarketDataInstrument, kind::MarketDataInstrumentKind,
};
use std::time::Duration;
use tokio_stream::StreamExt;
use tracing::{info, warn};

#[rustfmt::skip]
#[tokio::main]
async fn main() {
    // Initialise INFO Tracing log subscriber
    init_logging();

    // Build a combined MarketStream that fans-in Bybit public trades and L2 order book updates
    // for both Spot and USDT perpetual markets. Each subscribe() call opens a dedicated
    // WebSocket connection per subscription set.
    let streams: Streams<MarketStreamResult<MarketDataInstrument, DataKind>> = init_with_retry().await;

    // Merge both live feeds so downstream systems (eg/ Barter Engine) receive a single stream of
    // MarketEvent<DataKind> items tagged as PublicTrade or OrderBookL2 updates.
    let mut joined_stream = streams
        .select_all()
        .with_error_handler(|error| warn!(?error, "MarketStream generated error"));

    while let Some(event) = joined_stream.next().await {
        info!(?event, "received Bybit market event");
    }
}

async fn init_with_retry() -> Streams<MarketStreamResult<MarketDataInstrument, DataKind>> {
    let mut backoff = Duration::from_millis(500);

    loop {
        match build_dual_feed().await {
            Ok(streams) => return streams,
            Err(error) => {
                warn!(
                    ?error,
                    ?backoff,
                    "failed to initialise Bybit streams, retrying"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    }
}

async fn build_dual_feed() -> Result<
    Streams<MarketStreamResult<MarketDataInstrument, DataKind>>,
    barter_data::error::DataError,
> {
    Streams::builder_multi()
        // Public trades for Bybit Spot
        .add(Streams::<PublicTrades>::builder().subscribe([(
            BybitSpot::default(),
            "btc",
            "usdt",
            MarketDataInstrumentKind::Spot,
            PublicTrades,
        )]))
        // Public trades for Bybit USDT perpetuals
        .add(Streams::<PublicTrades>::builder().subscribe([(
            BybitPerpetualsUsd::default(),
            "btc",
            "usdt",
            MarketDataInstrumentKind::Perpetual,
            PublicTrades,
        )]))
        // Order book L2 (depth) for Bybit Spot
        .add(Streams::<OrderBooksL2>::builder().subscribe([(
            BybitSpot::default(),
            "btc",
            "usdt",
            MarketDataInstrumentKind::Spot,
            OrderBooksL2,
        )]))
        // Order book L2 (depth) for Bybit USDT perpetuals
        .add(Streams::<OrderBooksL2>::builder().subscribe([(
            BybitPerpetualsUsd::default(),
            "btc",
            "usdt",
            MarketDataInstrumentKind::Perpetual,
            OrderBooksL2,
        )]))
        .init()
        .await
}

// Initialise an INFO `Subscriber` for `Tracing` Json logs and install it as the global default.
fn init_logging() {
    tracing_subscriber::fmt()
        // Filter messages based on the INFO
        .with_env_filter(
            tracing_subscriber::filter::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        // Disable colours on release builds
        .with_ansi(cfg!(debug_assertions))
        // Enable Json formatting
        .json()
        // Install this Tracing subscriber as global default
        .init()
}