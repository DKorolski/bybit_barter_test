# Bybit live order book and trades example

This example demonstrates how to stream Bybit perpetual market data into the Barter engine without
placing orders. It connects to the Bybit public trades and L2 order book delta feeds for
`BTCUSDT` perpetual contracts and forwards them into the engine using an asynchronous feed.

## Key points
- Uses `tokio` to drive the runtime (`#[tokio::main]`).
- Builds a combined market stream with `init_indexed_multi_exchange_market_stream`, subscribing to
  both `PublicTrades` and `OrderBooksL2` for the configured instrument.
- Runs the engine with `EngineFeedMode::Stream` and `TradingState::Disabled`, so data is observed
  without sending any execution commands.
- After a short sleep, gracefully cancels orders (no-op) and shuts down the engine.

## Running
```bash
cargo run -p barter --example engine_async_with_bybit_live_orderbook_and_trades \
  --features "barter-data/bybit"
```

Logging is enabled by default via `init_logging()`. You can increase verbosity during development:
```bash
RUST_LOG=info,barter=info,barter_data=info \
  cargo run -p barter --example engine_async_with_bybit_live_orderbook_and_trades \
  --features "barter-data/bybit"
```

Expect to see periodic messages such as:
- successful stream initialisation for each subscription
- forwarded `public_trade` and `l2` events for the Bybit perpetual instrument
- a clean shutdown after the sleep duration