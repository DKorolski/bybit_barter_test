# Examples Overview

This document summarizes the example programs shipped across the workspace crates. Each entry highlights the goal of the example and the main components it exercises.

## `barter-data` examples
- **dynamic_multi_stream_multi_exchange.rs** – Demonstrates `DynamicStreams` handling mixed subscription batches (different exchanges and sub-kinds) and merging them into a single stream with custom error handling.
- **multi_stream_multi_exchange.rs** – Shows how to compose multiple typed `Streams` builders (trades, L1, L2) into a unified multi-exchange market event stream.
- **indexed_market_stream.rs** – Builds indexed instruments and spins up a market stream using generated subscriptions across multiple `SubKind` variants.
- **order_books_l1_streams.rs** – Creates separate and shared WebSocket connections for L1 order books on Binance Spot, then selects a single-exchange stream with error handling.
- **order_books_l1_streams_multi_exchange.rs** – Subscribes to L1 books on Binance spot and futures, merging all exchange streams into one consumer view.
- **order_books_l2_streams.rs** – Mirrors the L1 example but for L2 depth snapshots, with isolated connections for high-volume pairs.
- **order_books_l2_manager.rs** – Uses `init_multi_order_book_l2_manager` to keep local L2 order books updated and accessible via a shared `OrderBookMap`.
- **public_trades_streams.rs** – Streams public trades for configured instruments on a single exchange.
- **public_trades_streams_multi_exchange.rs** – Merges public trade streams across exchanges for consolidated processing.

## `barter` examples
- **backtests_concurrent.rs** – Loads indexed market data from disk, builds shared backtest args, clones dynamic args, and runs many backtests concurrently before ranking results.
- **engine_async_with_historic_market_data_and_mock_execution.rs** – Runs the engine in async mode fed by historic market data, configuring system builder options like audit/engine feed modes and trading state.
- **engine_sync_with_audit_replica_engine_state.rs** – Shows synchronous engine feed with audit replication of engine state for downstream consumers.
- **engine_sync_with_live_market_data_and_mock_execution_and_audit.rs** – Connects to live market data, feeds the engine synchronously, mocks execution, and emits audits.
- **engine_sync_with_multiple_strategies.rs** – Demonstrates composing multiple strategies with custom per-instrument data while running a live-clock engine and handling trading/audit configuration.
- **engine_sync_with_risk_manager_open_order_checks.rs** – Illustrates integrating a risk manager that checks open orders during synchronous engine operation.
- **statistical_trading_summary.rs** – Produces statistical summaries (e.g., Sharpe, PnL) for trading activity across instruments.

## `barter-integration` examples
- **signed_get_request.rs** – Implements a custom request signer and HTTP parser (modeled for FTX) to build signed REST requests and parse authorization errors.
- **simple_websocket_integration.rs** – Provides a lightweight websocket integration example using the shared protocol abstractions.

## custom examples
- **engine_async_with_bybit_live_orderbook_and_trades.rs** - (async) Bybit live order book and trades example, showing events flowing and the engine shutting down cleanly

- **engine_sync_with_live_bybit_orderbooks_and_trades.rs**
(sync) Bybit live order book and trades example, showing events flowing and the engine shutting down cleanly. Bybit L2 order book transformer to track the server type, enforce ExchangeServer bounds, and emit the correct exchange ID for snapshots and updates, preventing perpetual streams from being tagged as spot.

Propagated the server-aware transformer through the Bybit StreamSelector so connectivity tracking stays aligned with the active Bybit endpoint.

- **bybit_dual_feed.rs** -Bybit Public Trades + Order Book L2
See [`bybit_dual_feed.rs`](examples/bybit_dual_feed.rs) for a ready-to-run setup that fans in Bybit Spot & USDT-perpetual public trades together with L2 order book depth. The example builds a combined `MarketStreamResult<MarketDataInstrument, DataKind>` feed that downstream systems (eg/ the Barter Engine) can consume directly.

- **scalping_orderbook_backtest.rs** -example that mirrors the Python v8 scalper. The example replays historic orderbook snapshots and trades to simulate maker-only entries and exits with queue-aware fills, cluster-driven take-profits, time-stop repricing, and end-of-day forced closures. (examples/python_version_benchmark)