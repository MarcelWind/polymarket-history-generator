import signal
import sys
import time
import logging
import threading

from src.config import load_config
from src.market_discovery import MarketDiscovery
from src.ohlcv_aggregator import OHLCVAggregator
from src.storage import ParquetStorage
from src.websocket_orderbook import WebSocketOrderBook, MARKET_CHANNEL

WS_URL = "wss://ws-subscriptions-clob.polymarket.com"


def main():
    config = load_config()

    logging.basicConfig(
        level=getattr(logging, config.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger("main")

    discovery = MarketDiscovery()
    aggregator = OHLCVAggregator(config.candle_interval_seconds, tracked_assets=discovery.known_assets)
    storage = ParquetStorage(config.data_dir, market_lookup=discovery.known_assets)

    logger.info("Running initial market discovery...")
    initial_markets = discovery.discover(config.market_queries)
    if not initial_markets:
        logger.error("No markets found. Check your config.yaml market_queries.")
        sys.exit(1)

    asset_ids = [m.asset_id for m in initial_markets]
    event_count = len(set(m.event_slug for m in initial_markets))
    logger.info(f"Discovered {len(asset_ids)} assets across {event_count} events")

    ws = WebSocketOrderBook(
        channel_type=MARKET_CHANNEL,
        url=WS_URL,
        data=asset_ids,
        auth=None,
        message_callback=aggregator.on_message,
        verbose=config.verbose,
    )

    shutdown_event = threading.Event()

    def signal_handler(sig, frame):
        logger.info("Shutdown signal received")
        shutdown_event.set()
        ws.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    ws_thread = threading.Thread(target=ws.run, daemon=True, name="websocket")
    ws_thread.start()
    logger.info("WebSocket thread started")

    last_discovery = time.time()
    last_flush = time.time()

    while not shutdown_event.is_set():
        now = time.time()

        aggregator.flush_stale_candles()

        completed = aggregator.drain_completed_candles()
        if completed:
            count = storage.append_candles(completed)
            logger.info(
                f"Buffered {count} candles (buffer size: {storage.get_buffer_size()})"
            )

        if now - last_flush >= config.flush_interval_seconds:
            storage.flush_to_disk()
            storage.archive()
            last_flush = now

        if now - last_discovery >= config.discovery_interval_seconds:
            new_markets = discovery.discover(config.market_queries)
            if new_markets:
                new_ids = [m.asset_id for m in new_markets]
                logger.info(f"Subscribing to {len(new_ids)} new assets")
                ws.subscribe_to_tokens_ids(new_ids)
            last_discovery = now

        shutdown_event.wait(timeout=5)

    # Graceful shutdown: final flush
    logger.info("Shutting down...")
    aggregator.flush_stale_candles()
    completed = aggregator.drain_completed_candles()
    if completed:
        storage.append_candles(completed)
    storage.flush_to_disk()
    storage.archive()
    logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
