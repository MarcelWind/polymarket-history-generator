from websocket import WebSocketApp
import json
import logging
import threading

logger = logging.getLogger(__name__)

MARKET_CHANNEL = "market"
USER_CHANNEL = "user"


class WebSocketOrderBook:
    def __init__(self, channel_type, url, data, auth, message_callback, verbose):
        self.channel_type = channel_type
        self.url = url
        self.data = list(data)  # Copy so we can append dynamically
        self.auth = auth
        self.message_callback = message_callback
        self.verbose = verbose
        self._stop_event = threading.Event()
        self.orderbooks = {}
        self._init_ws()

    def _init_ws(self):
        furl = self.url + "/ws/" + self.channel_type
        self.ws = WebSocketApp(
            furl,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
        )

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            desired_events = {
                "book",
                "price_change",
                "tick_size_change",
                "last_trade_price",
                "best_bid_ask",
            }

            def get_event_type(d):
                return d.get("event") or d.get("event_type")

            if isinstance(data, list):
                for item in data:
                    event_type = get_event_type(item)
                    if isinstance(item, dict) and event_type in desired_events:
                        asset_id = item.get("asset_id")
                        if asset_id:
                            self.orderbooks[asset_id] = item
                        if self.message_callback:
                            self.message_callback(item)
                        if self.verbose:
                            logger.debug(f"Processed: {item}")
                    elif isinstance(item, dict) and self.verbose and event_type is not None:
                        logger.debug(f"Ignored event: {event_type}")
            elif isinstance(data, dict):
                event_type = get_event_type(data)
                if event_type in desired_events:
                    asset_id = data.get("asset_id")
                    if asset_id:
                        self.orderbooks[asset_id] = data
                    if self.message_callback:
                        self.message_callback(data)
                    if self.verbose:
                        logger.debug(f"Processed: {data}")
                elif self.verbose and event_type is not None:
                    logger.debug(f"Ignored event: {event_type}")
            else:
                logger.warning(f"Unexpected JSON data type: {type(data)}")
        except json.JSONDecodeError:
            if message.strip() == "PONG":
                if self.verbose:
                    logger.debug("Pong received")
            else:
                logger.warning(f"Non-JSON message: {message}")

    def on_error(self, ws, error):
        logger.error(f"WebSocket error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        logger.warning(f"WebSocket closed: {close_status_code} {close_msg}")
        if not self._stop_event.is_set():
            logger.info("Reconnecting in 5 seconds...")
            self._stop_event.wait(5)
            if not self._stop_event.is_set():
                self._init_ws()
                self.ws.run_forever()

    def on_open(self, ws):
        if self.channel_type == MARKET_CHANNEL:
            ws.send(json.dumps({"assets_ids": self.data, "type": MARKET_CHANNEL}))
            logger.info(f"Subscribed to {len(self.data)} assets")
        elif self.channel_type == USER_CHANNEL and self.auth:
            ws.send(
                json.dumps(
                    {"markets": self.data, "type": USER_CHANNEL, "auth": self.auth}
                )
            )
        else:
            logger.error("Invalid channel type or missing auth for user channel")
            ws.close()
            return

        thr = threading.Thread(target=self.ping, args=(ws,), daemon=True)
        thr.start()

    def subscribe_to_tokens_ids(self, assets_ids):
        if self.channel_type == MARKET_CHANNEL:
            self.ws.send(
                json.dumps({"assets_ids": assets_ids, "operation": "subscribe"})
            )
            self.data.extend(assets_ids)
            logger.info(f"Subscribed to {len(assets_ids)} new assets")

    def unsubscribe_to_tokens_ids(self, assets_ids):
        if self.channel_type == MARKET_CHANNEL:
            self.ws.send(
                json.dumps({"assets_ids": assets_ids, "operation": "unsubscribe"})
            )
            self.data = [a for a in self.data if a not in set(assets_ids)]

    def ping(self, ws):
        while not self._stop_event.is_set():
            try:
                ws.send("PING")
            except Exception:
                break
            self._stop_event.wait(10)

    def stop(self):
        self._stop_event.set()
        self.ws.close()

    def run(self):
        self.ws.run_forever()
