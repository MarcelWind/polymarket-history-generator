from websocket import WebSocketApp
import json
import time
import threading

MARKET_CHANNEL = "market"
USER_CHANNEL = "user"

class WebSocketOrderBook:
    def __init__(self, channel_type, url, data, auth, message_callback, verbose):
        self.channel_type = channel_type
        self.url = url
        self.data = data
        self.auth = auth
        self.message_callback = message_callback
        self.verbose = verbose
        furl = url + "/ws/" + channel_type
        self.ws = WebSocketApp(
            furl,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
        )
        self.orderbooks = {}

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            desired_events = {'book', 'price_change', 'tick_size_change', 'last_trade_price', 'best_bid_ask'}
            
            def get_event_type(d):
                return d.get('event') or d.get('event_type')
            
            if isinstance(data, list):
                # Handle list of updates
                for item in data:
                    event_type = get_event_type(item)
                    if isinstance(item, dict) and event_type in desired_events:
                        asset_id = item.get('asset_id')
                        if asset_id:
                            self.orderbooks[asset_id] = item  # Store/update order book
                        if self.message_callback:
                            self.message_callback(item)
                        if self.verbose:
                            print(f"Processed: {item}")
                    elif isinstance(item, dict) and self.verbose and event_type is not None:
                        print(f"Ignored event: {event_type}")
            elif isinstance(data, dict):
                event_type = get_event_type(data)
                if event_type in desired_events:
                    # Handle single dict
                    asset_id = data.get('asset_id')
                    if asset_id:
                        self.orderbooks[asset_id] = data
                    if self.message_callback:
                        self.message_callback(data)
                    if self.verbose:
                        print(f"Processed: {data}")
                elif self.verbose and event_type is not None:
                    print(f"Ignored event: {event_type}")
            else:
                print(f"Unexpected JSON data type: {type(data)}, data: {data}")
        except json.JSONDecodeError:
            # Handle non-JSON messages
            if message.strip() == "PONG":
                if self.verbose:
                    print("Pong received")
            else:
                print(f"Non-JSON message: {message}")

    def on_error(self, ws, error):
        print("Error: ", error)

    def on_close(self, ws, close_status_code, close_msg):
        print("closing")

    def on_open(self, ws):
        if self.channel_type == MARKET_CHANNEL:
            ws.send(json.dumps({"assets_ids": self.data, "type": MARKET_CHANNEL}))
        elif self.channel_type == USER_CHANNEL and self.auth:
            ws.send(
                json.dumps(
                    {"markets": self.data, "type": USER_CHANNEL, "auth": self.auth}
                )
            )
        else:
            print("Invalid channel type or missing auth for user channel")
            ws.close()

        thr = threading.Thread(target=self.ping, args=(ws,))
        thr.start()

    def subscribe_to_tokens_ids(self, assets_ids):
        if self.channel_type == MARKET_CHANNEL:
            self.ws.send(json.dumps({"assets_ids": assets_ids, "operation": "subscribe"}))

    def unsubscribe_to_tokens_ids(self, assets_ids):
        if self.channel_type == MARKET_CHANNEL:
            self.ws.send(json.dumps({"assets_ids": assets_ids, "operation": "unsubscribe"}))

    def ping(self, ws):
        while True:
            ws.send("PING")
            time.sleep(10)

    def run(self):
        self.ws.run_forever()