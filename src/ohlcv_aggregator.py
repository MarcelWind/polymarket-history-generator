import time
import logging
import threading
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class OHLCVCandle:
    asset_id: str
    timestamp: int  # Candle open time (Unix seconds, floored to interval)
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: int
    vwap: float


class OHLCVAggregator:
    def __init__(self, candle_interval_seconds: int = 60):
        self.interval = candle_interval_seconds
        self.lock = threading.Lock()
        self._current_candles: dict[str, dict] = {}
        self._completed_candles: list[OHLCVCandle] = []
        self._last_bbo: dict[str, tuple[float, float]] = {}

    def on_message(self, message: dict):
        event_type = message.get("event_type") or message.get("event")

        if event_type == "last_trade_price":
            self._handle_trade(message)
        elif event_type == "best_bid_ask":
            self._handle_bbo(message)
        elif event_type == "price_change":
            self._handle_price_change(message)
        elif event_type == "book":
            self._handle_book(message)

    def _handle_trade(self, msg: dict):
        asset_id = msg.get("asset_id")
        if not asset_id:
            return
        timestamp_ms = int(msg.get("timestamp", time.time() * 1000))
        price = float(msg.get("price", 0))
        size = float(msg.get("size", 0))

        if price <= 0:
            return

        with self.lock:
            self._update_candle(
                asset_id, timestamp_ms, price=price, trade_size=size, is_trade=True
            )

    def _handle_bbo(self, msg: dict):
        asset_id = msg.get("asset_id")
        if not asset_id:
            return

        best_bid = float(msg.get("best_bid", 0))
        best_ask = float(msg.get("best_ask", 0))
        timestamp_ms = int(msg.get("timestamp", time.time() * 1000))

        if best_bid > 0 and best_ask > 0:
            mid = (best_bid + best_ask) / 2
            with self.lock:
                self._last_bbo[asset_id] = (best_bid, best_ask)
                self._update_candle(asset_id, timestamp_ms, price=mid, is_trade=False)

    def _handle_price_change(self, msg: dict):
        timestamp_ms = int(msg.get("timestamp", time.time() * 1000))
        for change in msg.get("price_changes", []):
            asset_id = change.get("asset_id")
            if not asset_id:
                continue
            best_bid = float(change.get("best_bid", 0))
            best_ask = float(change.get("best_ask", 0))

            if best_bid > 0 and best_ask > 0:
                mid = (best_bid + best_ask) / 2
                with self.lock:
                    self._last_bbo[asset_id] = (best_bid, best_ask)
                    self._update_candle(
                        asset_id, timestamp_ms, price=mid, is_trade=False
                    )

    def _handle_book(self, msg: dict):
        asset_id = msg.get("asset_id")
        if not asset_id:
            return

        buys = msg.get("buys", [])
        sells = msg.get("sells", [])
        timestamp_ms = int(msg.get("timestamp", time.time() * 1000))

        best_bid = max((float(b.get("price", 0)) for b in buys), default=0)
        best_ask = min((float(s.get("price", 0)) for s in sells), default=0)

        if best_bid > 0 and best_ask > 0:
            mid = (best_bid + best_ask) / 2
            with self.lock:
                self._last_bbo[asset_id] = (best_bid, best_ask)
                self._update_candle(asset_id, timestamp_ms, price=mid, is_trade=False)

    def _candle_start_time(self, timestamp_ms: int) -> int:
        ts_seconds = timestamp_ms // 1000
        return (ts_seconds // self.interval) * self.interval

    def _update_candle(
        self,
        asset_id: str,
        timestamp_ms: int,
        price: float,
        trade_size: float = 0.0,
        is_trade: bool = False,
    ):
        candle_start = self._candle_start_time(timestamp_ms)

        if asset_id in self._current_candles:
            current = self._current_candles[asset_id]
            if current["start_time"] != candle_start:
                self._finalize_candle(current)
                self._current_candles[asset_id] = self._new_candle_state(
                    asset_id, candle_start, price
                )
        else:
            self._current_candles[asset_id] = self._new_candle_state(
                asset_id, candle_start, price
            )

        c = self._current_candles[asset_id]
        c["high"] = max(c["high"], price)
        c["low"] = min(c["low"], price)
        c["close"] = price

        if is_trade and trade_size > 0:
            c["volume"] += trade_size
            c["trade_count"] += 1
            c["vwap_numerator"] += price * trade_size

    def _new_candle_state(self, asset_id: str, start_time: int, price: float) -> dict:
        return {
            "asset_id": asset_id,
            "start_time": start_time,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": 0.0,
            "trade_count": 0,
            "vwap_numerator": 0.0,
        }

    def _finalize_candle(self, state: dict):
        vwap = (
            state["vwap_numerator"] / state["volume"]
            if state["volume"] > 0
            else state["close"]
        )

        candle = OHLCVCandle(
            asset_id=state["asset_id"],
            timestamp=state["start_time"],
            open=state["open"],
            high=state["high"],
            low=state["low"],
            close=state["close"],
            volume=state["volume"],
            trade_count=state["trade_count"],
            vwap=vwap,
        )
        self._completed_candles.append(candle)
        logger.debug(
            f"Candle finalized: {state['asset_id'][:16]}... @ {state['start_time']}"
        )

    def flush_stale_candles(self):
        now_ms = int(time.time() * 1000)
        current_candle_start = self._candle_start_time(now_ms)

        with self.lock:
            for asset_id in list(self._current_candles.keys()):
                c = self._current_candles[asset_id]
                if c["start_time"] < current_candle_start:
                    self._finalize_candle(c)
                    del self._current_candles[asset_id]

    def drain_completed_candles(self) -> list[OHLCVCandle]:
        with self.lock:
            candles = self._completed_candles
            self._completed_candles = []
            return candles
