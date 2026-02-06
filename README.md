# Polymarket Weather OHLCV Recorder

Records OHLCV (Open/High/Low/Close/Volume) candle data from Polymarket weather markets via WebSocket, with automatic discovery of new markets as they open.

## How It Works

1. **Market Discovery** - Searches the Polymarket Gamma API for active weather events matching your configured queries
2. **WebSocket Streaming** - Subscribes to real-time orderbook updates for all YES tokens across discovered markets
3. **OHLCV Aggregation** - Converts tick-level data into configurable-interval candles (default: 1 minute)
   - Price: mid-price derived from best bid/ask
   - Volume: from matched trades (`last_trade_price` messages)
4. **Periodic Flush** - Buffers candles in memory and writes to parquet files on a configurable interval

New markets (e.g. tomorrow's temperature forecast) are automatically discovered and subscribed to while running.

## Setup

```bash
pip install -r requirements.txt
```

## Configuration

Edit `config.yaml`:

```yaml
market_queries:
  - "highest-temperature-in-toronto"
  - "highest-temperature-in-nyc"

candle_interval_seconds: 60     # candle size
discovery_interval_seconds: 300 # poll for new markets every 5 min
flush_interval_seconds: 120     # write to disk every 2 min
data_dir: "data"
log_level: "INFO"
verbose: false
```

## Usage

```bash
python run.py
```

Stop with `Ctrl+C` - remaining buffered candles are flushed to disk on shutdown.

## Data Output

Parquet files are organized by event and market:

```
data/
  highest-temperature-in-toronto-on-february-6-2026/
    -6-c-or-below.parquet
    -5-c.parquet
    -4-c.parquet
    0-c-or-higher.parquet
  highest-temperature-in-toronto-on-february-7-2026/
    ...
```

Each parquet file contains:

| Column | Description |
|--------|-------------|
| `asset_id` | Polymarket CLOB token ID |
| `timestamp` | Candle open time (Unix seconds) |
| `datetime` | ISO 8601 UTC timestamp |
| `open` | First mid-price in candle |
| `high` | Highest mid-price |
| `low` | Lowest mid-price |
| `close` | Last mid-price |
| `volume` | Total trade size |
| `trade_count` | Number of trades |
| `vwap` | Volume-weighted average price |

Read with pandas:

```python
import pandas as pd
df = pd.read_parquet("data/highest-temperature-in-toronto-on-february-6-2026/-5-c.parquet")
```

## Architecture

```
Gamma API ──periodic poll──> MarketDiscovery ──new asset IDs──> WebSocket
                                                                   │
                          on_message callback                      │
                                │                                  │
                                v                                  │
run.py main loop ──> OHLCVAggregator ──completed candles──> ParquetStorage
                     (tick → candle)                        (buffer → disk)
```

Three threads:
- **Main** - orchestrator loop (discovery, candle flush, disk writes)
- **WebSocket** (daemon) - receives market data, feeds aggregator
- **Ping** (daemon) - keeps WebSocket alive

## Project Structure

```
├── config.yaml               # User configuration
├── run.py                    # Entry point / orchestrator
├── src/
│   ├── config.py             # Config loading
│   ├── market_discovery.py   # Gamma API market discovery
│   ├── ohlcv_aggregator.py   # Tick-to-candle aggregation
│   ├── storage.py            # Parquet persistence
│   └── websocket_orderbook.py # WebSocket connection
```
