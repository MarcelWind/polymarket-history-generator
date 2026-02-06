import pandas as pd
from pathlib import Path

total_candles = 0
total_trades = 0
total_volume = 0

for f in Path('data').rglob('*.parquet'):
    if 'unknown' in str(f):
        continue
    df = pd.read_parquet(f)
    total_candles += len(df)
    total_trades += df['trade_count'].sum()
    total_volume += df['volume'].sum()

print(f'Total candles: {total_candles}')
print(f'Total trades: {total_trades}')
print(f'Total volume: {total_volume}')

# Show any candles with trades
print('\nCandles with volume > 0:')
for f in Path('data').rglob('*.parquet'):
    if 'unknown' in str(f):
        continue
    df = pd.read_parquet(f)
    trades = df[df['trade_count'] > 0]
    if len(trades) > 0:
        print(f'  {f.parent.name}/{f.stem}: {len(trades)} candles, volume={trades["volume"].sum():.4f}')