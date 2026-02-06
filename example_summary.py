"""Aggregate volume summary from data.zip."""

import io
import zipfile
from pathlib import PurePosixPath

import pandas as pd

ARCHIVE = "data.zip"

total_candles = 0
total_trades = 0
total_volume = 0.0
volume_rows = []

with zipfile.ZipFile(ARCHIVE, "r") as zf:
    parquet_files = [
        n for n in zf.namelist()
        if n.endswith(".parquet") and "unknown" not in n.replace("\\", "/").split("/")
    ]

    for name in parquet_files:
        df = pd.read_parquet(io.BytesIO(zf.read(name)))
        total_candles += len(df)
        total_trades += df["trade_count"].sum()
        total_volume += df["volume"].sum()

        trades = df[df["trade_count"] > 0]
        if len(trades) > 0:
            p = PurePosixPath(name)
            label = f"{p.parent.name}/{p.stem}"
            volume_rows.append((label, len(trades), trades["volume"].sum()))

print(f"Total candles: {total_candles}")
print(f"Total trades: {total_trades}")
print(f"Total volume: {total_volume:.4f}")

print("\nCandles with volume > 0:")
for label, count, vol in sorted(volume_rows, key=lambda x: -x[2]):
    print(f"  {label}: {count} candles, volume={vol:.4f}")
