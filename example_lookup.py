"""Quick example: loading and inspecting saved OHLCV data from data.zip."""

import io
import zipfile
from pathlib import PurePosixPath

import pandas as pd

ARCHIVE = "data.zip"


def _open_zip():
    return zipfile.ZipFile(ARCHIVE, "r")


def load_event(event_slug: str) -> pd.DataFrame:
    """Load all market parquet files for an event into a single DataFrame."""
    prefix = f"data/{event_slug}/"
    frames = []
    with _open_zip() as zf:
        for name in zf.namelist():
            normalized = name.replace("\\", "/")
            if normalized.startswith(prefix) and normalized.endswith(".parquet"):
                stem = PurePosixPath(normalized).stem
                df = pd.read_parquet(io.BytesIO(zf.read(name)))
                df["market"] = stem
                frames.append(df)

    if not frames:
        raise FileNotFoundError(f"No data for event: {event_slug}")

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values(["market", "timestamp"]).reset_index(drop=True)
    return combined


def list_events() -> list[str]:
    """List all event slugs that have saved data."""
    events = set()
    with _open_zip() as zf:
        for name in zf.namelist():
            # Normalize backslashes (Windows zips) to forward slashes
            parts = name.replace("\\", "/").split("/")
            # data/{event_slug}/{market}.parquet
            if len(parts) == 3 and parts[2].endswith(".parquet") and parts[1] != "unknown":
                events.add(parts[1])
    return sorted(events)


if __name__ == "__main__":
    events = list_events()
    print(f"Available events ({len(events)}):")

    with _open_zip() as zf:
        all_names = [n.replace("\\", "/") for n in zf.namelist()]
    for e in events:
        prefix = f"data/{e}/"
        n_files = sum(1 for n in all_names if n.startswith(prefix) and n.endswith(".parquet"))
        print(f"  {e} ({n_files} markets)")

    event_slug = events[9] if len(events) > 9 else events[0] if events else None
    if not event_slug:
        print("No data found.")
        raise SystemExit

    print(f"\n--- Loading: {event_slug} ---")
    df = load_event(event_slug)
    print(f"Shape: {df.shape}")
    print(f"Markets: {df['market'].unique().tolist()}")
    print(f"Time range: {df['datetime'].min()} -> {df['datetime'].max()}")
    print(f"Total trades: {df['trade_count'].sum()}")
    print(f"Total volume: {df['volume'].sum():.4f}")

    print("\nLatest candle per market:")
    latest = df.sort_values("timestamp").groupby("market").last()
    print(latest[["datetime", "close", "volume", "trade_count"]].to_string())
