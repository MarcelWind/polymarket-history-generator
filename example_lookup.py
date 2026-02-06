"""Quick example: loading and inspecting saved OHLCV data."""

import pandas as pd
from pathlib import Path

DATA_DIR = Path("data")


def load_event(event_slug: str) -> pd.DataFrame:
    """Load all market parquet files for an event into a single DataFrame."""
    event_dir = DATA_DIR / event_slug
    if not event_dir.exists():
        raise FileNotFoundError(f"No data for event: {event_slug}")

    frames = []
    for f in event_dir.glob("*.parquet"):
        df = pd.read_parquet(f)
        df["market"] = f.stem  # e.g. "-15-c", "-10-c-or-higher"
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values(["market", "timestamp"]).reset_index(drop=True)
    return combined


def list_events() -> list[str]:
    """List all event slugs that have saved data."""
    return sorted(
        d.name for d in DATA_DIR.iterdir()
        if d.is_dir() and d.name != "unknown"
    )


if __name__ == "__main__":
    # Show available events
    events = list_events()
    print(f"Available events ({len(events)}):")
    for e in events:
        n_files = len(list((DATA_DIR / e).glob("*.parquet")))
        print(f"  {e} ({n_files} markets)")

    # Pick one event and load it
    event_slug = events[9] if events else None
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

    # Show latest candle per market
    print("\nLatest candle per market:")
    latest = df.sort_values("timestamp").groupby("market").last()
    print(latest[["datetime", "close", "volume", "trade_count"]].to_string())
