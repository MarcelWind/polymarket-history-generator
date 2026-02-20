from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.event_data_utils import load_and_prepare


def main() -> None:
    parser = argparse.ArgumentParser(description="Aggregate market volume by outcome from data.zip")
    parser.add_argument("--zip-path", default="data.zip", help="Path to zip archive")
    parser.add_argument("--event-slug", default=None, help="Optional event slug filter")
    parser.add_argument("--top", type=int, default=200, help="Rows to print")
    parser.add_argument("--save-csv", default=None, help="Optional output CSV path")
    args = parser.parse_args()

    df = load_and_prepare(zip_path=args.zip_path, event_slug=args.event_slug)

    agg = (
        df.groupby(["event_slug", "market", "outcome"], dropna=False)
        .agg(
            total_volume=("volume", "sum"),
            buy_volume=("buy_volume", "sum") if "buy_volume" in df.columns else ("volume", "sum"),
            sell_volume=("sell_volume", "sum") if "sell_volume" in df.columns else ("volume", "sum"),
            trade_count=("trade_count", "sum"),
        )
        .reset_index()
        .sort_values(["event_slug", "market", "outcome"])
    )

    print(agg.head(args.top).to_string(index=False))

    if args.event_slug:
        pivot = (
            agg[agg["event_slug"] == args.event_slug]
            .pivot_table(
                index="market",
                columns="outcome",
                values=["buy_volume", "sell_volume", "total_volume"],
                fill_value=0,
            )
            .sort_index()
        )
        print("\nPivot for event:")
        print(pivot.to_string())

    if args.save_csv:
        agg.to_csv(args.save_csv, index=False)
        print(f"\nSaved: {args.save_csv}")


if __name__ == "__main__":
    main()
