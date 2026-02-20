from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.event_data_utils import load_and_prepare, pick_plot_frame


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot event markets with gap-safe lines")
    parser.add_argument("--zip-path", default="data.zip", help="Path to zip archive")
    parser.add_argument("--event-slug", required=True, help="Event slug to plot")
    parser.add_argument(
        "--prefer-outcome",
        default=None,
        help="Optional outcome filter for plotting",
    )
    parser.add_argument("--gap-break-minutes", type=float, default=45.0, help="Break line when time gap exceeds this")
    parser.add_argument("--save", default=None, help="Optional output image path")
    args = parser.parse_args()

    df_work = load_and_prepare(zip_path=args.zip_path, event_slug=args.event_slug)
    plot_df = pick_plot_frame(df_work, prefer_outcome=args.prefer_outcome)

    if plot_df.empty:
        raise SystemExit("No data after filtering. Check event slug/outcome.")

    plt.figure(figsize=(12, 6))

    if args.prefer_outcome:
        # single-outcome mode: one series per market
        series_iter = [((market,), g.copy()) for market, g in plot_df.groupby("market")]
    else:
        # all-outcomes mode: keep yes/no separated per market
        series_iter = [
            ((market, outcome), g.copy())
            for (market, outcome), g in plot_df.groupby(["market", "outcome"], dropna=False)
        ]

    for key, df_market in series_iter:
        if df_market.empty:
            continue

        if "datetime" in df_market.columns:
            df_market["dt"] = pd.to_datetime(df_market["datetime"])
        else:
            df_market["dt"] = pd.to_datetime(df_market["timestamp"], unit="s")

        df_market = df_market.sort_values("dt")

        gap_seconds = df_market["dt"].diff().dt.total_seconds()
        break_mask = gap_seconds > (args.gap_break_minutes * 60.0)
        df_market.loc[break_mask, "close"] = float("nan")

        if args.prefer_outcome:
            label = key[0]
        else:
            if len(key) == 2:
                market, outcome = key
            else:
                market, outcome = key[0], None
            suffix = str(outcome).strip().lower() if outcome is not None else ""
            label = f"{market}__{suffix}" if suffix else str(market)

        plt.plot(df_market["dt"], df_market["close"], label=label)

    plt.title(f"Market prices for event: {args.event_slug} outcome: {args.prefer_outcome or 'ALL'}") 
    plt.xlabel("Datetime (UTC)")
    plt.ylabel("Close Price")
    plt.legend()
    plt.tight_layout()

    ax = plt.gca()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d-%m %H:%M"))

    if "datetime" in plot_df.columns:
        x_vol = pd.to_datetime(plot_df["datetime"])
    else:
        x_vol = pd.to_datetime(plot_df["timestamp"], unit="s")
    vol = plot_df["volume"]

    vol_bins = x_vol.dt.floor("h")
    vol_sum = vol.groupby(vol_bins).sum()

    ax2 = ax.twinx()
    ax2.bar(vol_sum.index, np.asarray(vol_sum.values, dtype="float64"), width=0.02, color="gray", alpha=0.2, label="Total Volume (hourly)")
    ax2.set_ylabel("Volume")
    ax2.legend(loc="upper right")

    if args.save:
        plt.savefig(args.save, dpi=150, bbox_inches="tight")
        print(f"Saved: {args.save}")
    else:
        plt.show()


if __name__ == "__main__":
    main()
