from __future__ import annotations

import re
from typing import Iterable

import pandas as pd

from fetch_data import load_zip


_SUFFIX_RE = r"__(?:yes|no)$"


def load_and_prepare(
    zip_path: str = "data.zip",
    event_slug: str | None = None,
) -> pd.DataFrame:
    df = load_zip(zip_path).copy()
    if event_slug:
        df = df[df["event_slug"] == event_slug].copy()
    return normalize_market_outcomes(df)


def normalize_market_outcomes(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "market" not in out.columns:
        raise ValueError("Expected 'market' column in dataframe")

    market_raw = out["market"].astype(str)
    has_suffix = market_raw.str.contains(_SUFFIX_RE, regex=True)
    suffix_outcome = market_raw.str.extract(r"__(yes|no)$", expand=False)

    out["market"] = market_raw.str.replace(_SUFFIX_RE, "", regex=True)

    if "outcome" not in out.columns:
        out["outcome"] = ""

    out["outcome"] = out["outcome"].fillna("").astype(str).str.strip().str.lower()

    fill_mask = out["outcome"].eq("") & suffix_outcome.notna()
    out.loc[fill_mask, "outcome"] = suffix_outcome[fill_mask]

    conflict_mask = suffix_outcome.notna() & out["outcome"].ne("") & out["outcome"].ne(suffix_outcome)
    out = out[~conflict_mask].copy()

    out["_has_suffix"] = has_suffix.loc[out.index].astype(int)

    if "timestamp" in out.columns:
        sort_cols = ["_has_suffix", "timestamp"]
        out = out.sort_values(sort_cols, ascending=[False, True])

        subset_cols: list[str] = []
        for col in ["event_slug", "asset_id", "market", "outcome", "timestamp"]:
            if col in out.columns:
                subset_cols.append(col)

        if subset_cols:
            out = out.drop_duplicates(subset=subset_cols, keep="first")

    out = out.drop(columns=["_has_suffix"], errors="ignore").reset_index(drop=True)
    return out


def pick_plot_frame(df: pd.DataFrame, prefer_outcome: str | None = None) -> pd.DataFrame:
    out = df.copy()
    if prefer_outcome and "outcome" in out.columns:
        target = prefer_outcome.strip().lower()
        out = out[out["outcome"].astype(str).str.lower() == target].copy()
    return out
