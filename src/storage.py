import logging
import shutil
import tempfile
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
import os
from pathlib import Path

from src.market_discovery import MarketInfo

logger = logging.getLogger(__name__)


class ParquetStorage:
    def __init__(self, data_dir: str = "data", market_lookup: dict[str, MarketInfo] | None = None):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.market_lookup = market_lookup if market_lookup is not None else {}
        self._buffer: list[dict] = []

    def _get_file_path(self, asset_id: str) -> Path:
        info = self.market_lookup.get(asset_id)
        if info:
            path = self.data_dir / info.event_slug / f"{info.market_slug}.parquet"
        else:
            path = self.data_dir / "unknown" / f"{asset_id[:16]}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def append_candles(self, candles: list) -> int:
        for c in candles:
            self._buffer.append(
                {
                    "asset_id": c.asset_id,
                    "timestamp": c.timestamp,
                    "datetime": datetime.fromtimestamp(
                        c.timestamp, tz=timezone.utc
                    ).isoformat(),
                    "open": c.open,
                    "high": c.high,
                    "low": c.low,
                    "close": c.close,
                    "volume": c.volume,
                    "trade_count": c.trade_count,
                    "vwap": c.vwap,
                    "buy_volume": c.buy_volume,
                    "sell_volume": c.sell_volume,
                    "outcome": getattr(c, "outcome", ""),
                }
            )
        return len(candles)

    def flush_to_disk(self):
        if not self._buffer:
            logger.debug("Nothing to flush")
            return

        df = pd.DataFrame(self._buffer)
        # group by asset_id and outcome so we keep separate rows per outcome
        grouped = df.groupby(["asset_id", "outcome"])

        try:
            for (raw_key, outcome), group_df in grouped:
                aid = str(raw_key)
                file_path = self._get_file_path(aid)

                if file_path.exists():
                    existing = pd.read_parquet(file_path)
                    combined = pd.concat([existing, group_df], ignore_index=True)
                    combined = combined.drop_duplicates(
                        subset=["asset_id", "outcome", "timestamp"], keep="last"
                    )
                    combined = combined.sort_values(["timestamp", "outcome"]).reset_index(drop=True)
                    combined.to_parquet(file_path, index=False, engine="pyarrow")
                else:
                    group_df.to_parquet(file_path, index=False, engine="pyarrow")

                info = self.market_lookup.get(aid)
                label = f"{info.event_slug}/{info.market_slug}/{outcome}" if info else f"{aid[:16]}/{outcome}"
                logger.info(f"Flushed {len(group_df)} candles -> {label}")

            flushed_count = len(self._buffer)
            self._buffer = []
            logger.info(f"Flush complete: {flushed_count} candles written to disk")
        except Exception:
            logger.exception("Error flushing to disk, buffer retained for retry")

    def load_existing(self, asset_id: str) -> pd.DataFrame | None:
        file_path = self._get_file_path(asset_id)
        if file_path.exists():
            return pd.read_parquet(file_path)
        return None

    def archive(self, archive_path: str = "data.zip"):
        """Zip the data directory, writing to a temp file then replacing atomically."""
        archive_dest = Path(archive_path)
        
        try:
            with tempfile.NamedTemporaryFile(
                dir=archive_dest.parent, suffix=".zip", delete=False
            ) as tmp:
                tmp_path = Path(tmp.name)

            # shutil.make_archive wants the base name without extension
            shutil.make_archive(
                str(tmp_path.with_suffix("")), "zip", root_dir=".", base_dir=str(self.data_dir)
            )
            # make_archive appends .zip to the base name
            created = tmp_path.with_suffix("").with_suffix(".zip")
            
            # Create backups before overwriting, but only if new archive is not smaller in size
            if archive_dest.exists():
                new_size = created.stat().st_size
                old_size = archive_dest.stat().st_size
                if new_size >= old_size:
                    backup1 = archive_dest.parent / "data_backup_1.zip"
                    backup2 = archive_dest.parent / "data_backup_2.zip"
                    if backup1.exists():
                        if not backup2.exists() or backup1.stat().st_size > backup2.stat().st_size:
                            backup1.replace(backup2)  # Move backup1 to backup2 only if larger
                        else:
                            logger.info("Skipping backup1 to backup2 move: backup1 not larger than backup2")
                    archive_dest.replace(backup1)  # Move current to backup1
                else:
                    logger.warning(f"New archive ({new_size} bytes) is smaller than existing ({old_size} bytes), skipping backup creation")
            
            created.replace(archive_dest)
            tmp_path.unlink(missing_ok=True)

            size_mb = archive_dest.stat().st_size / (1024 * 1024)
            logger.info(f"Archive updated: {archive_dest} ({size_mb:.1f} MB)")

            # chmod: rw-r--- (640)
            Path("data.zip").chmod(0o640)
            logger.info(f"Archive permissions set to 640")
        except Exception:
            logger.exception("Error creating archive")
            if 'tmp_path' in locals():
                tmp_path.unlink(missing_ok=True)

    def get_buffer_size(self) -> int:
        return len(self._buffer)
