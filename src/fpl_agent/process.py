import pandas as pd
import logging
from pathlib import Path
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


class DataPersistence:
    """
    Handles the transformation of raw API data into ML-ready Parquet files.
    Concern: Persistence and Data Structuring (SRP).
    """

    def __init__(self, output_dir: str = "data/processed"):
        self.output_path = Path(output_dir)
        self._ensure_output_directory()

    def _ensure_output_directory(self) -> None:
        """Internal helper to ensure the storage path exists."""
        self.output_path.mkdir(parents=True, exist_ok=True)

    def flatten_player_history(self, raw_histories: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Algorithm: Normalizes nested JSON 'history' lists into a flat table.
        Benefit: Converts hierarchical data into the 2D format required by XGBoost.
        """
        all_gameweeks = []

        for record in raw_histories:
            # The 'history' key contains the list of past Gameweeks
            history = record.get("history", [])
            if not history:
                continue

            history_df = pd.DataFrame(history)
            all_gameweeks.append(history_df)

        if not all_gameweeks:
            logger.warning("No player history found to flatten.")
            return pd.DataFrame()

        return pd.concat(all_gameweeks, ignore_index=True)

    def save_to_parquet(self, dataframe: pd.DataFrame, filename: str) -> None:
        """
        Persists the dataframe using the Parquet format.
        Algorithm: Snappy compression (Default) - balances speed and size.
        """
        if dataframe.empty:
            logger.error(
                "Attempted to save an empty DataFrame to %s", filename)
            return

        file_path = self.output_path / f"{filename}.parquet"

        try:
            # index=False: Clean Code - we don't need the arbitrary Pandas index on disk
            dataframe.to_parquet(file_path, index=False, engine="pyarrow")
            logger.info("Successfully saved %d rows to %s",
                        len(dataframe), file_path)
        except Exception as e:
            logger.error("Failed to save Parquet file: %s", e)
            raise
