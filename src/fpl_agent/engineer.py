import pandas as pd
import logging
from typing import List

logger = logging.getLogger(__name__)


class FeatureEngineer:
    """
    Transforms historical data into predictive features for XGBoost.
    Concern: Feature derivation and data cleaning (SRP).
    """

    def __init__(self, rolling_windows: List[int] = [3, 5]):
        self.rolling_windows = rolling_windows
        self.stats_to_engineer = ['total_points',
                                  'goals_scored', 'ict_index', 'minutes']

    def add_lag_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates the performance from the immediate previous Gameweek.
        Algorithm: Grouped Shift.
        Benefit: Tells the model if a player is currently 'active'.
        """
        df = df.sort_values(['element', 'round'])

        for stat in self.stats_to_engineer:
            df[f'{stat}_lag_1'] = df.groupby('element')[stat].shift(1)

        return df

    def add_rolling_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates moving averages (Form) over N games.
        Algorithm: Grouped Rolling Mean.
        Benefit: Captures sustained performance vs. one-off hauls.
        """
        for window in self.rolling_windows:
            for stat in self.stats_to_engineer:
                col_name = f"{stat}_roll_{window}"
                # closed='left' prevents data leakage from the future
                df[col_name] = (
                    df.groupby('element')[stat]
                    .transform(lambda x: x.shift(1).rolling(window=window, min_periods=1).mean())
                )
        return df

    def prepare_for_xgboost(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Final pass to handle types and nulls.
        Note: We do NOT scale features (Min-Max/Standard) as XGBoost is tree-based.
        """
        # Drop rows where we don't have enough history to make a prediction
        df = df.dropna(subset=[f'{s}_lag_1' for s in self.stats_to_engineer])

        # Convert IDs to categorical to help XGBoost partitioning
        categorical_cols = ['element_type', 'team']
        for col in categorical_cols:
            if col in df.columns:
                df[col] = df[col].astype('category')

        return df
