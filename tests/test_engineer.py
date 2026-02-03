import pytest
import pandas as pd
from fpl_agent.engineer import FeatureEngineer


@pytest.fixture
def engineer():
    return FeatureEngineer(rolling_windows=[2])


def test_add_lag_features_identifies_previous_score(engineer):
    # Single player, 2 gameweeks
    df = pd.DataFrame({
        'element': [1, 1],
        'round': [1, 2],
        'total_points': [5, 10],
        'goals_scored': [1, 0],
        'ict_index': [10.0, 5.0],
        'minutes': [90, 90]
    })

    processed = engineer.add_lag_features(df)

    # GW2 should 'see' GW1's 5 points
    assert processed.loc[processed['round'] ==
                         2, 'total_points_lag_1'].iloc[0] == 5
    # GW1 has no previous, should be NaN
    assert pd.isna(processed.loc[processed['round']
                   == 1, 'total_points_lag_1'].iloc[0])


def test_rolling_features_average_correctly(engineer):
    # Player with points 2, 4, 6
    df = pd.DataFrame({
        'element': [1, 1, 1],
        'round': [1, 2, 3],
        'total_points': [2, 4, 6],
        'goals_scored': [0, 0, 0], 'ict_index': [0, 0, 0], 'minutes': [90, 90, 90]
    })

    processed = engineer.add_rolling_features(df)

    # GW3 rolling average (window=2) of GW1 and GW2 points (2+4)/2 = 3
    result = processed.loc[processed['round']
                           == 3, 'total_points_roll_2'].iloc[0]
    assert result == 3.0
