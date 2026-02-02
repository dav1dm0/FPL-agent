import pytest
import pandas as pd
from fpl_agent.process import DataPersistence


@pytest.fixture
def persistence_service(tmp_path):
    # Use tmp_path fixture to avoid creating real files during tests
    return DataPersistence(output_dir=tmp_path)


def test_flatten_player_history_normal(persistence_service):
    #  Mock nested API response
    raw_data = [
        {"history": [{"element": 1, "total_points": 5},
                     {"element": 1, "total_points": 2}]},
        {"history": [{"element": 2, "total_points": 10}]}
    ]

    df = persistence_service.flatten_player_history(raw_data)

    assert len(df) == 3
    assert "total_points" in df.columns
    assert df["element"].iloc[0] == 1


def test_flatten_player_history_empty(persistence_service):
    # Invalid/Empty input
    raw_data = [{"history": []}, {"no_history_key": []}]

    df = persistence_service.flatten_player_history(raw_data)

    assert df.empty


def test_save_to_parquet_creates_file(persistence_service, tmp_path):
    df = pd.DataFrame({"id": [1], "score": [10.5]})
    filename = "test_output"

    persistence_service.save_to_parquet(df, filename)

    expected_file = tmp_path / "test_output.parquet"
    assert expected_file.exists()
