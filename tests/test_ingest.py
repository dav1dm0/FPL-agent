import pytest
import respx
import httpx
from httpx import Response
from fpl_agent.ingest import FPLDataIngestor


@respx.mock
@pytest.mark.asyncio
async def test_fetch_bootstrap_data_success(ingestor):
    """
    Test Case: Normal Data.
    Justification: Ensures the happy path works and JSON is parsed correctly.
    """
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    mock_data = {"elements": [{"id": 1, "name": "Saka"}]}
    respx.get(url).mock(return_value=Response(200, json=mock_data))

    result = await ingestor.fetch_bootstrap_data()

    assert result["elements"][0]["id"] == 1
    assert result["elements"][0]["name"] == "Saka"


@respx.mock
@pytest.mark.asyncio
async def test_fetch_single_player_history_success(ingestor):
    """Test Case: Successful single player fetch."""
    player_id = 1
    url = f"https://fantasy.premierleague.com/api/element-summary/{player_id}/"
    mock_data = {"id": 1, "history": [{"gameweek": 1, "points": 5}]}
    respx.get(url).mock(return_value=Response(200, json=mock_data))

    result = await ingestor.fetch_single_player_history(player_id)

    assert result["id"] == 1
    assert len(result["history"]) == 1


@respx.mock
@pytest.mark.asyncio
async def test_fetch_single_player_history_not_found(ingestor):
    """
    Test Case: Boundary/Erroneous Data (404).
    Justification: The system should not crash if a specific player ID is missing.
    """
    player_id = 999
    url = f"https://fantasy.premierleague.com/api/element-summary/{player_id}/"
    respx.get(url).mock(return_value=Response(404))

    result = await ingestor.fetch_single_player_history(player_id)

    assert result is None  # Should return None instead of raising an unhandled exception


@respx.mock
@pytest.mark.asyncio
async def test_fetch_bootstrap_data_timeout(ingestor):
    """
    Test Case: Reliability (Network Timeout).
    Justification: Validates that our timeout logic works and raises the correct error.
    """
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    respx.get(url).mock(
        side_effect=httpx.ConnectTimeout("Connection timed out"))

    with pytest.raises(httpx.ConnectTimeout):
        await ingestor.fetch_bootstrap_data()


@respx.mock
@pytest.mark.asyncio
async def test_fetch_all_player_histories_mixed_results(ingestor):
    """Test Case: Batch processing with partial failures."""
    player_ids = [1, 2, 999]
    url_base = "https://fantasy.premierleague.com/api/element-summary/"

    respx.get(f"{url_base}1/").mock(return_value=Response(200, json={"id": 1}))
    respx.get(f"{url_base}2/").mock(return_value=Response(200, json={"id": 2}))
    respx.get(f"{url_base}999/").mock(return_value=Response(404))

    results = await ingestor.fetch_all_player_histories(player_ids)

    assert len(results) == 2  # Only 2 successful, 1 filtered out
    assert all(r["id"] in [1, 2] for r in results)
