import asyncio
import logging
import httpx
import pandas as pd
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

FPL_BASE_URL = "https://fantasy.premierleague.com/api"
BOOTSTRAP_ENDPOINT = f"{FPL_BASE_URL}/bootstrap-static/"
ELEMENT_SUMMARY_ENDPOINT = f"{FPL_BASE_URL}/element-summary/"


class FPLDataIngestor:
    """
    Handles reliable, concurrent retrieval of FPL data.
    """

    def __init__(self, timeout_seconds: int = 10):
        self.timeout = timeout_seconds
        self.client = httpx.AsyncClient(timeout=self.timeout)

    async def fetch_bootstrap_data(self) -> Dict[str, Any]:
        """Retrieves global overview: players, teams, and gameweeks."""
        response = await self.client.get(BOOTSTRAP_ENDPOINT)
        response.raise_for_status()
        return response.json()

    async def fetch_single_player_history(self, player_id: int) -> Optional[Dict[str, Any]]:
        """
        Retrieves detailed history for one player.
        Handles individual failures so one bad ID doesn't crash the whole run.
        """
        url = f"{ELEMENT_SUMMARY_ENDPOINT}{player_id}/"
        try:
            response = await self.client.get(url)
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(
                    f"Non-200 response for player {player_id}: {response.status_code}")
        except httpx.RequestError as exc:
            logger.error(f"Error fetching player {player_id}: {exc}")
        return None

    async def fetch_all_player_histories(self, player_ids: List[int]) -> List[Dict[str, Any]]:
        """
        Performance choice: Concurrent fetching using asyncio.gather.
        Benefit: Drastic reduction in execution time.
        Tradeoff: Must respect rate limits (FPL is usually lenient but be careful).
        """
        tasks = [self.fetch_single_player_history(pid) for pid in player_ids]
        # Run all requests in parallel
        histories = await asyncio.gather(*tasks)
        # Filter out None results from failed requests
        successful = len([h for h in histories if h is not None])
        logger.info(
            f"Successfully fetched {successful}/{len(player_ids)} player histories")
        return [h for h in histories if h is not None]

    async def close(self):
        await self.client.aclose()

# Function to be called from the main workflow


async def run_ingestion_pipeline():
    ingestor = FPLDataIngestor()
    try:
        raw_meta = await ingestor.fetch_bootstrap_data()
        player_ids = [player['id'] for player in raw_meta['elements']]

        logger.info(f"Fetching history for {len(player_ids)} players...")
        histories = await ingestor.fetch_all_player_histories(player_ids)

        return raw_meta, histories
    finally:
        await ingestor.close()
