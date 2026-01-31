import pytest
import pytest_asyncio
from fpl_agent.ingest import FPLDataIngestor


@pytest_asyncio.fixture
async def ingestor():
    """Fixture that provides a properly initialized and cleaned up ingestor."""
    ingest_service = FPLDataIngestor(timeout_seconds=5)
    yield ingest_service
    await ingest_service.close()
