"""
Pytest global fixtures for test isolation.
"""

from pathlib import Path
import pytest
from app.database import DEFAULT_DB_PATH, init_db, set_db_path
from app.destination import (
    DestinationResponse,
    RetryConfig,
    SimulatedDestinationAdapter,
    set_destination_adapter,
    set_retry_config,
)


@pytest.fixture(autouse=True)
def isolated_test_db(tmp_path: Path):
    """
    Ensure every test runs against a clean, isolated temporary SQLite database.
    """
    db_file = str(tmp_path / "test_isolated.db")
    init_db(db_file)
    set_db_path(db_file)
    yield db_file
    set_db_path(DEFAULT_DB_PATH)


@pytest.fixture(autouse=True)
def reset_destination_simulator():
    """
    Ensure every test runs with a clean destination simulator and zero backoff delay.
    """
    sim = SimulatedDestinationAdapter(
        default_response=DestinationResponse(
            status_code=200,
            data={"status": "delivered"},
            error_message=None,
            is_timeout=False,
        )
    )
    set_destination_adapter(sim)
    set_retry_config(RetryConfig(max_attempts=3, base_delay=0.0, max_delay=10.0))
    yield sim
    # Reset back to default
    set_destination_adapter(SimulatedDestinationAdapter())
    set_retry_config(RetryConfig(max_attempts=3, base_delay=1.0, max_delay=10.0))
