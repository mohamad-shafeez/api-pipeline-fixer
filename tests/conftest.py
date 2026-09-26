"""
Pytest global fixtures for test isolation.
"""

from pathlib import Path
import pytest
from app.database import DEFAULT_DB_PATH, init_db, set_db_path


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
