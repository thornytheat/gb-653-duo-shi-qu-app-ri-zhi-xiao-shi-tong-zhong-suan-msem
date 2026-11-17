import os
import sys
from pathlib import Path
import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


@pytest.fixture
def client(tmp_path):
    os.environ["DB_PATH"] = str(tmp_path / "test.db")
    os.environ["ARCHIVE_DB_PATH"] = str(tmp_path / "archive.db")
    os.environ["COLD_ROOT"] = str(tmp_path / "cold")
    os.environ["HOT_QUOTA_BYTES"] = "512"
    os.environ["COLD_QUOTA_BYTES"] = "4096"
    from app.main import create_app
    with TestClient(create_app()) as value:
        yield value
