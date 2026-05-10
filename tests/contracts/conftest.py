import json
from pathlib import Path
import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"

@pytest.fixture
def exception_queue_rows():
    data = json.loads((FIXTURES_DIR / "databricks_exception_queue.json").read_text())
    return data

@pytest.fixture
def bapi_po_response():
    return json.loads((FIXTURES_DIR / "bapi_po_create_response.json").read_text())

@pytest.fixture
def bapi_goodsmvt_response():
    return json.loads((FIXTURES_DIR / "bapi_goodsmvt_response.json").read_text())
