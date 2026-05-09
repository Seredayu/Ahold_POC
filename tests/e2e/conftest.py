import os
import pytest
import httpx


@pytest.fixture(scope="session", autouse=True)
def require_e2e_api_base():
    if not os.environ.get("E2E_API_BASE"):
        pytest.skip("E2E_API_BASE not set — skipping e2e tests")


@pytest.fixture(scope="session")
def api_base() -> str:
    base = os.environ["E2E_API_BASE"].rstrip("/")
    return base


@pytest.fixture(scope="session")
def manager_id() -> str:
    return os.environ.get("E2E_MANAGER_ID", "e2e-test-manager")


@pytest.fixture(scope="session")
def client(api_base) -> httpx.Client:
    c = httpx.Client(base_url=api_base, timeout=30.0)
    yield c
    c.close()
