import os
import pytest
import httpx


def pytest_configure(config):
    """Skip all e2e tests if E2E_API_BASE is not set."""
    if not os.environ.get("E2E_API_BASE"):
        pytest.skip("E2E_API_BASE not set — skipping e2e tests", allow_module_level=True)


@pytest.fixture(scope="session")
def api_base() -> str:
    base = os.environ["E2E_API_BASE"].rstrip("/")
    return base


@pytest.fixture(scope="session")
def manager_id() -> str:
    return os.environ.get("E2E_MANAGER_ID", "e2e-test-manager")


@pytest.fixture(scope="session")
def client(api_base) -> httpx.Client:
    with httpx.Client(base_url=api_base, timeout=30.0) as c:
        yield c
