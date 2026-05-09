import pytest

try:
    from pyspark.sql import SparkSession as _SparkSession
    _PYSPARK_AVAILABLE = True
except ImportError:
    _PYSPARK_AVAILABLE = False


@pytest.fixture(scope="session")
def spark():
    if not _PYSPARK_AVAILABLE:
        pytest.skip("pyspark not installed")
    session = (
        _SparkSession.builder
        .master("local[1]")
        .appName("ahold-poc-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
