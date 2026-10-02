import pytest


@pytest.fixture(scope="session")
def spark():
    from afrishop.spark_session import get_spark

    session = get_spark("tests")
    session.sparkContext.setLogLevel("ERROR")
    session.conf.set("spark.sql.shuffle.partitions", "2")
    yield session
    session.stop()
