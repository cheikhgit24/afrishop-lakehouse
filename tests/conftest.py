import pytest


@pytest.fixture(scope="session", autouse=True)
def airflow_db(request):
    """Initialise la base SQLite d'Airflow si (et seulement si) les tests du DAG sont lances."""
    if not any("test_dag_integrity" in item.nodeid for item in request.session.items):
        return
    try:
        from airflow.utils.db import initdb
    except ImportError:  # environnement sans Airflow (venv Great Expectations)
        return
    initdb()


@pytest.fixture(scope="session")
def spark():
    from afrishop.spark_session import get_spark

    session = get_spark("tests")
    session.sparkContext.setLogLevel("ERROR")
    session.conf.set("spark.sql.shuffle.partitions", "2")
    yield session
    session.stop()
