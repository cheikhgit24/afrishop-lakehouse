FROM afrishop-airflow:latest

RUN pip install --no-cache-dir pytest pytest-cov black ruff mypy

WORKDIR /opt/airflow
ENV PYTHONPATH=/opt/airflow/src \
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/airflow_tests.db \
    AIRFLOW__CORE__LOAD_EXAMPLES=false
ENTRYPOINT []
