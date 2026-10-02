FROM afrishop-airflow:latest

RUN pip install --no-cache-dir --default-timeout=300 --retries 10 pytest pytest-cov black ruff mypy
RUN /opt/airflow/venvs/ge/bin/pip install --no-cache-dir --default-timeout=300 --retries 10 pytest

WORKDIR /opt/airflow
ENV PYTHONPATH=/opt/airflow/src \
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/airflow_tests.db \
    AIRFLOW__CORE__LOAD_EXAMPLES=false
ENTRYPOINT []
