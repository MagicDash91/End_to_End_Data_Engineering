ARG AIRFLOW_VERSION=3.2.1

# Java for the Spark driver (spark-submit runs in client mode inside this container).
FROM eclipse-temurin:17-jre AS jre

FROM apache/airflow:${AIRFLOW_VERSION}-python3.12
ARG AIRFLOW_VERSION
ARG PG_JDBC_VERSION=42.7.4

USER root
COPY --from=jre /opt/java/openjdk /opt/java
ENV JAVA_HOME=/opt/java PATH=/opt/java/bin:$PATH
RUN mkdir -p /opt/spark-jars \
 && curl -fsSL -o /opt/spark-jars/postgresql.jar \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar"

USER airflow
COPY requirements-airflow.txt requirements-dbt.txt /tmp/
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements-airflow.txt
# dbt lives in its own venv so its pins never fight Airflow's.
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt
ENV DBT_BIN=/home/airflow/dbt-venv/bin/dbt DBT_SEND_ANONYMOUS_USAGE_STATS=false
