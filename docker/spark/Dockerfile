# Spark standalone master/worker. Python 3.12 + pyspark 3.5.5 on purpose: they must match the driver
# in the Airflow image, or PySpark refuses to run ("Python in worker has different version").
FROM eclipse-temurin:17-jre AS jre

FROM python:3.12-slim-bookworm
ARG PYSPARK_VERSION=3.5.5

COPY --from=jre /opt/java/openjdk /opt/java
ENV JAVA_HOME=/opt/java PATH=/opt/java/bin:$PATH
RUN apt-get update \
 && apt-get install -y --no-install-recommends procps tini \
 && rm -rf /var/lib/apt/lists/* \
 && pip install --no-cache-dir "pyspark==${PYSPARK_VERSION}"

ENV SPARK_HOME=/usr/local/lib/python3.12/site-packages/pyspark \
    PYSPARK_PYTHON=python3 \
    SPARK_NO_DAEMONIZE=true
WORKDIR /opt/spark-work
ENTRYPOINT ["tini", "--"]
