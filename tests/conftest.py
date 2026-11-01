import os
import shutil
import sys

import pytest


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark.sql")
    if not (os.environ.get("JAVA_HOME") or shutil.which("java")):
        pytest.skip("Java is required for the Spark tests")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        pyspark.SparkSession.builder.master("local[2]")
        .appName("transforms-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
