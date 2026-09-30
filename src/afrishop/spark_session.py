"""Creation d'une session Spark locale avec Delta Lake active."""

from __future__ import annotations

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark(app_name: str = "afrishop") -> SparkSession:
    builder = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark