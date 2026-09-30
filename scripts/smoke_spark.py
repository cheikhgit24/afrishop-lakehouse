from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder.master("local[2]")
    .appName("smoke")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.read.csv("/opt/airflow/data/source/orders.csv", header=True, inferSchema=True)
print("lignes lues :", df.count())
df.write.format("delta").mode("overwrite").save("/opt/airflow/data/curated/_smoke_orders")
print("lignes Delta :", spark.read.format("delta").load("/opt/airflow/data/curated/_smoke_orders").count())
spark.stop()