from delta.tables import DeltaTable

from afrishop.spark_session import get_spark

spark = get_spark("check")
base = "/opt/airflow/data/curated"
for t in ["orders", "order_lines", "customers", "products", "payments", "deliveries"]:
    print(t, spark.read.format("delta").load(f"{base}/{t}").count())

q = spark.read.format("delta").load(f"{base}/_quarantine")
q.groupBy("source", "reason").count().orderBy("source", "reason").show(truncate=False)
DeltaTable.forPath(spark, f"{base}/orders").history().select("version", "operation").show()
spark.stop()