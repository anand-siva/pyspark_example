from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, count


spark = (
    SparkSession.builder
    .appName("RevenueByState")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

df = spark.read.json("s3a://spark-demo/transactions/")

result = (
    df.groupBy("state")
    .agg(
        count("*").alias("transaction_count"),
        spark_sum("amount").alias("total_revenue")
    )
    .orderBy("state")
)

result.show(truncate=False)

spark.stop()
