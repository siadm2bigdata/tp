from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("KafkaCSVtoSpark").getOrCreate()

# 1️⃣ Lire CSV généré par le consumer Kafka
df = spark.read.csv("/data/raw/orders.csv").toDF("order_id", "amount", "country", "timestamp")

# 2️⃣ Nettoyage simple
df_clean = df.withColumn("amount", col("amount").cast("double")).filter(col("amount") > 0)

# 3️⃣ Sauvegarde clean en Parquet
df_clean.write.mode("overwrite").parquet("/data/clean/orders")

# 4️⃣ Agrégation par pays
df_clean.groupBy("country") \
    .sum("amount") \
    .withColumnRenamed("sum(amount)", "total_ca") \
    .write.mode("overwrite").parquet("/data/agg/orders_by_country")

# 5️⃣ Lecture pour vérification
df_clean.show()
spark.read.parquet("/data/agg/orders_by_country").show()
