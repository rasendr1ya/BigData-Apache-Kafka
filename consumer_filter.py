# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StringType, IntegerType

# spark = SparkSession.builder \
#     .appName("GudangMonitoring") \
#     .master("local[*]") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # Schema
# schema_suhu = StructType().add("gudang_id", StringType()).add("suhu", IntegerType())
# schema_kelembapan = StructType().add("gudang_id", StringType()).add("kelembapan", IntegerType())

# # Stream dari Kafka - suhu
# suhu_df = spark.readStream.format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "sensor-suhu-gudang") \
#     .option("startingOffsets", "latest") \
#     .load()

# suhu_parsed = suhu_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema_suhu).alias("data")).select("data.*")
# peringatan_suhu = suhu_parsed.filter(col("suhu") > 80)

# # Stream dari Kafka - kelembapan
# kelembapan_df = spark.readStream.format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "sensor-kelembapan-gudang") \
#     .option("startingOffsets", "latest") \
#     .load()

# kelembapan_parsed = kelembapan_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema_kelembapan).alias("data")).select("data.*")
# peringatan_kelembapan = kelembapan_parsed.filter(col("kelembapan") > 70)

# # Output
# query_suhu = peringatan_suhu.writeStream.outputMode("append").format("console").start()
# query_kelembapan = peringatan_kelembapan.writeStream.outputMode("append").format("console").start()

# query_suhu.awaitTermination()
# query_kelembapan.awaitTermination()

#Inlude Filtering (Nomor 3B)
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, concat, lit
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder \
    .appName("GudangMonitoring") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema
schema_suhu = StructType().add("gudang_id", StringType()).add("suhu", IntegerType())
schema_kelembapan = StructType().add("gudang_id", StringType()).add("kelembapan", IntegerType())

# Stream dari Kafka - suhu
suhu_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .option("startingOffsets", "latest") \
    .load()

suhu_parsed = suhu_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema_suhu).alias("data")).select("data.*")
peringatan_suhu = suhu_parsed.filter(col("suhu") > 80)

# Stream dari Kafka - kelembapan
kelembapan_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-kelembapan-gudang") \
    .option("startingOffsets", "latest") \
    .load()

kelembapan_parsed = kelembapan_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema_kelembapan).alias("data")).select("data.*")
peringatan_kelembapan = kelembapan_parsed.filter(col("kelembapan") > 70)

# Custom output functions untuk formatting yang lebih baik
def process_suhu_batch(df, epoch_id):
    if not df.isEmpty():
        print("\n" + "="*50)
        rows = df.collect()
        for row in rows:
            print("[Peringatan Suhu Tinggi]")
            print(f"Gudang {row['gudang_id']}: Suhu {row['suhu']}°C")
            print()

def process_kelembapan_batch(df, epoch_id):
    if not df.isEmpty():
        print("\n" + "="*50)
        rows = df.collect()
        for row in rows:
            print("[Peringatan Kelembaban Tinggi]")
            print(f"Gudang {row['gudang_id']}: Kelembaban {row['kelembapan']}%")
            print()

# Output dengan format yang lebih sesuai
print("=== MONITORING GUDANG REAL-TIME ===")
print("Menunggu data peringatan...")

# Menggunakan foreachBatch untuk custom formatting
query_suhu = peringatan_suhu.writeStream \
    .outputMode("append") \
    .foreachBatch(process_suhu_batch) \
    .start()

query_kelembapan = peringatan_kelembapan.writeStream \
    .outputMode("append") \
    .foreachBatch(process_kelembapan_batch) \
    .start()

query_suhu.awaitTermination()
query_kelembapan.awaitTermination()