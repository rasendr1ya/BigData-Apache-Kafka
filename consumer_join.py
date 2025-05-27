from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, window, when, concat, lit
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

# Buat Spark session dengan Kafka connector
spark = SparkSession.builder \
    .appName("JoinSensorStream") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema untuk suhu
schema_suhu = StructType() \
    .add("gudang_id", StringType()) \
    .add("suhu", IntegerType())

# Schema untuk kelembaban
schema_kelembaban = StructType() \
    .add("gudang_id", StringType()) \
    .add("kelembaban", IntegerType())

# Baca stream suhu
suhu_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .option("startingOffsets", "latest") \
    .load()

# Parse data suhu
suhu_parsed = suhu_df.selectExpr("CAST(value AS STRING) as json", "timestamp") \
    .select(from_json("json", schema_suhu).alias("data"), "timestamp") \
    .select("data.*", "timestamp") \
    .withWatermark("timestamp", "10 seconds")

# Baca stream kelembaban
kelembaban_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .option("startingOffsets", "latest") \
    .load()

# Parse data kelembapan
kelembaban_parsed = kelembaban_df.selectExpr("CAST(value AS STRING) as json", "timestamp") \
    .select(from_json("json", schema_kelembaban).alias("data"), "timestamp") \
    .select("data.*", "timestamp") \
    .withWatermark("timestamp", "10 seconds")

# Join berdasarkan gudang_id dalam window 10 detik
joined_df = suhu_parsed.alias("s").join(
    kelembaban_parsed.alias("k"),
    expr("""
        s.gudang_id = k.gudang_id AND 
        s.timestamp >= k.timestamp - interval 10 seconds AND 
        s.timestamp <= k.timestamp + interval 10 seconds
    """)
)

# Buat kolom status berdasarkan kondisi
result_df = joined_df.select(
    col("s.gudang_id").alias("gudang_id"),
    col("s.suhu").alias("suhu"),
    col("k.kelembaban").alias("kelembaban")
).withColumn("status", 
    when((col("suhu") > 80) & (col("kelembaban") > 70), "Bahaya tinggi! Barang berisiko rusak")
    .when(col("suhu") > 80, "Suhu tinggi, kelembaban normal")
    .when(col("kelembaban") > 70, "Kelembaban tinggi, suhu aman")
    .otherwise("Aman")
)

# Custom output function untuk format yang lebih baik
def process_batch(df, epoch_id):
    if not df.isEmpty():
        print("\n" + "="*60)
        print("MONITORING GABUNGAN SENSOR GUDANG")
        print("="*60)
        
        rows = df.collect()
        for row in rows:
            gudang_id = row['gudang_id']
            suhu = row['suhu']
            kelembaban = row['kelembaban']
            status = row['status']
            
            if status == "Bahaya tinggi! Barang berisiko rusak":
                print(f"\n[PERINGATAN KRITIS]")
            
            print(f"Gudang {gudang_id}:")
            print(f"- Suhu: {suhu}°C")
            print(f"- Kelembaban: {kelembaban}%")
            print(f"- Status: {status}")
            print("-" * 40)

print("=== MONITORING GABUNGAN SENSOR GUDANG ===")
print("Menunggu data dari kedua sensor...")

# Output menggunakan foreachBatch untuk custom formatting
query = result_df.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping stream...")
    query.stop()
    spark.stop()