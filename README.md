# Laporan Tugas Apache Kafka

## Disusun Oleh:
|             Nama              |     NRP    |
|-------------------------------|------------|
| Danar Bagus Rasendriya        | 5027231055 |
***
### Overview Masalah
Terdapat sebuah perusahaan mempunyai beberapa gudang penyimpanan yang menyimpan barang sensitif seperti makanan, obat-obatan, dan elektronik. Gudang-gudang tersebut dilengkapi dengan dua jenis sensor:
- Sensor Suhu
- Sensor Kelembaban
Sensor akan mengirimkan data setiap detik. Perusahaan ingin memantau kondisi gudang secara real-time untuk mencegah kerusakan barang akibat suhu terlalu tinggi atau kelembaban berlebih.
***
### Solusi
Membuat sistem monitoring gudang secara real-time yang melakukan record data suhu dan kelembaban dengan menggunakan Apache Kafka (data streaming) dan PySpark (pemrosesan data). Data suhu dan kelembaban dari beberapa gudang akan dianalisis untuk menghasilkan peringatan jika terjadi kondisi kritis.
***
### Komponen sistem
1, Producer (Python)
- kelembapan_producer.py = Output data kelembaban
- suhu_producer.py = Output data suhu

2. Consumer (Python)
- consumer_filter.py = Define Constraints
- comnsumer_join.py = Join antara dua stream
***
### Prerequisite
- Docker & Docker Compose
- Java 11+ (Optional)
`brew install openjdk@11`
- Python
- PySpark
- Kafka (Optional, via Docker juga bisa)
`brew install kafka`
***
### Pengerjaan
- Install Prasyarat
- Start Kafka dan Zookeeper di dua terminal yang berbeda

`brew services start zookeeper`

`brew services start kafka`
***
### Soal
#### 1. Buat Topik Kafka
Buat dua topik di Apache Kafka:
- sensor-suhu-gudang
- sensor-kelembaban-gudang

##### Output
![Screenshot 2025-05-27 at 00 40 07](https://github.com/user-attachments/assets/c5f5127f-66dd-49cf-bd36-e8a6e32b8ef5)

```
docker exec -it kafka kafka-topics --create --topic sensor-suhu-gudang --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

docker exec -it kafka kafka-topics --create --topic sensor-kelembaban-gudang --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```
***
#### 2. Simulasikan Data Sensor (Producer Kafka)
Buat dua Kafka producer terpisah:
- Producer Suhu
```
from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

gudang_ids = ['G1', 'G2', 'G3']

while True:
    for gid in gudang_ids:
        suhu = random.randint(75, 90)
        data = {"gudang_id": gid, "suhu": suhu}
        producer.send('sensor-suhu-gudang', value=data)
        print("Suhu terkirim:", data)
    time.sleep(1)
```
##### Output
![Screenshot 2025-05-27 at 01 08 00](https://github.com/user-attachments/assets/0c457359-b324-40bd-bedc-ddec43ea8784)


- Producer Kelembaban
```
from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

gudang_ids = ['G1', 'G2', 'G3']

while True:
    for gid in gudang_ids:
        kelembaban = random.randint(65, 80)
        data = {"gudang_id": gid, "kelembaban": kelembaban}
        producer.send('sensor-kelembaban-gudang', value=data)
        print("Kelembaban terkirim:", data)
    time.sleep(1)
```
##### Output
![Screenshot 2025-05-27 at 01 07 29](https://github.com/user-attachments/assets/5e3265fc-85bf-43ef-9ac4-c12d0da3b6ba)
***
#### 3. Olah Data dengan PySpark
- Buat PySpark Consumer

![Screenshot 2025-05-27 at 01 55 16](https://github.com/user-attachments/assets/82bc038c-4643-4a7d-9db5-d64b74e60fa0)
```
//Include Filtering (Nomor 3B)
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, concat, lit
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder \
    .appName("GudangMonitoring") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

// Schema
schema_suhu = StructType().add("gudang_id", StringType()).add("suhu", IntegerType())
schema_kelembapan = StructType().add("gudang_id", StringType()).add("kelembapan", IntegerType())

// Stream dari Kafka - suhu
suhu_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .option("startingOffsets", "latest") \
    .load()

suhu_parsed = suhu_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema_suhu).alias("data")).select("data.*")
peringatan_suhu = suhu_parsed.filter(col("suhu") > 80)

// Stream dari Kafka - kelembapan
kelembapan_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-kelembapan-gudang") \
    .option("startingOffsets", "latest") \
    .load()

kelembapan_parsed = kelembapan_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema_kelembapan).alias("data")).select("data.*")
peringatan_kelembapan = kelembapan_parsed.filter(col("kelembapan") > 70)

// Custom output functions untuk formatting yang lebih baik
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

// Output dengan format yang lebih sesuai
print("=== MONITORING GUDANG REAL-TIME ===")
print("Menunggu data peringatan...")

// Menggunakan foreachBatch untuk custom formatting
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
```
- Lakukan Filtering
![Screenshot 2025-05-27 at 02 22 25](https://github.com/user-attachments/assets/cabc80fb-9746-4112-b59b-3c212559ea32)
***
#### Gabungkan Stream dari Dua Sensor
```
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
```
#### Output
![Screenshot 2025-05-27 at 04 09 50](https://github.com/user-attachments/assets/a66675af-0ede-4052-8706-b958310552f3)


