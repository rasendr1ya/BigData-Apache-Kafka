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

![Screenshot 2025-05-27 at 00 40 07](https://github.com/user-attachments/assets/c5f5127f-66dd-49cf-bd36-e8a6e32b8ef5)

```
docker exec -it kafka kafka-topics --create --topte sensor-suhu-gudang --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it kafka kafka-topcs --create --tople sensor-kelembaban-gudang --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```
