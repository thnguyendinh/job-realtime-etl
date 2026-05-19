```markdown
# Job Realtime ETL Pipeline

[![Docker](https://img.shields.io/badge/Docker-20.10%2B-blue)](https://docker.com)
[![Spark](https://img.shields.io/badge/Spark-3.5.1-orange)](https://spark.apache.org)
[![Kafka](https://img.shields.io/badge/Kafka-7.5.0-black)](https://kafka.apache.org)
[![MySQL](https://img.shields.io/badge/MySQL-8.0-blue)](https://mysql.com)
[![Grafana](https://img.shields.io/badge/Grafana-9.5-yellow)](https://grafana.com)

> **Một pipeline xử lý dữ liệu realtime mô phỏng luồng sự kiện việc làm (view, click, apply, ...) từ generator → S3 → Spark → MySQL → Grafana.**

---

## Tổng quan

Dự án này xây dựng một hệ thống xử lý dữ liệu near‑realtime (gần thực) hoàn chỉnh, bao gồm:

- **Fake Data Generator** – sinh sự kiện giả (6‑8 events/giây) dựa trên danh sách job thật, upload lên **AWS S3** theo cấu trúc phân vùng thời gian.
- **Apache Spark (PySpark)** – đọc dữ liệu từ S3, join với dimension, aggregate, và ghi vào **MySQL**.
- **Apache Kafka** – (tuỳ chọn) streaming tracking history hoặc làm message queue.
- **Apache Airflow** – (tuỳ chọn) orchestrate ETL workflow.
- **Grafana** – dashboard trực quan hóa dữ liệu từ MySQL, refresh tự động mỗi 30 giây.
- **Docker & Docker Compose** – đóng gói toàn bộ services, dễ dàng khởi chạy với một lệnh.
- **AWS EC2 + IAM Role** – chạy toàn bộ stack trên cloud, bảo mật bằng IMDSv2.

---

##  Kiến trúc hệ thống

![Architecture](https://via.placeholder.com/800x400?text=Architecture+Diagram+-+S3+%2B+Spark+%2B+Kafka+%2B+MySQL+%2B+Grafana)

> *Luồng dữ liệu:*  
> `Generator (fake events)` → `S3 raw/events/` → `Spark ETL` → `MySQL fact_job_events` → `Grafana Dashboard`

Hoặc với Kafka:  
> `Producer (tracking.csv)` → `Kafka topic job-tracking` → `Spark Streaming` → `MySQL`

---

## Bắt đầu nhanh

### Yêu cầu

- **AWS Account** (với S3 bucket và IAM Role gắn vào EC2)
- **EC2 instance** (t3.xlarge khuyến nghị, Ubuntu 22.04, 50GB gp3)
- Security Group mở các cổng: `22` (SSH), `3000` (Grafana), `8080` (Spark UI), `8081` (Airflow)
- **Docker & Docker Compose** đã cài đặt trên EC2
- **AWS CLI** (để tương tác với S3)

### Cấu hình biến môi trường (tuỳ chọn)

```bash
export BUCKET_NAME="an-job-realtime-lake"
export MYSQL_ROOT_PASSWORD="root"
```

---

## Cài đặt và chạy với Docker

### 1. Clone repository

```bash
git clone https://github.com/thnguyendinh/job-realtime-etl.git
cd job-realtime-etl
```

### 2. Khởi động toàn bộ các services

```bash
docker compose up -d
```

Kiểm tra trạng thái:

```bash
docker compose ps
```

### 3. Tạo bảng MySQL (nếu chưa có)

```bash
docker exec mysql mysql -uroot -proot job_dw -e "
CREATE TABLE IF NOT EXISTS fact_job_events (
    job_id INT,
    job_title VARCHAR(255),
    city_name VARCHAR(100),
    state VARCHAR(50),
    major_category VARCHAR(100),
    custom_track VARCHAR(50),
    event_count INT,
    avg_salary DOUBLE
);"
```

### 4. Upload dữ liệu tham chiếu lên S3

```bash
aws s3 cp search.csv s3://$BUCKET_NAME/raw/search.csv
aws s3 cp tracking.csv s3://$BUCKET_NAME/raw/tracking.csv
```

### 5. Chạy fake data generator (background)

```bash
nohup python3 generate_realtime_events.py > generator.log 2>&1 &
echo $! > generator.pid
```

Theo dõi log:

```bash
tail -f generator.log
```

### 6. Chạy ETP (Spark) lần đầu

```bash
docker exec spark spark-submit \
  --master local[*] \
  --driver-memory 2g \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,mysql:mysql-connector-java:8.0.33 \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  /opt/spark/apps/etl_job.py
```

### 7. (Tuỳ chọn) Chạy ETL Loop tự động mỗi 60 giây

```bash
chmod +x etl_loop.sh
nohup ./etl_loop.sh > etl_loop.log 2>&1 &
echo $! > etl_loop.pid
```

### 8. Thiết lập Grafana dashboard

Truy cập `http://<EC2_PUBLIC_IP>:3000` (đăng nhập `admin` / `admin`)

- **Add data source**: MySQL  
  Host: `mysql:3306` | Database: `job_dw` | User: `root` | Password: `root`

- **Import dashboard** (hoặc tự tạo panels) sử dụng các câu SQL mẫu trong tài liệu.

### 9. (Tuỳ chọn) Kafka streaming tracking data

```bash
# Tạo topic
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic job-tracking --partitions 3 --replication-factor 1

# Chạy producer
python3 kafka_producer.py
```

---

## Cấu trúc thư mục dự án

```
job-realtime-etl/
├── docker-compose.yml              # Định nghĩa services: spark, kafka, mysql, grafana, airflow
├── docker/
│   └── spark/
│       ├── Dockerfile              # Build image Spark custom (vì bitnami image bị xoá)
│       └── entrypoint.sh           # Script khởi động master/worker
├── spark-apps/
│   └── etl_job.py                  # PySpark ETL: đọc S3, join, agg, ghi MySQL
├── dags/                           # (Airflow DAGs – nếu dùng)
│   └── job_etl_dag.py
├── generate_realtime_events.py     # Fake generator (6-8 events/s, xóa dữ liệu cũ 7 ngày)
├── simulate_realtime.py            # Phiên bản cũ (giữ để tham khảo)
├── kafka_producer.py               # Replay tracking.csv vào Kafka
├── etl_loop.sh                     # Vòng lặp ETL với refresh credentials IMDSv2
├── cleanup_s3_old.sh               # Xoá file S3 cũ hơn 7 ngày (cron)
├── search.csv                      # Job listings (dimension)
├── tracking.csv                    # Historical user events
├── .gitignore
└── README.md
```
---

##  Dọn dẹp

Dừng tất cả containers:

```bash
docker compose down -v
```

Dừng generator và ETL loop:

```bash
kill $(cat generator.pid) 2>/dev/null
kill $(cat etl_loop.pid) 2>/dev/null
rm *.pid
```

Xoá sạch dữ liệu S3 cũ (nếu muốn):

```bash
aws s3 rm s3://$BUCKET_NAME/raw/events/ --recursive
```

---

##  Mở rộng & Tối ưu

- **Tăng throughput generator**: giảm `time.sleep(0.15)` xuống 0.05 để đạt ~20 events/s.
- **Spark streaming** thay vì batch: đọc từ Kafka topic thay vì quét S3.
- **Partition & bucketing** trên MySQL để tăng tốc query.
- **Tích hợp với AWS Glue / Athena** cho ad-hoc query trên data lake.
- **Sử dụng Iceberg** (đã có JAR) để quản lý schema evolution và time travel.

---

##  Giấy phép

MIT License – hoàn toàn miễn phí sử dụng cho mục đích học tập và sản xuất.

---

##  Tác giả
📧 thang.nd@example.com  *Data Engineer*
🔗 [GitHub](https://github.com/thnguyendinh)

---

##  Hỗ trợ

Nếu bạn thấy dự án hữu ích, hãy để lại ⭐ trên GitHub. Mọi ý kiến đóng góp hoặc báo lỗi xin tạo Issue.

---

###  *Từ ý tưởng đến dashboard chỉ với một lệnh `docker compose up -d`.*
```

---

Bạn có thể sao chép đoạn văn bản trên, lưu thành `README.md`, rồi chạy:

```bash
git add README.md
git commit -m "Add professional README"
git push
```

Sau đó truy cập repo GitHub sẽ thấy giao diện đẹp mắt với markdown đầy đủ. Chúc bạn thành công!
