from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, length, lit

spark = SparkSession.builder.appName("JobRealtimeETL").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1. Đọc dimension jobs (search.csv)
df_jobs = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("s3a://an-job-realtime-lake/raw/search.csv") \
    .select(
        col("job_id").alias("dim_job_id"),
        col("title").alias("job_title"),
        col("company_name"),
        col("city_name"),
        col("state"),
        col("major_category"),
        col("pay_from"),
        col("pay_to")
    )

# 2. Đọc tracking history
df_hist = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("s3a://an-job-realtime-lake/raw/tracking.csv")

# Đảm bảo có cột salary (thêm null nếu thiếu)
if "salary" not in df_hist.columns:
    df_hist = df_hist.withColumn("salary", lit(None).cast("double"))

# 3. Đọc sự kiện fake (nếu có)
print("📦 Reading fake realtime events from S3...")
try:
    df_fake = spark.read.json("s3a://an-job-realtime-lake/raw/events/*/*/*/*.json")
    print(f"   → {df_fake.count()} fake events loaded")
    # Đảm bảo df_fake có các cột cần thiết
    for col_name in ["job_id", "custom_track", "salary"]:
        if col_name not in df_fake.columns:
            df_fake = df_fake.withColumn(col_name, lit(None))
except Exception as e:
    df_fake = None
    print(f"   → No fake events yet: {e}")

# 4. Gộp dữ liệu
df_all = df_hist.unionByName(df_fake, allowMissingColumns=True) if df_fake is not None else df_hist

# 5. Chọn cột cần thiết
df_events = df_all.select("job_id", "custom_track", "salary")

# 6. Join với dimension jobs
df_joined = df_events.join(df_jobs, df_events.job_id == df_jobs.dim_job_id, "left") \
                     .drop("dim_job_id")

# 7. Aggregation
df_agg = df_joined.groupBy("job_title", "city_name", "state", "major_category", "custom_track") \
    .agg(
        count("*").alias("event_count"),
        avg("salary").alias("avg_salary")
    )

# 8. Lọc bỏ job title rác
df_clean = df_agg.filter(
    (col("job_title").isNotNull()) &
    (~col("job_title").rlike("(?i)^(create job|123|test|dummy)$")) &
    (~col("job_title").rlike("^[0-9]+$")) &
    (length(col("job_title")) > 2)
)

# 9. Ghi vào MySQL
df_clean.write.format("jdbc") \
    .option("url", "jdbc:mysql://mysql:3306/job_dw") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "fact_job_events") \
    .option("user", "root") \
    .option("password", "root") \
    .mode("overwrite") \
    .save()

print("✅ ETL job finished successfully!")
spark.stop()
