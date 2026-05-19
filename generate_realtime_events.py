import boto3, json, random, time, pandas as pd
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
s3 = boto3.client('s3')
BUCKET = 'an-job-realtime-lake'

# Đọc danh sách job_id thật từ search.csv (loại bỏ dòng lỗi)
df = pd.read_csv('search.csv')
REAL_JOB_IDS = df['job_id'].dropna().astype(int).tolist()
# Lấy title, city_name tương ứng để sinh dữ liệu thống nhất (không tạo rác)
JOB_INFO = df[['job_id', 'title', 'city_name']].dropna().to_dict('records')

print("🚀 Generator started - 6-8 events/sec, cleaning old data every hour")
count = 0

def cleanup_old_files():
    """Xóa file JSON cũ hơn 7 ngày trên S3"""
    cutoff = datetime.utcnow() - timedelta(days=7)
    prefix = "raw/events/"
    try:
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        if 'Contents' not in response:
            return
        for obj in response['Contents']:
            # Lấy timestamp từ key (giả sử key có dạng raw/events/YYYY/MM/DD/HH/uuid.json)
            key = obj['Key']
            parts = key.split('/')
            if len(parts) >= 5 and parts[2].isdigit() and parts[3].isdigit() and parts[4].isdigit():
                date_str = f"{parts[2]}-{parts[3]}-{parts[4]}"
                obj_date = datetime.strptime(date_str, "%Y-%m-%d")
                if obj_date < cutoff:
                    s3.delete_object(Bucket=BUCKET, Key=key)
                    print(f"🗑️ Deleted old file: {key}")
    except Exception as e:
        print(f"Cleanup error: {e}")

last_cleanup = datetime.utcnow()
while True:
    try:
        # Mỗi giờ dọn dẹp 1 lần
        if datetime.utcnow() - last_cleanup > timedelta(hours=1):
            cleanup_old_files()
            last_cleanup = datetime.utcnow()

        # Lấy job info thật
        job_info = random.choice(JOB_INFO)
        job_id = job_info['job_id']
        title = job_info['title']
        city = job_info['city_name']

        event = {
            "event_id": fake.uuid4(),
            "job_id": int(job_id),
            "user_id": random.randint(1, 15000),
            "event_type": random.choice(['view','click','apply','alive','conversion']),
            "ts": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "bn": random.choice(['Chrome 103','Firefox','Safari','Edge']),
            "city_name": str(city),
            "title": str(title),
            "salary": random.randint(1500, 9500)
        }

        key = f"raw/events/{datetime.utcnow().strftime('%Y/%m/%d/%H')}/{event['event_id']}.json"
        s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(event))

        count += 1
        if count % 20 == 0:
            print(f"✅ {count} events | {event['event_type']} | Job {job_id} | {title[:20]}")

        time.sleep(0.15)  # ~6-7 events/sec
    except Exception as e:
        print(f"Error: {e}")
        time.sleep(1)
