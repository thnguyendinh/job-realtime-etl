#!/bin/bash
BUCKET="an-job-realtime-lake"
PREFIX="raw/events/"
DAYS=7

# Lấy danh sách file và xóa những file có ngày < (now - DAYS)
aws s3 ls s3://$BUCKET/$PREFIX --recursive | while read -r line; do
    file_date=$(echo $line | awk '{print $1}')
    file_key=$(echo $line | awk '{print $4}')
    # Chuyển đổi date sang timestamp
    file_ts=$(date -d "$file_date" +%s 2>/dev/null)
    if [ -n "$file_ts" ]; then
        cutoff_ts=$(date -d "-$DAYS days" +%s)
        if [ $file_ts -lt $cutoff_ts ]; then
            aws s3 rm s3://$BUCKET/$file_key
        fi
    fi
done
