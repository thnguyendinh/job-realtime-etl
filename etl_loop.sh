#!/bin/bash
# Refresh credentials mỗi vòng (token expire sau 1h)
refresh_creds() {
    IMDS_TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" \
      -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
    ROLE=$(curl -s -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" \
      http://169.254.169.254/latest/meta-data/iam/security-credentials/)
    CREDS=$(curl -s -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" \
      "http://169.254.169.254/latest/meta-data/iam/security-credentials/$ROLE")
    ACCESS=$(echo $CREDS | python3 -c "import sys,json; print(json.load(sys.stdin)['AccessKeyId'])")
    SECRET=$(echo $CREDS | python3 -c "import sys,json; print(json.load(sys.stdin)['SecretAccessKey'])")
    TOKEN=$(echo $CREDS  | python3 -c "import sys,json; print(json.load(sys.stdin)['Token'])")
}

echo "🔄 ETL Loop started — runs every 60 seconds"
RUN=0

while true; do
    RUN=$((RUN+1))
    START=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$START] Run #$RUN — refreshing credentials..."
    refresh_creds

    docker exec \
      -e AWS_ACCESS_KEY_ID=$ACCESS \
      -e AWS_SECRET_ACCESS_KEY=$SECRET \
      -e AWS_SESSION_TOKEN=$TOKEN \
      spark spark-submit \
        --master spark://spark:7077 \
        --packages "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,mysql:mysql-connector-java:8.0.33" \
        --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
        --conf "spark.hadoop.fs.s3a.access.key=$ACCESS" \
        --conf "spark.hadoop.fs.s3a.secret.key=$SECRET" \
        --conf "spark.hadoop.fs.s3a.session.token=$TOKEN" \
        --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider" \
        /opt/spark/apps/etl_job.py 2>&1 | grep -E "✅|❌|events|jobs|Aggregated|Complete|ERROR"

    END=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$END] Run #$RUN done. Sleeping 60s..."
    sleep 60
done
