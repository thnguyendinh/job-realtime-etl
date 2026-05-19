FROM python:3.11-slim

# Java (bắt buộc cho Spark)
RUN apt-get update && apt-get install -y \
    default-jdk wget curl procps \
    && rm -rf /var/lib/apt/lists/*

# PySpark + dependencies
RUN pip install pyspark==3.5.1 boto3 faker kafka-python mysql-connector-python

# Spark standalone scripts
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3

WORKDIR /opt/spark/apps

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
