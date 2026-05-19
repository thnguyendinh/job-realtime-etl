#!/bin/bash
set -e

if [ "$SPARK_MODE" = "master" ]; then
    echo "🚀 Starting Spark Master..."
    exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master \
        --host 0.0.0.0 \
        --port 7077 \
        --webui-port 8080
elif [ "$SPARK_MODE" = "worker" ]; then
    echo "🚀 Starting Spark Worker..."
    exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker \
        $SPARK_MASTER_URL
else
    exec "$@"
fi
