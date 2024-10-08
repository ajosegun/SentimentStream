#!/bin/bash

# Start the Spark Worker
bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077 &

# Wait for the Spark Worker to fully start
sleep 10

# Run the Python script
python /opt/bitnami/spark/jobs/spark-streaming.py
