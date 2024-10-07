import pyspark
from pyspark.sql import SparkSession
# from pyspark.sql.functions import window, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import from_json, col
from config.config import config
import time

def start_streaming(spark):
    # Create a streaming DataFrame that reads from a socket
    topic = "customers_review"
    try:
        stream_df = (
            spark.readStream.format("socket")
            .option("host", "127.0.0.1")
            .option("port", 9999)
            .load()
        )

        schema = StructType([
            StructField(name="review_id", dataType=StringType()),
            StructField(name="user_id", dataType=StringType()),
            StructField(name="business_id", dataType=StringType()),
            StructField(name="stars", dataType=FloatType()),
            StructField(name="date", dataType=StringType()),
            StructField(name="text", dataType=StringType())
        ])

        # Print the schema of the stream_df
        print(stream_df.printSchema())

        stream_df = stream_df.select(from_json(col("value"), schema).alias("data")).select(("data.*"))

        kafka_df = stream_df.selectExpr("CAST(review_id AS STRING) AS key", "to_json(struct(*)) AS value")

        query = (kafka_df.writeStream
                .format("kafka")
                .option("kafka.bootstrap.servers", config['kafka']['bootstrap.servers'])
                .option("kafka.security.protocol", config['kafka']['security.protocol'])
                .option('kafka.sasl.mechanism', config['kafka']['sasl.mechanisms'])
                .option('kafka.sasl.jaas.config',
                        'org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" '
                        'password="{password}";'.format(
                            username=config['kafka']['sasl.username'],
                            password=config['kafka']['sasl.password']
                        ))
                .option('checkpointLocation', '/tmp/checkpoint')
                .option('topic', topic)
                .start() 
                .awaitTermination()
            )


    except Exception as e:
        print(f"Exception encountered: {e}. Retrying...")
        time.sleep(10)
        # start_streaming(spark)

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("SocketStreamConsumer") \
        .getOrCreate()

    start_streaming(spark)