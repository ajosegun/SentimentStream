import pyspark
from pyspark.sql import SparkSession
# from pyspark.sql.functions import window, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import from_json, col, udf, when
from config.config import config
import time
import openai
from openai import AzureOpenAI
from azure.ai.inference import ChatCompletionsClient
from azure.core.credentials import AzureKeyCredential



def sentiment_analysis_azure_ai_hub(comment) -> str:
    if comment:

        client = ChatCompletionsClient(
            endpoint= config['azure_ai_hub']['endpoint'],
            credential=AzureKeyCredential(config['azure_ai_hub']['credential'])
        )

        model_info = client.get_model_info()

        prompt = """You're a machine learning model with a task of classifying comments into POSITIVE, NEGATIVE, NEUTRAL.
                            You are to respond with one word from the option specified above, do not add anything else.
                            Here is the comment:
                        """

        payload = {
        "messages": [
            {
            "role": "assistant",
            "content": prompt
            },
            {
            "role": "user",
            "content": comment
            }
        ],
        "max_tokens": 20,
        "temperature": 0.5,
        "top_p": 0.1,
        "presence_penalty": 0
        }
        response = client.complete(payload)

        return response.choices[0].message.content

    return "Empty"

def sentiment_analysis_azure_openai(comment) -> str:
    if comment:

        client = AzureOpenAI(
            api_key = config['azure_openai']['api_key'],  
            api_version = config['azure_openai']['api_version'],
            azure_endpoint = config['azure_openai']['azure_endpoint'],
            )
        
        #  Please pass one of `api_key`, `azure_ad_token`, `azure_ad_token_provider`, or the `AZURE_OPENAI_API_KEY` or `AZURE_OPENAI_AD_TOKEN` environment variables.
        
        prompt = """You're a machine learning model with a task of classifying comments into POSITIVE, NEGATIVE, NEUTRAL.
                    You are to respond with one word from the option specified above, do not add anything else.
                    Here is the comment:
                """

        response = client.chat.completions.create(
            model=config['azure_openai']['deployment_name'], 
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": comment}
            ]
        )

        #print(response)
        print(response.model_dump_json(indent=2))
        print(response.choices[0].message.content)

        return response.choices[0].message.content
    return "Empty"



def sentiment_analysis_openai(comment) -> str:
    if comment:
        openai.api_key = config['openai']['api_key']
        completion = openai.ChatCompletion.create(
            model='gpt-3.5-turbo',
            messages = [
                {
                    "role": "system",
                    "content": """
                        You're a machine learning model with a task of classifying comments into POSITIVE, NEGATIVE, NEUTRAL.
                        You are to respond with one word from the option specified above, do not add anything else.
                        Here is the comment:
                        
                        {comment}
                    """.format(comment=comment)
                }
            ]
        )
        return completion.choices[0].message['content']
    return "Empty"

def start_streaming(spark):
    # Create a streaming DataFrame that reads from a socket
    topic = "customers_review"
    while True:
        try:
            stream_df = (
                spark.readStream.format("socket")
                .option("host", "0.0.0.0")
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

            ## use one of these 3 models
            sentiment_analysis_udf = udf(sentiment_analysis_azure_ai_hub, StringType())
            # sentiment_analysis_udf = udf(sentiment_analysis_azure_openai, StringType())
            # sentiment_analysis_udf = udf(sentiment_analysis_openai, StringType())
            
            stream_df = stream_df.withColumn("feedback", 
                                                when(col('text').isNotNull(), sentiment_analysis_udf(col('text')))
                                                .otherwise(None)
                                            )
            

            kafka_df = stream_df.selectExpr("CAST(review_id AS STRING) AS key", "to_json(struct(*)) AS value")

            query = (kafka_df.writeStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", config['kafka']['bootstrap.servers'])
                    .option("kafka.security.protocol", config['kafka']['security.protocol'])
                    .option('kafka.sasl.mechanism', config['kafka']['sasl.mechanism'])
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