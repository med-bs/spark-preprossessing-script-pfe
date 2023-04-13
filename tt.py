#pip install pyspark

import pyspark
print(pyspark.__version__)

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 pyspark-shell'

from pyspark.sql import SparkSession

# create a Spark session
spark = SparkSession.builder.appName("kafkaConsumer").getOrCreate()

# create a Kafka stream
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "dbserver2.inventory.addresses") \
    .load()


# select the value column from the Kafka stream
value_df = df.selectExpr("CAST(value AS STRING)")

# print the value column to the console
query = value_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# wait for the query to terminate
query.awaitTermination()