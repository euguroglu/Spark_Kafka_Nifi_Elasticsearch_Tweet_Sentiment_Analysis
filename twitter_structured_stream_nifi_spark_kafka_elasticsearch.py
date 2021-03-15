from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":

    def evaluate(avg):
        try:
            if avg < 0:
                return 'Negative'
            elif avg >0:
                return 'Positive'
            else:
                return 'Neutral'
        except:
            return 'Neutral'


    eval_udf = udf(evaluate, StringType())
	#Create Spark Context to Connect Spark Cluster
    spark = SparkSession \
        .builder \
        .appName("PythonStreamingKafkaTweetSentiment") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()
    #Preparing schema for tweets
    schema = StructType([
    	StructField("text", StringType()),
        StructField("sentimental_score", DoubleType()),
        StructField("timestamp", StringType())
    ])
    #Read from kafka topic named "twitter"
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "twitter") \
        .option("startingOffsets", "earliest") \
        .load()

    kafka_df.printSchema()

    value_df = kafka_df.select(from_json(col("value").cast("string"),schema).alias("value"))

    value_df.printSchema()

    explode_df = value_df.selectExpr("value.text",
                                      "value.sentimental_score", "value.timestamp")

#Set timeParserPolicy=Legacy to parse timestamp in given format
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
#Convert string type to timestamp
    transformed_df = explode_df.select('*') \
        .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

    transformed_df.printSchema()

    average_df = transformed_df.select(avg('sentimental_score').alias('average_sentimental_score'))


    evaluated_df = average_df.withColumn('status', eval_udf('average_sentimental_score')) \
                             .withColumn('timestamp', to_timestamp(lit(current_timestamp()))) \
                             .withColumn('date', to_date(col('timestamp')))

    evaluated_df.printSchema()

    kafka_df = evaluated_df.select("*")

    kafka_target_df = kafka_df.selectExpr("status as key",
                                                 "to_json(struct(*)) as value")

    kafka_target_df.printSchema()
    
    nifi_query = kafka_target_df \
            .writeStream \
            .queryName("Notification Writer") \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "twitter2") \
            .outputMode("complete") \
            .option("checkpointLocation", "chk-point-dir") \
            .start()

    nifi_query.awaitTermination()

    # window_query = evaluated_df.writeStream \
    # .format("console") \
    # .outputMode("complete") \
    # .option("checkpointLocation", "chk-point-dir") \
    # .trigger(processingTime="1 minute") \
    # .start()
    #
    # window_query.awaitTermination()
