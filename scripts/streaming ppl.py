from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, expr, sum, date_format
from pyspark.sql.types import StructType, StructField, StringType

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("File Streaming Demo") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
        .config("spark.sql.shuffle.partitions", 2) \
        .getOrCreate()

    logger = Log4j(spark)

    schema = StructType([
        StructField("CreatedTime", StringType()),
        StructField("Type", StringType())
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "test") \
        .option("startingOffsets", "earliest") \
        .load()

    value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value"))

    c_df = value_df.select("value.*") \
        .withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("F", expr("case when Type == 'F' then 1 else 0 end")) \
        .withColumn("M", expr("case when Type == 'M' then 1 else 0 end"))

    window_agg_df = c_df \
        .groupBy(
            window(col("CreatedTime"), "4 second"),
            col("Type")) \
        .agg(sum("F").alias("TotalFemale"),
             sum("M").alias("TotalMale"))

    output_df = window_agg_df.select("window.start", "TotalFemale", "TotalMale") \
        .withColumn("start", date_format(col("start"), "yyyy-MM-dd HH:mm:ss"))

    kafka_target_df = output_df.selectExpr("start as key",
                                           """to_json(named_struct(
                                           'TotalMale', TotalMale,
                                           'TotalFemale', TotalFemale)) as value""")

    window_query = kafka_target_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "result") \
        .outputMode("update") \
        .option("checkpointLocation", "chk-point-dir") \
        .trigger(processingTime="4 second") \
        .start()

        # .format("console") \
        # .outputMode("update") \
        # .option("checkpointLocation", "chk-point-dir") \
        # .trigger(processingTime="4 second") \
        # .start()



    logger.info("Waiting for Query")
    window_query.awaitTermination()