


from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType


def main():
    spark = (
        SparkSession.builder
        .appName("SmartCityStreaming")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "software.amazon.awssdk:bundle:2.25.1")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", "xxxxx")
        .config("spark.hadoop.fs.s3a.secret.key", "xxxxx")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # Add S3 performance and resilience configs
        .config("spark.hadoop.fs.s3a.connection.timeout", "10000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "5")
        .config("spark.hadoop.fs.s3a.retry.limit", "10")
        .config("spark.hadoop.fs.s3a.committer.name", "directory")
        .config("spark.sql.adaptive.enabled", "false")  # Disable for streaming
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("INFO")  # Change to INFO for debugging

    # Your schemas remain the same...
    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True)
    ])

    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True)
    ])

    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("cameraId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("snapshot", StringType(), True)
    ])

    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", DoubleType(), True),
    ])

    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True)
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "broker:29092")
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                # Add Kafka resilience options
                .option("kafka.session.timeout.ms", "30000")
                .option("kafka.request.timeout.ms", "30000")
                .option("maxOffsetsPerTrigger", "100")  # Process in smaller batches
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"), schema).alias("data"))
                .select("data.*")
                .withWatermark("timestamp", "2 minutes")
                )

    def stream_writer(input: DataFrame, checkpointfolder, output):
        print(f"Starting stream writer for {output}")

        return (input.writeStream
                .format("parquet")
                .option("checkpointLocation", checkpointfolder)
                .option("path", output)
                .option("failOnDataLoss", "false")
                .outputMode("append")
                .start()
                )

    # Read from Kafka topics
    print("Reading from Kafka topics...")
    vehicleDF = read_kafka_topic('vehicle_data', vehicleSchema)
    gpsDF = read_kafka_topic('gps_data', gpsSchema)
    trafficDF = read_kafka_topic('traffic_data', trafficSchema)
    weatherDF = read_kafka_topic('weather_data', weatherSchema)
    emergencyDF = read_kafka_topic('emergency_data', emergencySchema)

    # Clear old checkpoints for fresh start
    print("Starting fresh streams...")

    # Write streams to S3
    query1 = stream_writer(vehicleDF,
                           's3a://spark-streaming-lontobirm/checkpoints/vehicle_data',
                           's3a://spark-streaming-lontobirm/data/vehicle_data')

    query2 = stream_writer(gpsDF,
                           's3a://spark-streaming-lontobirm/checkpoints/gps_data',
                           's3a://spark-streaming-lontobirm/data/gps_data')

    query3 = stream_writer(trafficDF,
                           's3a://spark-streaming-lontobirm/checkpoints/traffic_data',
                           's3a://spark-streaming-lontobirm/data/traffic_data')

    query4 = stream_writer(weatherDF,
                           's3a://spark-streaming-lontobirm/checkpoints/weather_data',
                           's3a://spark-streaming-lontobirm/data/weather_data')

    query5 = stream_writer(emergencyDF,
                           's3a://spark-streaming-lontobirm/checkpoints/emergency_data',
                           's3a://spark-streaming-lontobirm/data/emergency_data')

    print("All streaming queries started successfully!")
    print("Writing data from Kafka to S3...")
    print("Topics: vehicle_data, gps_data, traffic_data, weather_data, emergency_data")
    print("Press Ctrl+C to stop the streaming application")

    # Wait for termination
    query5.awaitTermination()


if __name__ == "__main__":
    main()