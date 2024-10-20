from pyspark.sql import SparkSession
from pyspark.sql.types import  StructType, StructField, StringType
from pyspark.sql.functions import from_json, col, to_timestamp

spark = SparkSession \
    .builder \
    .appName("StreamingJoin") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()

# 데이터를 받아올 스키마를 두개를 만듬, 두가지 토픽에서 두개의 다른 형식의 데이터가 들어올 것
impression_schema = StructType([
                        StructField("placement_id", StringType()),
                        StructField("uuid", StringType()),
                        StructField("create_date", StringType()),
                        StructField("campaign", StringType())
                    ])

click_schema = StructType([
                    StructField("placement_id", StringType()),
                    StructField("uuid", StringType()),
                    StructField("create_date", StringType())
                ])

# impression 이벤트에 대한 토픽 생성
impression_events = spark \
                    .readStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", "kafka:9092") \
                    .option("subscribe", "impression") \
                    .option("startingOffsets", "earliest") \
                    .option("failOnDataLoss", "false") \
                    .load()

# 이벤트를 가져와서 스키마에 매핑, create_date 포맷 지정
# 워터마크는 30분
timestamp_format = "yyyy-MM-dd HH:mm:ss"
impressions_df = impression_events.select(
                    col('key'),
                    from_json(
                        col("value").cast("string"), impression_schema).alias("value")) \
                    .select(
                        'value.*') \
                    .withColumn("create_date", to_timestamp("create_date", timestamp_format)) \
                    .withWatermark("create_date", "30 minutes")


# 클릭 이벤트 가져오기 마찬가지로 create_date 포맷 지정 
# 워터마크는 10분
click_events = spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:9092") \
                .option("subscribe", "click") \
                .option("startingOffsets", "earliest") \
                .option("failOnDataLoss", "false") \
                .load()

timestamp_format = "yyyy-MM-dd HH:mm:ss"
clicks_df = click_events.select(
                    col('key'),
                    from_json(
                        col("value").cast("string"), click_schema).alias("value")) \
                    .select(
                        'value.*') \
                    .withColumn("create_date", to_timestamp("create_date", timestamp_format)) \
                    .withWatermark("create_date", "10 minutes")

# impression과 click의 조인을 진행, uuid를 기준으로 진행하고 중복되는 uuid유ㅏ placement_id는 제거
join_df = impressions_df.join(
            clicks_df,
            impressions_df.uuid == clicks_df.uuid,
            "inner").drop(clicks_df.uuid).drop(clicks_df.placement_id)

# Start running the query that prints the running counts to the console
query = join_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

query.awaitTermination()
