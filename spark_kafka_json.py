from pyspark.sql import SparkSession
from pyspark.sql.types import  StructType, StructField, StringType
from pyspark.sql.functions import expr, from_json, col, lower

spark = SparkSession \
    .builder \
    .appName("StructuredWordCount") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()

schema = StructType([
            StructField("city", StringType()),
            StructField("domain", StringType()),
            StructField("event", StringType())
        ])

# Create DataFrame representing the stream of input lines from connection to localhost:9999
# failOnDataLoss false는 kafka의 offsdet이 out of range가 되었을때 에러가 나는데 일단 false로 두어서 무시하고 진행
# events = spark \
#     .read \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:9092") \
#     .option("subscribe", "raw") \
#     .option("startingOffsets", "earliest") \
#     .option("failOnDataLoss", "false") \
#     .load()

events = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "raw") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

value_df = events.select(
            col('key'),
            from_json(
                col("value").cast("string"), schema).alias("value"))

# value_df.show()
# {"city":Urbana, "domain":"test.univercity", "event":"viewed"}

tf_df = value_df.selectExpr(
            'value.city',
            'value.domain',
            'value.event as behavior')

concat_df = tf_df.withColumn('lower_city', lower(col('city'))) \
                .withColumn('domain', expr('domain')) \
                .withColumn('behavior', expr('behavior')).drop('city')

output_df = concat_df.selectExpr(
                'null',
"""
    to_json(named_struct("lower_city", lower_city, "domain", domain, "behavior", behavior)) as value
""".strip())

# output_df.show()

# Start running the query that prints the running counts to the console
# 스트리밍을 사용해서 데이터를 transformed라는 토픽으로 넣을것
# 포맷은 카프카, 카프카 서버 설정 입력
# insert할거기 때문에 append로 넣음
query = output_df \
    .writeStream \
    .queryName("transformed writer") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("checkpointLocation", "checkpoint") \
    .option("topic", "transformed") \
    .outputMode("append") \
    .start()

query.awaitTermination()
