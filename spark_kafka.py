from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


# SparkSession.builder : SparkSession을 생성하기 위한 빌더를 만든다. Spark SQL을 사용하기 위해 필요하다.
# .appName() : 애플리케이션의 이름을 설정한다. 이 이름은 Spark UI 및 로그에 나타나며, 작업을 추적하고 모니터링하기 쉽게 만든다.
# .config("spark.streaming.stopGracefullyOnShutdown", "true") : 애플리케이션이 종료될때 스트리밍 작업이 완전히 처리된 후에 세션이 종료되도록 보장한다.
# .config("spark.sql.shuffle.partitions", "3") : groupBy나 join과 같은 작업중에 데이터를 셔플할때 사용되는 파티션의 수를 3으로 설정한다. 병렬처리수준을 제어하는데 도움이 됨
# 기본은 200이라고 한다.
# .getOrCreate() : 새로운 SparkSession을 생성하거나, 이미 존재하는 세션이 있으면 가져온다. 
spark = SparkSession \
    .builder \
    .appName("StructuredWordCount") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()

# Create DataFrame representing the stream of input lines from connection to localhost:9999
# spark.readStream : Spark의 실시간 데이터 스트리밍 데이터 소스를 정의한다. 
# .format("kafka") : 스트리밍 소스로 kafka를 지정한다. 
# .option("kafka.bootstrap.servers", "kafka:9092") : kafka 클러스터의 부트스트랩 서버를 지정한다. kafka:9092는 kafka 브로커의 호스트와 포트를 의미하며 Spark가 kafka에 연결하기 위한 진입점
# .option("subscribe", "quickstart") : 읽을 kafka 토픽을 지정한다. quickstart라는 토픽을 구독하여 데이터를 읽어오도록 설정한다.
# .option("startingOffsets", "earliest") : kafka에서 데이터를 읽을 시작 지점을 지정한다. "earlist"는 가장 이른 오프셋을 의미하며 스트리밍을 시작할때 가장 오래된 메시지부터 읽어온다.
# .load() : 스트리밍 데이터를 실제로 불러온다. 이 메서드가 호출되면 kafka에서 데이터를 읽기 시작하고 events라는 dataframe에 데이터를 담아 처리할 수 있다.
# 배치에 텀을 두지 않아서 데이터가 들어올때마다 마이크로 배치가 실행된다.
events = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "quickstart") \
    .option("startingOffsets", "earliest") \
    .load()

# # print schema
# events.printSchema()

# Split the lines into words
words = events.select(
   explode(
       split(events.value, " ")
   ).alias("word")
)

# Generate running word count
wordCounts = words.groupBy("word").count()

 # Start running the query that prints the running counts to the console
query = wordCounts \
    .writeStream \
    .option("checkpointLocation", "checkpoint") \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
