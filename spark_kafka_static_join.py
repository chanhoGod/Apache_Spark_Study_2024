from pyspark.sql import SparkSession
from pyspark.sql.types import  StructType, StructField, StringType
from pyspark.sql.functions import from_json, col, to_timestamp


# 넣어야할 정보
# 1. Cassandra의 설정 정보
# 2. Spark - Cassandra 연결 설정, SQL사용관련 라이브러리를 설정
spark = SparkSession \
    .builder \
    .appName("WaterMark") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
    .config("spark.sql.catalog.lh", "com.datastax.spark.connector.datasource.CassandraCatalog") \
    .getOrCreate()

# 스키마 생성
schema = StructType([
            StructField("create_date", StringType()),
            StructField("login_id", StringType())
        ])

# 카프카 리드스트림 설정
# .readStream : 카프카에서 스트리밍 방식으로 데이터를 읽어옴
# .format : 데이터 소스를 kafka로 지정
# .option("kafka.bootstrap.servers", "kafka:9092") : 카프카의 클러스터의 주소와 포트 설정
# .option("subscribe", "login_event") : kafka의 특정 토픽에 subscribe한다는 의미 해당 토픽에서 데이터를 읽어온다는 뜻
# .option("startingOffsets", "earliest") : kafka에서 토픽의 어느시점에서부터 데이터를 읽어오는지 지정, earliest 가장 처음의 오프셋부터 읽어옴, 현재시점은 latest
# .option("failOnDataLoss", "false") : kafka에서 데이터 손실이 발생했을때 스트리밍을 중단할지 지정, false면 손실이 발생하더라도 스트리밍을 중단하지 않음, 없을시 예외발생해서 중단됨
events = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "login_event") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()

# 카프카 이벤트에서 가져온 데이터를 사전에 지정한 스키마로 데이터를 구성함
value_df = events.select(
            col('key'),
            from_json(
                col("value").cast("string"), schema).alias("value"))

# create_date의 포맷 변경
timestamp_format = "yyyy-MM-dd HH:mm:ss"
event_df = value_df.select("value.*") \
                .withColumn("create_date", to_timestamp("create_date", timestamp_format))

# 카산드라에 있는 키스페이스와 테이블을 가져온다.
user_df = spark \
            .read \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "test_db") \
            .option("table", "users") \
            .load()

# rdbms처럼 inner join을 통해 서로의 데이터셋에서 가져온 데이터를 결합한 후 drop을 통해서 중복되는 컬럼은 제거함
join_df = event_df.join(
            user_df,
            event_df.login_id == user_df.login_id,
            "inner").drop(user_df.login_id)

output_df = join_df.select(
                col("login_id"),
                col("user_name"),
                col("create_date").alias("last_login"))

# foreachBatch에서 함수를 호출해서 사용할 수 있음 해당 함수를 사용해서 데이터를 유연하게 다룰 수 있음 
# batch_df : 매 배치에서 들어오는 데이터를 저장한 데이터 프레임, Spark 스트리밍은 실시간 데이터를 작은 배치 단위로 처리하기 때문에 각 배치 데이터가 들어올때마다 함수 호출
# .write.format("org.apache.spark.sql.cassandra") : cassandra에 데이터를 write하기 위한 포맷 설정
# .option("keyspace", "test_db") : 저장할 cassandra keyspace 지정
# .option("table", "users") : 저장할 cassandra table 지정
# .mode("append") : 데이터를 append하는 모드로 지정
# .save() : 데이터를 저장하는 명령어
# .show()를 사용해 저장 될때마다 저장된 데이터 출력
def cassandraWriter(batch_df, batch_id):
    batch_df \
        .write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "test_db") \
        .option("table", "users") \
        .mode("append") \
        .save()
    
    batch_df.show()


# output_df.writeStream : kafka와 cassandra의 데이터를 조인하여 만든 데이터 프레임을 스트리밍 방식으로 처리
# .foreachBatch(cassandraWriter) : 매 배치마다 cassandraWriter함수를 호출해 배치 데이터를 Cassandra에 저장
# .outputMode("update") : 스트리밍 데이터를 변경된 데이터만 처리
# .trigger(processingTime="5 seconds") : 스트리밍 데이터 처리를 5초마다 트리거
# .start() : 스트리밍 쿼리를 실행하는 명령어
query = output_df \
        .writeStream \
        .foreachBatch(cassandraWriter) \
        .outputMode("update") \
        .trigger(processingTime="5 seconds") \
        .start()

#query.awaitTermination() : 프로그램이 종료되지 않고 스트리밍 작업이 완료될때까지 대기하는 역할을 함
query.awaitTermination()