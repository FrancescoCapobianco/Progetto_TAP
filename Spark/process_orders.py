from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType

def main():
    # 1. conf. iniziale
    spark = SparkSession.builder \
        .appName("E-Commerce_BehaviorAnalysis") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # 2. schema json: recupero dati
    json_scheme = StructType([
        StructField("order_id", IntegerType()),
        StructField("timestamp", StringType()),
        StructField("items", ArrayType(StructType([
            StructField("product_name", StringType()),
            StructField("quantity", IntegerType()),
            StructField("total", FloatType())
        ]))),
        StructField("customer", StructType([
            StructField("first_name", StringType()),
            StructField("email", StringType())
        ])),
        StructField("currency", StringType()),
        StructField("total", FloatType()),
        StructField("duration_session", IntegerType())
    ])

    # 3. Lettura da Topic_Purchase
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "e-commerce_kafkaserver:9092") \
        .option("subscribe", "Topic_Purchase") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load() # 'earliest' per prendere tutto lo storico riletto da FluentBit, altrimenti 'latest'
    
    # Processo col_Value -> String -> JSON
    json_stream = raw_stream.select(
        from_json(col("value").cast("string"), json_scheme).alias("data")
    ).select("data.*")

    # 4. Data Enrichment: 

    processed_stream = json_stream.withColumn(
        "spender_type",
        when(col("total") > 150, "High Spender").otherwise("Standard Spender")
        ).withColumn(
         "stock_alert",
         when(col("total") > 500, "High Demand").otherwise("Normal Demand")
        )
    
    # 5. Scrittura su Topic_Analysis
    query = processed_stream.select(
        to_json(struct(col("*"))).alias("value")) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "e-commerce_kafkaserver:9092") \
        .option("topic", "Topic_Analysis") \
        .option("checkpointLocation", "/opt/spark-apps/checkpoint_sql") \
        .trigger(processingTime='20 seconds') \
        .start()
    
    print("--- Streaming Behaviour_Analysis started ---")
    query.awaitTermination()

if __name__ == "__main__":
    main()
