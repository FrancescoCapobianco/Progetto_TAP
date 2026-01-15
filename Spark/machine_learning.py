from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

def train_kmeans_model(spark):
    csv_path = "/opt/spark-apps/training_data.csv"
    try:
        df_train = spark.read.option("header", "true") \
            .option("inferSchema", "true") \
            .csv(csv_path)
        
        df_train = df_train.withColumn("duration_session", col("duration_session").cast("integer"))

        assembler = VectorAssembler(inputCols=["duration_session"], outputCol="features")
        kmeans = KMeans(k=2, seed=1)
        pipeline = Pipeline(stages=[assembler, kmeans])

        # Addestramento
        model = pipeline.fit(df_train)

        # Cluster
        centers = model.stages[-1].clusterCenters()
        impulsive_cluster = 0 if centers[0][0] < centers[1][0] else 1
        return model, impulsive_cluster
    
    except Exception as E:
        print(f"Error Loading Training Data: {E} !!!")
        return None, 0

def main():
    spark = SparkSession \
        .builder \
        .appName("E-Commerce_Machine_Learning") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    ml_model, impulsive_idx = train_kmeans_model(spark)

    if ml_model is None: 
        print("Stopping due to training error")
        return

    # 1. Schema Input dal Topic_Analysis
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
        StructField("duration_session", IntegerType()),
        StructField("spender_type", StringType()), # Extra field
        StructField("stock_alert", StringType())   # Extra field
    ])

    # 2. Lettura da Topic_Analysis
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "e-commerce_kafkaserver:9092") \
        .option("subscribe", "Topic_Analysis") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    json_stream = raw_stream.select(
        from_json(col("value").cast("string"), json_scheme).alias("data")
    ).select("data.*")

    # 3. ML Prediction
    prediction_stream = ml_model.transform(json_stream)

    final_stream = prediction_stream.withColumn(
        "purchase_behaviour",
        when(col("prediction") == impulsive_idx, "Impulsive")
        .otherwise("Reasoned")
    ).drop("features", "prediction")

    # 4. Scrittura su Topic_Dashboard
    query = final_stream.select(to_json(struct(col("*"))).alias("value")) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "e-commerce_kafkaserver:9092") \
        .option("topic", "Topic_Dashboard") \
        .option("checkpointLocation", "/opt/spark-apps/checkpoint_ml") \
        .trigger(processingTime='20 seconds') \
        .start()
    
    print("--- Machine Learning Started ---")
    query.awaitTermination()

if __name__ == "__main__":
    main()
