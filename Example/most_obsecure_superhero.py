from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

session = SparkSession.builder.appName("most_obsecure_superhero").getOrCreate()
session.sparkContext.setLogLevel("ERROR")

schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ]
)

heroes = session.read.option("sep", " ").schema(schema).csv("../src/resources/Marvel-names.txt")
lines = session.read.text("../src/resources/Marvel-graph.txt")

connections = lines.withColumn("id", F.split(F.col("value"), " ")[0]) \
    .withColumn("connections", F.size(F.split(F.col("value"), " ")) - 1) \
    .groupBy("id") \
    .agg(F.sum("connections").alias("connections"))

min_connections_count = connections.agg(F.min("connections")).first()[0]
min_connections = connections.where(col("connections") == min_connections_count)
result_df = min_connections.join(heroes, "id")
result_df.show()
