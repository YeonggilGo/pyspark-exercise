from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

session = SparkSession.builder.getOrCreate()
session.sparkContext.setLogLevel("ERROR")

scheme = StructType(
    [
        StructField("customer_id", IntegerType(), True),
        StructField("item_id", IntegerType(), True),
        StructField("item_price", FloatType(), True),
    ]
)

lines = session.read.option("header", True).schema(scheme).csv("../src/resources/customer_orders.csv")
infos = lines.groupBy("customer_id")\
    .agg(F.round(F.sum("item_price"), 2).alias("spent_money"))\
    .sort("customer_id")