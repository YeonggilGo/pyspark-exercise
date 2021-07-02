from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

session = SparkSession.builder.master("local").appName("min_temp").getOrCreate()
session.sparkContext.setLogLevel("ERROR")

columns = StructType(
    [
        StructField("geo_id", StringType(), True),
        StructField("date", IntegerType(), True),
        StructField("measure_type", StringType(), True),
        StructField("temperature", FloatType(), True),
    ]
)

temp_infos = session.read.schema(columns).csv("../src/resources/1800_weather.csv")

min_temps = temp_infos.filter(temp_infos.measure_type == "TMIN")
geo_min_temp = min_temps.groupBy("geo_id") \
    .min("temperature") \
    .show()
