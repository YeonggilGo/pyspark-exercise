from pyspark.sql import SparkSession
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
heroes.show()