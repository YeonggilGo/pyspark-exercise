from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

session = SparkSession.builder.master("local").appName("SparkSQL").getOrCreate()
session.sparkContext.setLogLevel("ERROR")

# CSV
# orders = session.read.option("header", True).csv("../src/resources/customer_orders.csv")

# txt
fake_person = session.sparkContext.textFile("../src/resources/fake_person.txt")

# txt with schema

schema = StructType(
    [StructField("id", IntegerType(), True), StructField("name", StringType(), True)]
)
names = session.read.option("sep", " ").schema(schema).csv("../src/resources/Marvel-names.txt")
names.show()