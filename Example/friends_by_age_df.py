from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local").appName("SparkSql").getOrCreate()

lines = (spark.read.option("header", True)\
    .option("inferSchema", True)\
    .csv("../src/resources/fakefriends-header.csv"))

friends_by_age = lines.select("age", "friends")\
    .groupBy("age")\
    .avg("friends")\
    .sort("age")\
    .show()