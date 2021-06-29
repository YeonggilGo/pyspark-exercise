from pyspark.sql import SparkSession

session = SparkSession.builder.appName("SparkSqlDF").getOrCreate()
session.sparkContext.setLogLevel("ERROR")

people = (
    session.read.option("header", True)
    .option("inferSchema", True)
    .csv("../src/resources/fakefriends-header.csv")
)

print("Inferred scheme: ")
people.printSchema()

print("Name column: ")
people.select(people.name).show()

print("Filter age < 21: ")
people.filter("age < 21").show()

print("Group by age: ")
people.groupBy("age").count().show()

print("Make everyone + 10y older: ")
people.select(people.name, people.age + 10).show()



