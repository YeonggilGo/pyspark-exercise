from pyspark.sql import SparkSession, Row


def parse_line(line):
    return Row(
        id=int(line[0]),
        name=str(line[1].encode("utf-8")),
        age=int(line[2]),
        num_friends=int(line[3])
    )


session = SparkSession.builder.appName("FakePersonGenerator").getOrCreate()
session.sparkContext.setLogLevel("ERROR")

lines = session.read.option("header", True).csv("../src/resources/fakefriends-header.csv")
people = lines.rdd.map(parse_line)

schemePeople = session.createDataFrame(people).cache()
schemePeople.createOrReplaceTempView("people")

# option 1
teens = session.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
for teen in teens.collect():
    print(teen.name, teen.age, teen.num_friends)

# option 2
schemePeople.groupBy("age").count().orderBy("age").show()

session.stop()
