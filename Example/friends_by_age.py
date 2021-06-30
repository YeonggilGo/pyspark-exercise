from pyspark.sql import SparkSession, Row


def parse_line(line):
    fields = line.split(",")
    return Row(
        age=int(fields[2]),
        num_friends=int(fields[3])
    )


session = SparkSession.builder.getOrCreate()
session.sparkContext.setLogLevel("ERROR")

lines = session.sparkContext.textFile("../src/output/fake_person.txt")
rdds = lines.map(parse_line)
totalAges_num = rdds.mapValues(lambda x: (x, 1)).reduceByKey(
    lambda x, y: (x[0] + y[0], x[1] + y[1])
)
aveByAge = totalAges_num.mapValues(lambda x: x[0] / x[1])
results = aveByAge.collect()
for result in sorted(results, key=lambda x: x[0]):
    print(result)
