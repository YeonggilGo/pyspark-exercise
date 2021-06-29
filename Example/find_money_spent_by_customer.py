# find total money spent by customer
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import format_number


def parse_lines(line):
    return Row(
        customer_id=int(line[0]),
        item_id=int(line[1]),
        item_price=float(line[2])
    )


session = SparkSession.builder.getOrCreate()
session.sparkContext.setLogLevel("ERROR")

lines = session.read.option("header", True).csv("../src/resources/customer_orders.csv")
rdds = lines.rdd.map(parse_lines)

infos = rdds.map(lambda x: (x.customer_id, x.item_price))
spent_moneys = infos.reduceByKey(lambda x, y: x + y)

spent_moneys.sortByKey().toDF(["id", "temp_money"])\
    .withColumn("spent_money", format_number("temp_money", 2))\
    .select("id", "spent_money")\
    .show()