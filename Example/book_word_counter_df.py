from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.sql import functions as F

session = SparkSession.builder.appName("BookWordCounter").getOrCreate()
session.sparkContext.setLogLevel("ERROR")

schema = StructType(
    [StructField("word", StringType(), True)]
)
book = session.read.text("../src/resources/Self-Employment_book.txt")
words = book.select(F.explode(F.split(book.value, "\\W+")).alias("word"))
word_counts = words.groupBy("word").count().filter("count > 10").sort("count", ascending=False)
word_counts.show()

session.stop()
