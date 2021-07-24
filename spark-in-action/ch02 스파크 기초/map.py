from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
numbers = sc.parallelize(range(10, 51, 10))
numbers.foreach(lambda x: print(x))
numbersSquared = numbers.map(lambda num: num * num)
numbersSquared.foreach(lambda x: print(x))

reversed = numbersSquared.map(lambda x: str(x)[::-1])
reversed.foreach(lambda x: print(x))
