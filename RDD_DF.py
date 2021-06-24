from pyspark.sql import SparkSession

session = SparkSession.builder.getOrCreate()

# Create RDD
rdd = session.sparkContext.parallelize([1, 2, 3])
print(rdd.take(num=2))
print(rdd.count())
print(rdd.collect())

# Create DataFrame
df = session.createDataFrame(
    [[1, 2, 3], [4, 5, 6]], ['column1', 'column2', 'column3']
)
print(df.take(2))
print(df.count())
print(df.collect())
print(df.show(n=2))

# RDD and DF are Immutable.
rdd = session.sparkContext.parallelize([1, 2, 3])
rdd = rdd.map(lambda x: x * 100)
print(rdd.collect())

