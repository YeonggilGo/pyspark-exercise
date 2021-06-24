from pyspark.sql import SparkSession

session = SparkSession.builder.getOrCreate()

# Create RDD
# rdd = session.sparkContext.parallelize([1, 2, 3])
# print(rdd.take(num=2))
# print(rdd.count())
# print(rdd.collect())

# Create DataFrame
# df = session.createDataFrame(
#     [[1, 2, 3], [4, 5, 6]], ['column1', 'column2', 'column3']
# )
# print(df.take(2))
# print(df.count())
# print(df.collect())
# print(df.show(n=2))

# # RDD and DF are Immutable.
# rdd = session.sparkContext.parallelize([1, 2, 3])
# rdd = rdd.map(lambda x: x * 100)
# print(rdd.collect())

# UDF : USer Defined Function
# import pyspark.sql.types as types
# import pyspark.sql.functions as funcs
#
#
# def mul_by_ten(num):
#     return num * 10.0
#
#
# mul_udf = funcs.udf(mul_by_ten, types.DoubleType())
# df = session.createDataFrame([[1, 2, 3], [4, 5, 6]], ['column1', 'column2', 'column3'])
# transformed_df = df.withColumn('multiplied', mul_udf('column1'))
# transformed_df.show()