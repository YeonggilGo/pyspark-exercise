# UDF : USer Defined Function
import pyspark.sql.types as types
import pyspark.sql.functions as funcs
from pyspark.sql import SparkSession

session = SparkSession.builder.getOrCreate()


def mul_by_ten(num):
    return num * 10.0


mul_udf = funcs.udf(mul_by_ten, types.DoubleType())
df = session.createDataFrame([[1, 2, 3], [4, 5, 6]], ['column1', 'column2', 'column3'])
transformed_df = df.withColumn('multiplied', mul_udf('column1'))
transformed_df.show()
