import math

from pyspark.sql import types
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


# RDD Mapping


def take_log_in_all_columns(row: types.Row):
    old_row = row.asDict()
    new_row = {f'log({column_name})': math.log(value)
               for column_name, value in old_row.items()}
    return types.Row(**new_row)


logarithmic_df = df.rdd.map(take_log_in_all_columns).toDF()
logarithmic_df.show()
