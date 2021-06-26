from pyspark.sql import SparkSession

session = SparkSession.builder.getOrCreate()
df = session.createDataFrame(
    [[1, 2, 3], [4, 5, 6]], ['column1', 'column2', 'column3']
)

# df.select('column1', 'column2').show()
# df.where('column1 = 1').show()
# df.join(df, ['column1'], how='inner').show()
df.createOrReplaceTempView("table1")
session.sql("select column1 as f1, column2 as f2 from table1").show()