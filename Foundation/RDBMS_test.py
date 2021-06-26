from pyspark.sql import SparkSession

session = SparkSession.builder.config(
    'spark.jars', 'bin/postgresql-42.2.22.jar'
).config(
    'spark.driver.extraClassPath', 'bin/postgresql-42.2.22.jar'
).getOrCreate()
url = f"jdbc:postgresql://127.0.0.1:5432/test"
properties = {'user': 'jekyll', 'password': 'r49768'}

# Read from DB
# df = session.read.jdbc(
#     url=url, table='name', properties=properties
# )

# Write to DB
df = session.createDataFrame(
    [[1, 2, 3], [4, 5, 6]], ['column1', 'column2', 'column3']
)
df.write.jdbc(
    url=url, table='new_table', mode='append', properties=properties
)
