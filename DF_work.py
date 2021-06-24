from pyspark.sql import SparkSession
from pyspark.sql import functions as funcs
session = SparkSession.builder.getOrCreate()
csv_df = session.read.csv(
    'data/adult.data.csv', header=False, inferSchema=True
)

ADULT_COLUMN_NAMES = [
     "age",
     "workclass",
     "fnlwgt",
     "education",
     "education_num",
     "marital_status",
     "occupation",
     "relationship",
     "race",
     "sex",
     "capital_gain",
     "capital_loss",
     "hours_per_week",
     "native_country",
     "income"
]

for new_col, old_col in zip(ADULT_COLUMN_NAMES, csv_df.columns):
    csv_df = csv_df.withColumnRenamed(old_col, new_col)

# csv_df.describe().show()

work_hours_df = csv_df.groupBy(
    'age'
).agg(
    funcs.avg('hours_per_week'),
    funcs.stddev_samp('hours_per_week')
).sort('age')
work_hours_df.show()