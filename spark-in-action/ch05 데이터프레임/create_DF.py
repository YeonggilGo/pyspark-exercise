# RDD to DF
itPostsRows = sc.textFile("first-edition/ch05/italianPosts.csv")
itPostsSplit = itPostsRows.map(lambda x: x.split("~"))

itPostsRDD = itPostsSplit.map(lambda x: (x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10],x[11],x[12]))
itPostsDF = itPostsRDD.toDF(["commentCount", "lastActivityDate", "ownerUserId", "body", "score", "creationDate", "viewCount", "title", "tags", "answerCount", "acceptedAnswerId", "postTypeId", "id"])

itPostsDF.printSchema()




postSchema = StructType([
  StructField("commentCount", IntegerType(), True),
  StructField("lastActivityDate", TimestampType(), True),
  StructField("ownerUserId", LongType(), True),
  StructField("body", StringType(), True),
  StructField("score", IntegerType(), True),
  StructField("creationDate", TimestampType(), True),
  StructField("viewCount", IntegerType(), True),
  StructField("title", StringType(), True),
  StructField("tags", StringType(), True),
  StructField("answerCount", IntegerType(), True),
  StructField("acceptedAnswerId", LongType(), True),
  StructField("postTypeId", LongType(), True),
  StructField("id", LongType(), False)
  ])

rowRDD = itPostsRows.map(lambda x: stringToPost(x))
itPostsDFStruct = sqlContext.createDataFrame(rowRDD, postSchema)
itPostsDFStruct.printSchema()
itPostsDFStruct.columns
itPostsDFStruct.dtypes
