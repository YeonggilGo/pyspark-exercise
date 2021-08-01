# select & drop
postsDf = itPostsDFStruct
postsIdBody = postsDf.select("id", "body")

postsIdBody = postsDf.select(postsDf["id"], postsDf["body"])

postIds = postsIdBody.drop("body")

# filter & where

from pyspark.sql.functions import *
postsIdBody.filter(instr(postsIdBody["body"], "Italiano") > 0).count()

noAnswer = postsDf.filter((postsDf["postTypeId"] == 1) & isnull(postsDf["acceptedAnswerId"]))

firstTenQs = postsDf.filter(postsDf["postTypeId"] == 1).limit(10)
firstTenQsRn = firstTenQs.withColumnRenamed("ownerUserId", "owner")

postsDf.filter(postsDf.postTypeId == 1).withColumn("ratio", postsDf.viewCount / postsDf.score).where("ratio < 35").show()
#+------------+--------------------+-----------+--------------------+-----+--------------------+---------+-----+--------------------+-----------+----------------+----------+----+-------------------+
#|commentCount|    lastActivityDate|ownerUserId|                body|score|        creationDate|viewCount|title|                tags|answerCount|acceptedAnswerId|postTypeId|  id|              ratio|
#+------------+--------------------+-----------+--------------------+-----+--------------------+---------+-----+--------------------+-----------+----------------+----------+----+-------------------+
#|           5|2013-11-21 14:04:...|          8|&lt;p&gt;The use ...|   13|2013-11-11 21:01:...|      142| null|&lt;prepositions&...|          2|            1212|         1|1192| 10.923076923076923|
#|           0|2013-11-12 09:26:...|         17|&lt;p&gt;When wri...|    5|2013-11-11 21:01:...|       70| null|&lt;punctuation&g...|          4|            1195|         1|1193|               14.0|
#|           1|2013-11-12 12:53:...|         99|&lt;p&gt;I can't ...|   -3|2013-11-12 10:57:...|       68| null|&lt;grammar&gt;&l...|          3|            1216|         1|1203|-22.666666666666668|
#|           3|2014-09-11 14:37:...|         63|&lt;p&gt;The plur...|    5|2013-11-12 13:34:...|       59| null|&lt;plural&gt;&lt...|          1|            1227|         1|1221|               11.8|
#|           1|2013-11-12 13:49:...|         63|&lt;p&gt;I rememb...|    6|2013-11-12 13:38:...|       53| null|&lt;usage&gt;&lt;...|          1|            1223|         1|1222|  8.833333333333334|
#|           5|2013-11-13 00:32:...|        159|&lt;p&gt;Girando ...|    6|2013-11-12 23:50:...|       88| null|&lt;grammar&gt;&l...|          1|            1247|         1|1246| 14.666666666666666|
#|           0|2013-11-14 00:54:...|        159|&lt;p&gt;Mi A¨ ca...|    7|2013-11-14 00:19:...|       70| null|       &lt;verbs&gt;|          1|            null|         1|1258|               10.0|
#|           1|2013-11-15 12:17:...|         18|&lt;p&gt;Clearly ...|    7|2013-11-14 01:21:...|       68| null|&lt;grammar&gt;&l...|          2|            null|         1|1262|  9.714285714285714|
#|           0|2013-11-14 21:14:...|         79|&lt;p&gt;Alle ele...|    8|2013-11-14 20:16:...|       96| null|&lt;grammar&gt;&l...|          1|            1271|         1|1270|               12.0|
#|           0|2013-11-15 17:12:...|         63|&lt;p&gt;In Itali...|    8|2013-11-15 14:54:...|       68| null|&lt;usage&gt;&lt;...|          1|            1277|         1|1275|                8.5|
#|           3|2013-11-19 18:08:...|          8|&lt;p&gt;The Ital...|    6|2013-11-15 16:09:...|       87| null|&lt;grammar&gt;&l...|          1|            null|         1|1276|               14.5|
#|           1|2014-08-14 13:13:...|         12|&lt;p&gt;When I s...|    5|2013-11-16 09:36:...|       74| null|&lt;regional&gt;&...|          3|            null|         1|1279|               14.8|
#|          10|2014-03-15 08:25:...|        176|&lt;p&gt;In Engli...|   12|2013-11-16 11:13:...|      148| null|&lt;punctuation&g...|          2|            1286|         1|1285| 12.333333333333334|
#|           2|2013-11-17 15:54:...|         79|&lt;p&gt;Al di fu...|    7|2013-11-16 13:16:...|       70| null|     &lt;accents&gt;|          2|            null|         1|1287|               10.0|
#|           1|2013-11-16 19:05:...|        176|&lt;p&gt;Often ti...|   12|2013-11-16 14:16:...|      106| null|&lt;verbs&gt;&lt;...|          1|            null|         1|1290|  8.833333333333334|
#|           4|2013-11-17 15:50:...|         22|&lt;p&gt;The verb...|    6|2013-11-17 14:30:...|       66| null|&lt;verbs&gt;&lt;...|          1|            null|         1|1298|               11.0|
#|           0|2014-09-12 10:55:...|          8|&lt;p&gt;Wikipedi...|   10|2013-11-20 16:42:...|      145| null|&lt;orthography&g...|          5|            1336|         1|1321|               14.5|
#|           2|2013-11-21 12:09:...|         22|&lt;p&gt;La parol...|    5|2013-11-20 20:48:...|       49| null|&lt;usage&gt;&lt;...|          1|            1338|         1|1324|                9.8|
#|           0|2013-11-22 13:34:...|        114|&lt;p&gt;There ar...|    7|2013-11-20 20:53:...|       69| null|   &lt;homograph&gt;|          2|            1330|         1|1325|  9.857142857142858|
#|           6|2013-11-26 19:12:...|         12|&lt;p&gt;Sento ch...|   -3|2013-11-21 21:12:...|       79| null|  &lt;word-usage&gt;|          2|            null|         1|1347|-26.333333333333332|
#+------------+--------------------+-----------+--------------------+-----+--------------------+---------+-----+--------------------+-----------+----------------+----------+----+-------------------+
# only showing top 20 rows

#The 10 most recently modified questions:
postsDf.filter(postsDf.postTypeId == 1).orderBy(postsDf.lastActivityDate.desc()).limit(10).show()

# withColumn & withColumnRenamed

from pyspark.sql.functions import *
postsDf.filter(postsDf.postTypeId == 1).withColumn("activePeriod", datediff(postsDf.lastActivityDate, postsDf.creationDate)).orderBy(desc("activePeriod")).head().body.replace("&lt;","<").replace("&gt;",">")
#<p>The plural of <em>braccio</em> is <em>braccia</em>, and the plural of <em>avambraccio</em> is <em>avambracci</em>.</p><p>Why are the plural of those words so different, if they both are referring to parts of the human body, and <em>avambraccio</em> derives from <em>braccio</em>?</p>

postsDf.select(avg(postsDf.score), max(postsDf.score), count(postsDf.score)).show()

# Window function

from pyspark.sql.window import Window
winDf = postsDf.filter(postsDf.postTypeId == 1).select(postsDf.ownerUserId, postsDf.acceptedAnswerId, postsDf.score, max(postsDf.score).over(Window.partitionBy(postsDf.ownerUserId)).alias("maxPerUser"))
winDf.withColumn("toMax", winDf.maxPerUser - winDf.score).show(10)
# +-----------+----------------+-----+----------+-----+
# |ownerUserId|acceptedAnswerId|score|maxPerUser|toMax|
# +-----------+----------------+-----+----------+-----+
# |        232|            2185|    6|         6|    0|
# |        833|            2277|    4|         4|    0|
# |        833|            null|    1|         4|    3|
# |        235|            2004|   10|        10|    0|
# |        835|            2280|    3|         3|    0|
# |         37|            null|    4|        13|    9|
# |         37|            null|   13|        13|    0|
# |         37|            2313|    8|        13|    5|
# |         37|              20|   13|        13|    0|
# |         37|            null|    4|        13|    9|
# +-----------+----------------+-----+----------+-----+

postsDf.filter(postsDf.postTypeId == 1).select(postsDf.ownerUserId, postsDf.id, postsDf.creationDate, lag(postsDf.id, 1).over(Window.partitionBy(postsDf.ownerUserId).orderBy(postsDf.creationDate)).alias("prev"), lead(postsDf.id, 1).over(Window.partitionBy(postsDf.ownerUserId).orderBy(postsDf.creationDate)).alias("next")).orderBy(postsDf.ownerUserId, postsDf.id).show()
# +-----------+----+--------------------+----+----+
# |ownerUserId|  id|        creationDate|prev|next|
# +-----------+----+--------------------+----+----+
# |          4|1637|2014-01-24 06:51:...|null|null|
# |          8|   1|2013-11-05 20:22:...|null| 112|
# |          8| 112|2013-11-08 13:14:...|   1|1192|
# |          8|1192|2013-11-11 21:01:...| 112|1276|
# |          8|1276|2013-11-15 16:09:...|1192|1321|
# |          8|1321|2013-11-20 16:42:...|1276|1365|
# |          8|1365|2013-11-23 09:09:...|1321|null|
# |         12|  11|2013-11-05 21:30:...|null|  17|
# |         12|  17|2013-11-05 22:17:...|  11|  18|
# |         12|  18|2013-11-05 22:34:...|  17|  19|
# |         12|  19|2013-11-05 22:38:...|  18|  63|
# |         12|  63|2013-11-06 17:54:...|  19|  65|
# |         12|  65|2013-11-06 18:07:...|  63|  69|
# |         12|  69|2013-11-06 19:41:...|  65|  70|
# |         12|  70|2013-11-06 20:35:...|  69|  89|
# |         12|  89|2013-11-07 19:22:...|  70|  94|
# |         12|  94|2013-11-07 20:42:...|  89| 107|
# |         12| 107|2013-11-08 08:27:...|  94| 122|
# |         12| 122|2013-11-08 20:55:...| 107|1141|
# |         12|1141|2013-11-09 20:50:...| 122|1142|
# +-----------+----+--------------------+----+----+


countTags = udf(lambda (tags): tags.count("&lt;"), IntegerType())
postsDf.filter(postsDf.postTypeId == 1).select("tags", countTags(postsDf.tags).alias("tagCnt")).show(10, False)
# +-------------------------------------------------------------------+------+
# |tags                                                               |tagCnt|
# +-------------------------------------------------------------------+------+
# |&lt;word-choice&gt;                                                |1     |
# |&lt;english-comparison&gt;&lt;translation&gt;&lt;phrase-request&gt;|3     |
# |&lt;usage&gt;&lt;verbs&gt;                                         |2     |
# |&lt;usage&gt;&lt;tenses&gt;&lt;english-comparison&gt;              |3     |
# |&lt;usage&gt;&lt;punctuation&gt;                                   |2     |
# |&lt;usage&gt;&lt;tenses&gt;                                        |2     |
# |&lt;history&gt;&lt;english-comparison&gt;                          |2     |
# |&lt;idioms&gt;&lt;etymology&gt;                                    |2     |
# |&lt;idioms&gt;&lt;regional&gt;                                     |2     |
# |&lt;grammar&gt;                                                    |1     |
# +-------------------------------------------------------------------+------+


# groupBy, orderBy

postsDf.groupBy(postsDf.ownerUserId, postsDf.tags, postsDf.postTypeId).count().orderBy(postsDf.ownerUserId.desc()).show(10)
#+-----------+--------------------+----------+-----+
#|ownerUserId|                tags|postTypeId|count|
#+-----------+--------------------+----------+-----+
#|        862|                    |         2|    1|
#|        855|         <resources>|         1|    1|
#|        846|<translation><eng...|         1|    1|
#|        845|<word-meaning><tr...|         1|    1|
#|        842|  <verbs><resources>|         1|    1|
#|        835|    <grammar><verbs>|         1|    1|
#|        833|                    |         2|    1|
#|        833|           <meaning>|         1|    1|
#|        833|<meaning><article...|         1|    1|
#|        814|                    |         2|    1|
#+-----------+--------------------+----------+-----+

postsDf.groupBy(postsDf.ownerUserId).agg(max(postsDf.lastActivityDate), max(postsDf.score)).show(10)
postsDf.groupBy(postsDf.ownerUserId).agg({"lastActivityDate": "max", "score": "max"}).show(10)
# +-----------+---------------------+----------+
# |ownerUserId|max(lastActivityDate)|max(score)|
# +-----------+---------------------+----------+
# |        431| 2014-02-16 14:16:...|         1|
# |        232| 2014-08-18 20:25:...|         6|
# |        833| 2014-09-03 19:53:...|         4|
# |        633| 2014-05-15 22:22:...|         1|
# |        634| 2014-05-27 09:22:...|         6|
# |        234| 2014-07-12 17:56:...|         5|
# |        235| 2014-08-28 19:30:...|        10|
# |        435| 2014-02-18 13:10:...|        -2|
# |        835| 2014-08-26 15:35:...|         3|
# |         37| 2014-09-13 13:29:...|        23|
# +-----------+---------------------+----------+
postsDf.groupBy(postsDf.ownerUserId).agg(max(postsDf.lastActivityDate), max(postsDf.score) > 5).show(10)
# +-----------+---------------------+----------------+
# |ownerUserId|max(lastActivityDate)|(max(score) > 5)|
# +-----------+---------------------+----------------+
# |        431| 2014-02-16 14:16:...|           false|
# |        232| 2014-08-18 20:25:...|            true|
# |        833| 2014-09-03 19:53:...|           false|
# |        633| 2014-05-15 22:22:...|           false|
# |        634| 2014-05-27 09:22:...|            true|
# |        234| 2014-07-12 17:56:...|           false|
# |        235| 2014-08-28 19:30:...|            true|
# |        435| 2014-02-18 13:10:...|           false|
# |        835| 2014-08-26 15:35:...|           false|
# |         37| 2014-09-13 13:29:...|            true|
# +-----------+---------------------+----------------+

smplDf = postsDf.where((postsDf.ownerUserId >= 13) & (postsDf.ownerUserId <= 15))
smplDf.groupBy(smplDf.ownerUserId, smplDf.tags, smplDf.postTypeId).count().show()
# +-----------+----+----------+-----+
# |ownerUserId|tags|postTypeId|count|
# +-----------+----+----------+-----+
# |         15|    |         2|    2|
# |         14|    |         2|    2|
# |         13|    |         2|    1|
# +-----------+----+----------+-----+
smplDf.rollup(smplDf.ownerUserId, smplDf.tags, smplDf.postTypeId).count().show()
# +-----------+----+----------+-----+
# |ownerUserId|tags|postTypeId|count|
# +-----------+----+----------+-----+
# |         15|    |         2|    2|
# |         13|    |      null|    1|
# |         13|null|      null|    1|
# |         14|    |      null|    2|
# |         13|    |         2|    1|
# |         14|null|      null|    2|
# |         15|    |      null|    2|
# |         14|    |         2|    2|
# |         15|null|      null|    2|
# |       null|null|      null|    5|
# +-----------+----+----------+-----+
smplDf.cube(smplDf.ownerUserId, smplDf.tags, smplDf.postTypeId).count().show()
# +-----------+----+----------+-----+
# |ownerUserId|tags|postTypeId|count|
# +-----------+----+----------+-----+
# |         15|    |         2|    2|
# |       null|    |         2|    5|
# |         13|    |      null|    1|
# |         15|null|         2|    2|
# |       null|null|         2|    5|
# |         13|null|      null|    1|
# |         14|    |      null|    2|
# |         13|    |         2|    1|
# |         14|null|      null|    2|
# |         15|    |      null|    2|
# |         13|null|         2|    1|
# |       null|    |      null|    5|
# |         14|    |         2|    2|
# |         15|null|      null|    2|
# |       null|null|      null|    5|
# |         14|null|         2|    2|
# +-----------+----+----------+-----+

# Join

itVotesRaw = sc.textFile("first-edition/ch05/italianVotes.csv").map(lambda x: x.split("~"))
itVotesRows = itVotesRaw.map(lambda row: Row(id=long(row[0]), postId=long(row[1]), voteTypeId=int(row[2]), creationDate=datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S.%f")))
votesSchema = StructType([
  StructField("creationDate", TimestampType(), False),
  StructField("id", LongType(), False),
  StructField("postId", LongType(), False),
  StructField("voteTypeId", IntegerType(), False)
  ])

votesDf = sqlContext.createDataFrame(itVotesRows, votesSchema)

postsVotes = postsDf.join(votesDf, postsDf.id == votesDf.postId)
postsVotesOuter = postsDf.join(votesDf, postsDf.id == votesDf.postId, "outer")