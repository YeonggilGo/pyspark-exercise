
# *************** terminal ******************
# echo "15,16,20,20
# 77,80,94
# 94,98,16,31
# 31,15,20" > ~/client-ids.log
# ************* end terminal *****************

lines = sc.textFile("/home/spark/client-ids.log")

idsStr = lines.map(lambda line: line.split(","))
idsStr.foreach(lambda x: print(x))
ids = lines.flatMap(lambda x: x.split(","))
intIds = ids.map(lambda x: int(x))

uniqueIds = intIds.distinct()
finalCount = uniqueIds.count()
transactionCount = ids.count()
