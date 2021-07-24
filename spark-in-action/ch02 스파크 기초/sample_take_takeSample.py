lines = sc.textFile("/home/spark/client-ids.log")

idsStr = lines.map(lambda line: line.split(","))
idsStr.foreach(lambda x: print(x))
ids = lines.flatMap(lambda x: x.split(","))
intIds = ids.map(lambda x: int(x))

unique_ids = intIds.distinct()
finalCount = unique_ids.count()
transactionCount = ids.count()

s = unique_ids.sample(False, 0.3)
s.count()
s.collect()

swr = unique_ids.sample(True, 0.5)
swr.count()
swr.collect()

taken = unique_ids.takeSample(False, 5)
unique_ids.take(3)
