from pyspark import SparkContext

# Broadcast
# Broadcast variables are used to save the copy of data across all nodes.
sc = SparkContext("local", "Broadcast app")
words_new = sc.broadcast(["scala", "java", "hadoop", "spark", "akka"])
data = words_new.value
print("Stored data -> %s" % data)
elem = words_new.value[2]
print("Printing a particular element in RDD -> %s" % elem)

# Accumulator
# Accumulator variables are used for aggregating the information through associative and commutative operations
sc = SparkContext("local", "Accumulator app")
num = sc.accumulator(10)


def f(x):
    global num
    num += x


rdd = sc.parallelize([20, 30, 40, 50])
rdd.foreach(f)
final = num.value
print("Accumulated value is -> %i" % final)
