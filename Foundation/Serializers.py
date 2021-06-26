from pyspark.context import SparkContext
from pyspark.serializers import MarshalSerializer, PickleSerializer

sc = SparkContext("local", "serialization app", serializer=MarshalSerializer())
print(sc.parallelize(list(range(1000))).map(lambda x: x * 2).take(10))
sc.stop()

# Marshal is faster than Pickle.
# Pickle supports nearly any Python object.
