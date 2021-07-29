from pyspark.sql import SparkSession

# Load the spark session if it doesn't exist
try:
  spark

except NameError:
  spark = SparkSession\
    .builder\
    .appName("SparkRDDDemo")\
    .getOrCreate()

sc = spark.sparkContext

#-------------------------------------------------------------
# Load an RDD called nums using parallelize
nums = sc.parallelize([1, 2, 3, 4])

# Create a new RDD called squared by mapping each 
# value in nums and squaring the values
squared = nums.map(lambda x: x * x).collect()

# Let's see the results
for num in squared:
    print("%i " % (num))