from pyspark import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import SparkSession

sc = SparkContext()

spark = SparkSession.builder.getOrCreate()

spark.sql("SET spark.sql.autoBroadcastJoinThreshold = -1")
####
# 1. Setup : Write a function to load it in an RDD & DataFrame
####

# RDD API
# Columns:
# 0: word (string), 1: year (int), 2: frequency (int), 3: books (int)


# Spark SQL - DataFrame API


db = sc.textFile("gbooks")

word_fields = [StructField(name, StringType(), True) for name in ['word']]

int_fields = [StructField(name, IntegerType(), True) for name in ['year', 'frequency', 'books']]

schema = StructType(word_fields + int_fields)

def mapper(wrd):
    return [val if i == 0 else int(val) for i, val in enumerate(wrd.split('\t'))]

df = spark.createDataFrame(db.map(mapper, schema))

####
# 5. Joining : The following program construct a new dataframe out of 'df' with a much smaller size.
####

df2 = df.select("word", "year").distinct().limit(1000)
df2.createOrReplaceTempView('gbooks2')

# Now we are going to perform a JOIN operation on 'df2'. Do a self-join on 'df2' in lines with the same #'count1' values and see how many lines this JOIN could produce. Answer this question via Spark SQL API

# Spark SQL API

# output: 166

res = spark.sql("SELECT A.year FROM gbooks2 A, gbooks2 B WHERE A.year = B.year")
print(res.count())