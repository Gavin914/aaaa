from pyspark import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import SparkSession

sc = SparkContext()
spark = SparkSession.builder.getOrCreate()

####
# 1. Setup : Write a function to load it in an RDD & DataFrame
####

# RDD API
# Columns:
# 0: word (string), 1: year (int), 2: frequency (int), 3: books (int)


# Spark SQL - DataFrame API

db = sc.textFile("gbooks")
def mapper(wrd):
    return [val if i == 0 else int(val) for i, val in enumerate(wrd.split('\t'))]

word_fields = [StructField(name, StringType(), True) for name in ['word']]
int_fields = [StructField(name, IntegerType(), True) for name in ['year', 'frequency', 'books']]
schema = StructType(word_fields + int_fields)


####
# 2. Counting : How many lines does the file contains? Answer this question via both RDD api & #Spark SQL
####

# Spark SQL

# sqlContext.sql(query).show() or df.show()
# +--------+
# |count(1)|
# +--------+
# |   50013|
# +--------+

df = spark.createDataFrame(db.map(mapper, schema))
df.createOrReplaceTempView("gbooks")

df.select("*").where(dataFrame("word") === "00650")

