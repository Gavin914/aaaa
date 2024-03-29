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

df = spark.createDataFrame(db.map(mapper, schema))
df.createOrReplaceTempView("gbooks")
####
# 4. MapReduce : List the top three words that have appeared in the greatest number of years.
####

# Spark SQL

# +-------------+--------+
# |         word|count(1)|
# +-------------+--------+
# |    ATTRIBUTE|      11|
# |approximation|       4|
# |    agast_ADV|       4|
# +-------------+--------+
# only showing top 3 rows

# The above output may look slightly different for you due to ties with other words

spark.sql("SELECT `_1` AS word, COUNT(*) FROM gbooks GROUP BY `_1` ORDER BY COUNT(*) DESC").show(3)