from pyspark.sql.functions import col, lag
from pyspark import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *

sc = SparkContext()
spark = SparkSession.builder.getOrCreate()

####
# 1. Setup : Write a function to load it in an RDD & DataFrame
####

# RDD API
# Columns:
# 0: word (string), 1: year (int), 2: frequency (int), 3: books (int)

db = sc.textFile("gbooks")

word_fields = [StructField(name, StringType(), True) for name in ['word']]

int_fields = [StructField(name, IntegerType(), True) for name in ['year', 'frequency', 'books']]

schema = StructType(word_fields + int_fields)

def mapper(wrd):
    return [val if i == 0 else int(val) for i, val in enumerate(wrd.split('\t'))]

df = spark.createDataFrame(db.map(mapper, schema))

df_filtered = df.filter("`_2`"  >= 1500).filter("`_2`" <= 2000)


###
# 2. Frequency Increase : analyze the frequency increase of words starting from the year 1500 to the year 2000
###
# Spark SQL - DataFrame API

window = Window.partitionBy("word").orderBy("year")
df_with_increase = df_filtered.withColumn("prev_frequency", lag("frequency").over(window)) .withColumn("frequency_increase", col("frequency") - col("prev_frequency"))
df_with_increase = df_with_increase.na.fill(value=0, subset=["frequency_increase"])
df_total_increase = df_with_increase.groupBy("word").agg(sum("frequency_increase").alias("total_increase"))
df_total_increase.orderBy(col("total_increase").desc()).show(5)
# df_word_increase.show()