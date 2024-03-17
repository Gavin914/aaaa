from pyspark import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import SparkSession


sc = SparkContext()
spark = SparkSession.builder.getOrCreate()

gbooks = sc.textFile("gbooks")

word_fields = [StructField(name, StringType(), True) for name in ['word']]
int_fields = [StructField(name, IntegerType(), True) for name in ['year', 'frequency', 'books']]
schema = StructType(word_fields + int_fields)

df = spark.createDataFrame(gbooks.map(lambda wrd: [val if i == 0 else int(val) for i, val in enumerate(wrd.split('\t'))]), schema)
df.printSchema()


