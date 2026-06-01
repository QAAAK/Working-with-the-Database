
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .appName("app_name") \
    .getOrCreate()


def hash_function(password):
    hash_value = 0
    for char, index in zip(password, range(len(password))):
        hash_value += ord(char) * index
    return hash_value % 10**9


udf_func = udf(hash_function, StringType())

spark.table('table_name').filter(col('column_name').between(10,20)).withColumn('new_column', hash_function(col('column_name')))
