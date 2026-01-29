
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .appName("app_name") \
    .getOrCreate()


def find_range(value):
    step = 50 
    # Проверка на None (Null)
    if value is None:
        return "Нет данных"
    value = int(round(value, 0))
    # Для значений от 0 до 50 включительно
    if value <= 50:
        return "0-50"
    # Для значений больше 50
    lower_bound = ((value - 1) // step) * step + 1
    upper_bound = lower_bound + step - 1
    
    return f"{lower_bound}-{upper_bound}" 


udf_func = udf(find_range, StringType())

spark.table('table_name').filter(col('column_name').between(10,20)).withColumn('new_column', fing_range(col('column_name')))
