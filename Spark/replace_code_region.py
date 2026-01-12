from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .appName('Phone number conversion') \
    .config('spark.ui.showConsoleProgress', True) \
    .config('spark.sql.execution.arrow.pyspark.enabled', True) \
    .config('spark.driver.memory', '2g') \
    .config('spark.driver.cores', 2) \
    .config('spark.executor.cores', 5) \
    .config('spark.executor.memory', '16g') \
    .config('spark.submit.deploymode', 'client') \
    .config('spark.dynamicAllocation.enabled', True) \
    .config('spark.dynamicAllocation.minExecutors', 5) \
    .config('spark.dynamicAllocation.maxExecutors', 8) \
    .config('spark.dynamicAllocation.executorIdleTimeout', '60s') \
    .config('spark.yarn.queue', 'fob2b') \
    .getOrCreate()

df = spark.table('table_name')

def convert_phone_number(phone):
    if phone.startswith('8'):
        return '+7' + phone[1:]
    return phone

udf_convert_phone_number = udf(convert_phone_number, StringType())
df = df.withColumn('phone', udf_convert_phone_number(col('phone')))

spark.stop()
