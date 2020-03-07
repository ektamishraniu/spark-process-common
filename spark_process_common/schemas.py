from pyspark.sql.types import DecimalType, StructType, StructField, StringType

ESSBASE_LOAD = StructType([
    StructField('customer', StringType(), True),
    StructField('ship_to', StringType(), True),
    StructField('business_unit', StringType(), True),
    StructField('origination', StringType(), True),
    StructField('product', StringType(), True),
    StructField('modes', StringType(), True),
    StructField('channel', StringType(), True),
    StructField('geography', StringType(), True),
    StructField('end_use', StringType(), True),
    StructField('scenario', StringType(), True),
    StructField('version', StringType(), True),
    StructField('measure', StringType(), True),
    StructField('fiscal_year', StringType(), True),
    StructField('account', StringType(), True),
    StructField('period', StringType(), True),
    StructField('amount', DecimalType(precision=16, scale=2), True),
])
