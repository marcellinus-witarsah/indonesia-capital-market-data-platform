from pyspark.sql.functions import col, trim

def string_cleanser(col: col):
    return trim(col).when(col == "", None).otherwise(col)