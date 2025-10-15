from pyspark.sql import SparkSession

def get_spark(app_name="CustomerAnalytics", master=None):
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    builder = builder.config("spark.sql.shuffle.partitions", "200")
    return builder.getOrCreate()
