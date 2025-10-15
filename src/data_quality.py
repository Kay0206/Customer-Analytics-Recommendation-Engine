from pyspark.sql.functions import coalesce, lit, to_date, to_timestamp

def clean_customers(df):
    return df.dropDuplicates().withColumn("age", coalesce(df.age, lit(-1)))

def clean_products(df):
    return df.dropDuplicates().withColumn("price", coalesce(df.price, lit(0.0)))

def clean_purchases(df):
    return df.dropDuplicates().withColumn("quantity", coalesce(df.quantity, lit(0)))\
             .withColumn("purchase_date", to_date(df.purchase_date))

def clean_browsing(df):
    return df.dropDuplicates().withColumn("view_timestamp", to_timestamp(df.view_timestamp))
