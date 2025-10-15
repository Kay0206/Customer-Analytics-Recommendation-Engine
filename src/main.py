from spark_utils import get_spark
from data_quality import clean_customers, clean_products, clean_purchases, clean_browsing
from analytics import customer_segmentation, product_popularity, trending_products, customer_lifetime_value
from recommender import frequent_pairs, recommend_for_customers_from_pairs, recommend_from_browsing
from export import write_parquet
import yaml, os
from pyspark.sql.types import *

def load_config(path='config.yaml'):
    with open(path,'r') as f: return yaml.safe_load(f)

def main():
    cfg = load_config()
    spark = get_spark()

    # Schemas
    customer_schema = StructType([
        StructField('customer_id',IntegerType()),
        StructField('name',StringType()),
        StructField('age',IntegerType()),
        StructField('city',StringType()),
        StructField('signup_date',DateType())
    ])
    product_schema = StructType([
        StructField('product_id',IntegerType()),
        StructField('product_name',StringType()),
        StructField('category',StringType()),
        StructField('price',FloatType())
    ])
    purchase_schema = StructType([
        StructField('purchase_id',IntegerType()),
        StructField('customer_id',IntegerType()),
        StructField('product_id',IntegerType()),
        StructField('purchase_date',DateType()),
        StructField('quantity',IntegerType())
    ])
    browsing_schema = StructType([
        StructField('session_id',LongType()),
        StructField('customer_id',IntegerType()),
        StructField('product_id',IntegerType()),
        StructField('view_timestamp',TimestampType())
    ])

    customers = spark.read.csv(cfg['data']['customers_csv'], header=True, schema=customer_schema)
    products = spark.read.csv(cfg['data']['products_csv'], header=True, schema=product_schema)
    purchases = spark.read.csv(cfg['data']['purchases_csv'], header=True, schema=purchase_schema)
    browsing = spark.read.csv(cfg['data']['browsing_csv'], header=True, schema=browsing_schema)

    customers = clean_customers(customers); products = clean_products(products)
    purchases = clean_purchases(purchases); browsing = clean_browsing(browsing)

    value_per_segment = customer_segmentation(customers,purchases,products)
    top10, top10_per_cat = product_popularity(purchases,products)
    trending = trending_products(browsing,products)
    clv = customer_lifetime_value(purchases,products,customers)

    pairs = frequent_pairs(purchases)
    cust_prods = purchases.select('customer_id','product_id').distinct()
    rec1 = recommend_for_customers_from_pairs(cust_prods,pairs,products)
    rec2 = recommend_from_browsing(browsing,products,purchases)

    write_parquet(value_per_segment,cfg['output']['purchases_per_segment'])
    write_parquet(top10,cfg['output']['top10_products'])
    write_parquet(top10_per_cat,cfg['output']['top10_per_category'])
    write_parquet(trending,cfg['output']['trending_products'])
    write_parquet(clv,cfg['output']['revenue_per_customer'])
    write_parquet(rec1,cfg['output']['rec_freq_together'])
    write_parquet(rec2,cfg['output']['rec_browsing'])

if __name__ == '__main__': main()
