from pyspark.sql.functions import col, when, lit, avg, sum as _sum, count, countDistinct, to_date, current_date, date_sub, row_number, desc
from pyspark.sql.window import Window

def customer_segmentation(customers_df, purchases_df, products_df):
    cust_seg = customers_df.withColumn(
        "age_bracket",
        when((col("age") >= 18) & (col("age") <= 25), "18-25")
        .when((col("age") >= 26) & (col("age") <= 35), "26-35")
        .when((col("age") >= 36) & (col("age") <= 50), "36-50")
        .otherwise("unclassified")
    )
    joined = purchases_df.filter(col("quantity") > 0)\
        .join(products_df, "product_id")\
        .withColumn("purchase_value", col("price") * col("quantity"))\
        .join(cust_seg, "customer_id")

    return joined.groupBy("city", "age_bracket").agg(
        avg("purchase_value").alias("avg_purchase_value"),
        _sum("quantity").alias("total_quantity"),
        countDistinct("customer_id").alias("num_customers")
    )

def product_popularity(purchases_df, products_df, top_n=10):
    df = purchases_df.join(products_df, "product_id").filter(col("quantity") > 0)
    top10 = df.groupBy("product_id", "product_name").agg(_sum("quantity").alias("total_sold"))\
              .orderBy(desc("total_sold")).limit(top_n)
    cat_window = Window.partitionBy("category").orderBy(desc("total_sold"))
    per_cat = df.groupBy("category", "product_id", "product_name")\
                .agg(_sum("quantity").alias("total_sold"))\
                .withColumn("rank", row_number().over(cat_window))\
                .filter(col("rank") <= top_n)
    return top10, per_cat

def trending_products(browsing_df, products_df, days=7):
    end_date = current_date()
    recent_start = date_sub(end_date, days - 1)
    previous_start = date_sub(end_date, 2 * days - 1)
    previous_end = date_sub(end_date, days)

    recent = browsing_df.withColumn("view_date", to_date(col("view_timestamp")))\
                .filter((col("view_date") >= recent_start) & (col("view_date") <= end_date))\
                .groupBy("product_id")\
                .agg(count("*").alias("views_recent"))
    past = browsing_df.withColumn("view_date", to_date(col("view_timestamp")))\
                .filter((col("view_date") >= previous_start) & (col("view_date") <= previous_end))\
                .groupBy("product_id")\
                .agg(count("*").alias("views_past"))

    trend = recent.join(past, "product_id", "full_outer")\
                .join(products_df, "product_id", "left")\
                .fillna(0, ["views_recent", "views_past"])\
                .withColumn("view_difference", col("views_recent") - col("views_past"))\
                .withColumn("growth_rate", when(col("views_past") != 0, 
                                                            (col("views_recent") - col("views_past")) / col("views_past"))\
                                                .otherwise(None)
                                        )
                .orderBy(desc("view_difference"))
    return trend

def customer_lifetime_value(purchases_df, products_df, customers_df):
    joined = purchases_df.filter(col("quantity") > 0)\
        .join(products_df, "product_id")\
        .join(customers_df, "customer_id")\
        .withColumn("total_sale", col("price") * col("quantity"))
    return joined.groupBy("customer_id", "name").agg(_sum("total_sale").alias("revenue_generated"))
