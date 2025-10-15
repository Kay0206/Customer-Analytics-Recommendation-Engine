from pyspark.sql.functions import col, count, desc, row_number
from pyspark.sql.window import Window

def frequent_pairs(purchases_df, top_k=5):
    items = purchases_df.filter(col("quantity") > 0).select("purchase_id", "product_id").distinct()
    p1, p2 = items.alias("p1"), items.alias("p2")
    pairs = p1.join(p2, "purchase_id")\
        .filter(col("p1.product_id") < col("p2.product_id"))\
        .groupBy(col("p1.product_id").alias("prod_a"), col("p2.product_id").alias("prod_b"))\
        .agg(count("*").alias("pair_count"))
    a_to_b = pairs.select(col("prod_a").alias("base_product"), col("prod_b").alias("recommended_product"), "pair_count")
    b_to_a = pairs.select(col("prod_b").alias("base_product"), col("prod_a").alias("recommended_product"), "pair_count")
    all_pairs = a_to_b.unionByName(b_to_a)
    window = Window.partitionBy("base_product").orderBy(desc("pair_count"))
    return all_pairs.withColumn("rank", row_number().over(window)).filter(col("rank") <= top_k)

def recommend_for_customers_from_pairs(customer_products_df, top_pairs_df, products_df):
    raw = customer_products_df.alias("cp")\
        .join(top_pairs_df.alias("tp"), col("cp.product_id") == col("tp.base_product"))\
        .select(col("cp.customer_id"), col("tp.recommended_product"), col("tp.pair_count"))
    already = customer_products_df.select("customer_id", "product_id").alias("a")
    recs = raw.join(already, (raw.customer_id == already.customer_id) & (raw.recommended_product == already.product_id), "left_anti")
    return recs.join(products_df, recs.recommended_product == products_df.product_id, "left")\
            .select("customer_id", "recommended_product", "product_name", "category", "pair_count")

def recommend_from_browsing(browsing_df, products_df, purchases_df):
    browsed = browsing_df.join(products_df, "product_id").select("customer_id", "category").dropDuplicates()
    purchased = purchases_df.join(products_df, "product_id").select("customer_id", "product_id", "category").dropDuplicates()
    popular = purchases_df.join(products_df, "product_id")\
        .groupBy("category", "product_id", "product_name")\
        .agg(count("*").alias("times_bought"))
    rec = browsed.join(popular, "category").select("customer_id", "product_id", "product_name", "category", "times_bought")
    return rec.join(purchased, (rec.customer_id == purchased.customer_id) & (rec.product_id == purchased.product_id), "left_anti")
