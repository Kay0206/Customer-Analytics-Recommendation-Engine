import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col

# Import your project modules
from src.spark_utils import get_spark
from src.data_quality import clean_customers, clean_products, clean_purchases, clean_browsing
from src.analytics import customer_segmentation, product_popularity, customer_lifetime_value
from src.recommender import frequent_pairs, recommend_for_customers_from_pairs

@pytest.fixture(scope="session")
def spark():
    """Create a single Spark session for all tests."""
    spark = get_spark(app_name="UnitTest", master="local[*]")
    yield spark
    spark.stop()


def test_clean_customers(spark):
    data = [(1, "Alice", None, "Mumbai", "2024-01-01"),
            (1, "Alice", None, "Mumbai", "2024-01-01")]
    df = spark.createDataFrame(data, ["customer_id", "name", "age", "city", "signup_date"])
    cleaned = clean_customers(df)
    result = cleaned.collect()
    assert len(result) == 1  # duplicates removed
    assert result[0]["age"] == -1  # missing age replaced


def test_product_popularity(spark):
    products = spark.createDataFrame([
        (1, "A", "cat1", 100.0),
        (2, "B", "cat1", 200.0)
    ], ["product_id", "product_name", "category", "price"])

    purchases = spark.createDataFrame([
        (101, 1, 1, "2024-10-10", 2),
        (102, 2, 2, "2024-10-10", 5),
        (103, 3, 2, "2024-10-10", 3)
    ], ["purchase_id", "customer_id", "product_id", "purchase_date", "quantity"])

    top10, per_cat = product_popularity(purchases, products)
    top_products = [r["product_id"] for r in top10.collect()]
    assert top_products == [2, 1]
    assert per_cat.filter(col("category") == "cat1").count() == 2


def test_customer_segmentation(spark):
    customers = spark.createDataFrame([
        (1, "A", 24, "Mumbai", "2024-01-01"),
        (2, "B", 40, "Delhi", "2024-01-01")
    ], ["customer_id", "name", "age", "city", "signup_date"])

    products = spark.createDataFrame([
        (10, "X", "cat1", 100.0),
        (20, "Y", "cat1", 200.0)
    ], ["product_id", "product_name", "category", "price"])

    purchases = spark.createDataFrame([
        (101, 1, 10, "2024-10-10", 2),
        (102, 2, 20, "2024-10-11", 1)
    ], ["purchase_id", "customer_id", "product_id", "purchase_date", "quantity"])

    seg = customer_segmentation(customers, purchases, products)
    assert set(seg.columns) == {"city", "age_bracket", "avg_purchase_value", "total_quantity", "num_customers"}
    assert seg.count() == 2


def test_customer_lifetime_value(spark):
    customers = spark.createDataFrame([
        (1, "Alice", 30, "Mumbai", "2024-01-01")
    ], ["customer_id", "name", "age", "city", "signup_date"])
    products = spark.createDataFrame([
        (101, "Phone", "electronics", 500.0)
    ], ["product_id", "product_name", "category", "price"])
    purchases = spark.createDataFrame([
        (1, 1, 101, "2024-10-10", 2)
    ], ["purchase_id", "customer_id", "product_id", "purchase_date", "quantity"])

    clv = customer_lifetime_value(purchases, products, customers)
    assert clv.collect()[0]["revenue_generated"] == 1000.0


def test_frequent_pairs_and_recommendation(spark):
    purchases = spark.createDataFrame([
        (1, 1, 10, "2024-01-01", 1),
        (1, 1, 20, "2024-01-01", 1),
        (2, 2, 20, "2024-01-02", 1),
        (2, 2, 30, "2024-01-02", 1),
    ], ["purchase_id", "customer_id", "product_id", "purchase_date", "quantity"])

    products = spark.createDataFrame([
        (10, "A", "cat1", 100.0),
        (20, "B", "cat1", 200.0),
        (30, "C", "cat1", 300.0)
    ], ["product_id", "product_name", "category", "price"])

    pairs = frequent_pairs(purchases, top_k=2)
    assert pairs.count() > 0

    cust_products = purchases.select("customer_id", "product_id").distinct()
    recs = recommend_for_customers_from_pairs(cust_products, pairs, products)
    assert "recommended_product" in recs.columns
    assert recs.count() > 0
