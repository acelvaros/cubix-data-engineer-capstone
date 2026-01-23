from datetime import datetime
from decimal import Decimal
from unittest.mock import patch

import pyspark.sql.types as st
import pyspark.testing as spark_testing
from cubix_data_engineer_capstone.etl.gold.wide_sales import _join_master_tables, get_wide_sales

def test_join_master_tables(spark):
    """
    Positive test that the function _join_master_tables returns the expected DataFrame
    """
    sales_master_test_data = [
        ("5001", datetime(2017, 1, 1), 2, 3)
    ]
    sales_schema = st.StructType([
            st.StructField("SalesOrderNumber", st.StringType(), True),
            st.StructField("OrderDate", st.TimestampType(), True),
            st.StructField("CustomerKey", st.IntegerType(), True),
            st.StructField("ProductKey", st.IntegerType(), True)
    ])
    sales_master_test = spark.createDataFrame(sales_master_test_data, schema=sales_schema)

    calendar_master_test_data = [
        (datetime(2017, 1, 1), "Monday")
    ]
    calendar_schema = st.StructType([
            st.StructField("Date", st.TimestampType(), True),
            st.StructField("DayName", st.StringType(), True)
    ])
    calendar_master_test = spark.createDataFrame(
        calendar_master_test_data,
        schema=calendar_schema
    )

    customers_master_test_data = [
        (2, "John Doe")
    ]
    customer_schema = st.StructType([
            st.StructField("CustomerKey", st.IntegerType(), True),
            st.StructField("Name", st.StringType(), True)
    ])
    customers_master_test = spark.createDataFrame(
        customers_master_test_data,
        schema=customer_schema
    )

    product_master_test_data = [
        (3, 4, "Test Pruduct")
    ]
    products_schema = st.StructType([
            st.StructField("ProductKey", st.IntegerType(), True),
            st.StructField("ProductSubcategoryKey", st.IntegerType(), True),
            st.StructField("ProductName", st.StringType(), True)
    ])
    products_master_test = spark.createDataFrame(
        product_master_test_data,
        schema=products_schema
    )

    product_subcategory_master_test_data = [
        (4, 5, "Test Product Subcategory")
    ]
    product_subcategory_schema = st.StructType([
            st.StructField("ProductSubcategoryKey", st.IntegerType(), True),
            st.StructField("ProductCategoryKey", st.IntegerType(), True),
            st.StructField("EnglishProductSubcategoryName", st.StringType(), True)
    ])
    product_subcategory_master_test = spark.createDataFrame(
        product_subcategory_master_test_data, schema=product_subcategory_schema
    )

    product_category_master_test_data = [
        (5, "Test Product Category")
    ]
    product_category_schema = st.StructType([
            st.StructField("ProductCategoryKey", st.IntegerType(), True),
            st.StructField("EnglishProductCategoryName", st.StringType(), True)
    ])
    product_category_master_test = spark.createDataFrame(
        product_category_master_test_data,
        schema=product_category_schema
    )

    result = _join_master_tables(
        sales_master_test,
        calendar_master_test,
        customers_master_test,
        products_master_test,
        product_subcategory_master_test,
        product_category_master_test
    )

    excepted_schema = st.StructType([
            st.StructField("ProductKey", st.IntegerType(), True),
            st.StructField("CustomerKey", st.IntegerType(), True),
            st.StructField("SalesOrderNumber", st.StringType(), True),
            st.StructField("OrderDate", st.TimestampType(), True),
            st.StructField("DayName", st.StringType(), True),
            st.StructField("Name", st.StringType(), True),
            st.StructField("ProductSubcategoryKey", st.IntegerType(), True),
            st.StructField("ProductName", st.StringType(), True),
            st.StructField("ProductCategoryKey", st.IntegerType(), True),
            st.StructField("EnglishProductSubcategoryName", st.StringType(), True),
            st.StructField("EnglishProductCategoryName", st.StringType(), True)
    ])

    excepted_data = [
        (
            3,
            2,
            "5001",
            datetime(2017, 1, 1),
            "Monday",
            "John Doe",
            4,
            "Test Pruduct",
            5,
            "Test Product Subcategory",
            "Test Product Category"            
        )
    ]

    expected = spark.createDataFrame(excepted_data, schema=excepted_schema)

    spark_testing.assertDataFrameEqual(result, expected)