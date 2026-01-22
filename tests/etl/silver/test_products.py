import pyspark.sql.types as st
import pyspark.testing as spark_testing
from cubix_data_engineer_capstone.etl.silver.products import get_products
from decimal import Decimal

def test_get_products(spark):
    """
    Positive test that the function get_products returns the expected DataFrame
    """

    test_data = spark.createDataFrame(
        [
            # include - samle to keep
            ("1", "1", "name_1", "10.1111", "12.1111", "14.1111", "color_1", "40", "N/A", "1.6543", "nameofmodel_1", "500", "desc_1", "extra_col"), # noqa: E501
            # exclude - duplicate
            ("1", "1", "name_1", "10.1111", "12.1111", "14.1111", "color_1", "40", "N/A", "1.6543", "nameofmodel_1", "500", "desc_1", "extra_col"), # noqa: E501
        ],
        schema=[
            "pk",
            "psck",
            "name",
            "stancost",
            "dealerprice",
            "listprice",
            "color",
            "size",
            "range",
            "weight",
            "nameofmodel",
            "ssl",
            "desc",
            "extra_col"
        ]
    )

    result = get_products(test_data)

    excpected_schema = st.StructType(
        [
            st.StructField("ProductKey", st.IntegerType(), True),
            st.StructField("ProductSubCategoryKey", st.IntegerType(), True),
            st.StructField("ProductName", st.StringType(), True),
            st.StructField("StandardCost", st.DecimalType(10, 2), True),
            st.StructField("DealerPrice", st.DecimalType(10, 2), True),
            st.StructField("ListPrice", st.DecimalType(10, 2), True),
            st.StructField("Color", st.StringType(), True),
            st.StructField("Size", st.IntegerType(), True),
            st.StructField("SizeRange", st.StringType(), True),
            st.StructField("Weight", st.DecimalType(10, 2), True),
            st.StructField("ModelName", st.StringType(), True),
            st.StructField("SafetyStockLevel", st.IntegerType(), True),
            st.StructField("Description", st.StringType(), True),
            st.StructField("ProfitMargin", st.DecimalType(10, 2), True)
        ]
    )
       
    excpected = spark.createDataFrame(
        [
            (
                1,
                1,
                "name_1",
                Decimal("10.11"),
                Decimal("12.11"),
                Decimal("14.11"),
                "color_1",
                40,
                None,
                Decimal("1.65"),
                "nameofmodel_1",
                500,
                "desc_1",
                Decimal("2.0")
            )
        ],
        schema=excpected_schema
    )

    spark_testing.assertDataFrameEqual(result, excpected)