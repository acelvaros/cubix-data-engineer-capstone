from decimal import Decimal

import pyspark.sql.types as st
import pyspark.testing as spark_testing
from cubix_data_engineer_capstone.etl.silver.product_subcategory import get_product_subcategory

def test_get_product_subcategory(spark):
    """
    Positive test that the function get_product_subcategory returns the expected DataFrame
    """

    test_data = spark.createDataFrame(
        [
            # include - samle to keep
            ("1", "1", "english_name_1", "spanish_1", "french_name_1", "extra_value"),
            # exclude - duplicate
            ("1", "1", "english_name_1", "spanish_1", "french_name_1", "extra_value")
        ],
        schema=[
            "psk",
            "pck",
            "epsn",
            "spsn",
            "fpsn",
            "extra_col"
        ]
    )

    result = get_product_subcategory(test_data)

    excpected_schema = st.StructType(
        [
            st.StructField("ProductKey", st.IntegerType(), True),
            st.StructField("ProductSubCategoryKey", st.IntegerType(), True),
            st.StructField("EnglishProductSubcategoryName", st.StringType(), True),
            st.StructField("SpanishProductSubcategoryName", st.StringType(), True),
            st.StructField("FrenchProductSubcategoryName", st.StringType(), True)
        ]
    )

    excpected = spark.createDataFrame(
        [
            (
                1,
                1,
                "english_name_1",
                "spanish_1",
                "french_name_1"
            )
        ],
        schema=excpected_schema
    )

    spark_testing.assertDataFrameEqual(result, excpected)