import pyspark.sql.types as st
import pyspark.testing as spark_testing
from cubix_data_engineer_capstone.etl.silver.product_category import get_product_category

def test_get_product_category(spark):
    """
    Positive test that the function get_product_category returns the expected DataFrame
    """

    test_data = spark.createDataFrame(
        [
            # include - samle to keep
            ("1", "Bikes", "Bicicleta", "Vélo", "extra_value"),
            # exclude - duplicate
            ("1", "Bikes", "Bicicleta", "Vélo", "extra_value"),
            # include - unmodified values
            ("2", "Components", "Componente", "Composant", "extra_value"),
            ("3", "Clothing", "Prenda", "Vêtements", "extra_value"),
            ("4", "Accessories", "Accesorio", "Accessoire", "extra_value"),

        ],
        schema=[
            "pck",
            "epcn",
            "spcn",
            "fpcn",
            "extra_col"
        ]
    )

    result = get_product_category(test_data)

    excpected_schema = st.StructType(
        [
            st.StructField("ProductCategoryKey", st.IntegerType(), True),
            st.StructField("EnglishProductCategoryName", st.StringType(), True),
            st.StructField("SpanishProductCategoryName", st.StringType(), True),
            st.StructField("FrenchProductCategoryName", st.StringType(), True)
        ]
    )

    excpected = spark.createDataFrame(
        [
            (
                1,
                "Bikes",
                "Bicicleta",
                "Vélo"
            ),
            (
                2,
                "Components",
                "Componente",
                "Composant"
            ),
            (
                3,
                "Clothing",
                "Prenda",
                "Vêtements"
            ),
            (
                4,
                "Accessories",
                "Accesorio",
                "Accessoire"
            )
        ],
        schema=excpected_schema
    )

    spark_testing.assertDataFrameEqual(result, excpected)