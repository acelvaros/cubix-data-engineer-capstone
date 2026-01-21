import pyspark.sql.functions as sf
from pyspark.sql import DataFrame

PRODUCT_SUBCATEGORY_MAPPING= {
"psk": "ProductSubcategoryKey",
"pck": "ProductCategoryKey",
"epsn": "EnglishProductSubcategoryName",
"spsn": "SpanishProductSubcategoryName",
"fpsn": "FrenchProductSubcategoryName",
}

def get_product_subcategory(products_subcategory_raw: DataFrame) -> DataFrame:
    """Transform and filter Product Subcategory data.
    1. Select needed columns, and cast data types.
    2. Rename columns.
    :param products_subcategory_raw: Raw Product Subcategory data
    :return: Cleaned, filtered, and transformed Product Subcategory data.
    """
        
    return(
        products_subcategory_raw
        .select(
            sf.col("psk").cast("int"),
            sf.col("pck").cast("int"),
            sf.col("epsn"),
            sf.col("spsn"),
            sf.col("fpsn")
        )
        .withColumnsRenamed(PRODUCT_SUBCATEGORY_MAPPING)
        .dropDuplicates()
    )