from pyspark.sql import functions as sf
from pyspark.sql import DataFrame

PRODUCT_CATEGORY_MAPPING = {
    "pck": "ProductCategoryKey", # Correct mapping to match expected schema
    "epcn": "EnglishProductCategoryName",
    "spcn": "SpanishProductCategoryName",
    "fpcn": "FrenchProductCategoryName"
}

def get_product_category(products_category_raw: DataFrame) -> DataFrame:
    """Map and filter Product Subcategory data.

    Step-by-step phases:
    1. Select and cast the relevant columns from the input DataFrame:
        - Cast "pck" to integer
        - Select "epcn", "spcn", "fpcn"
    2. Rename columns according to PRODUCT_CATEGORY_MAPPING to match the expected schema.
    3. Remove duplicate rows from the resulting DataFrame.

    :param product_category_raw: Raw Product Subcategory data.
    :return: Mapped and filtered Product Subcategory data.
    """

    return(
        products_category_raw
        .select(
            sf.col("pck").cast("int"),
            sf.col("epcn"),
            sf.col("spcn"),
            sf.col("fpcn")
        )
        .withColumnsRenamed(PRODUCT_CATEGORY_MAPPING)
        .dropDuplicates()
    )
