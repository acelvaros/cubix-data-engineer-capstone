from pyspark.sql import functions as sf
from pyspark.sql import DataFrame

PRODUCT_CATEGORY_MAPPING = {
    "pck": "ProductKey", # Correct mapping to match expected schema
    "pcak": "ProductCategoryKey", # Correct mapping to match expected schema
    "epcn": "EnglishProductCategoryName",
    "spcn": "SpanishProductCategoryName",
    "fpcn": "FrenchProductCategoryName"
}

def get_product_category(products_category_raw: DataFrame) -> DataFrame:
    """Map and filter Product Subcategory data.

    :param product_category_raw: Raw Product Subcategory data.
    :return: Mapped and filtered Product Subcategory data.
    """

    return(
        products_category_raw
        .select(
            sf.col("pck").cast("int"),
            sf.col("pcak").cast("int"),
            sf.col("epcn"),
            sf.col("spcn"),
            sf.col("fpcn")
        )
        .withColumnsRenamed(PRODUCT_CATEGORY_MAPPING)
        .dropDuplicates()
    )