from pyspark.sql import functions as sf
from pyspark.sql import DataFrame

PRODUCT_CATEGORY_MAPPING = {
    "pck": "ProductKey", # Correct mapping to match expected schema
    "pcak": "ProductCategoryKey", # Correct mapping to match expected schema
    "epcn": "EnglishProductCategoryName",
    "spcn": "SpanishProductCategoryName",
    "fpcn": "FrenchProductCategoryName"
}

def get_product_category(product_category_raw: DataFrame) -> DataFrame:
    """Map and filter Product Subcategory data.

    :param product_category_raw: Raw Product Subcategory data.
    :return: Mapped and filtered Product Subcategory data.
    """
    # Select and cast the columns
    product_category_mapped = (
        product_category_raw
        .select(
            sf.col("pck").cast("int"),
            sf.col("pcak").cast("int"),
            sf.col("epcn"),
            sf.col("spcn"),
            sf.col("fpcn")
        )
    )

    # Apply the column renaming
    for old_name, new_name in PRODUCT_CATEGORY_MAPPING.items():
        product_category_mapped = product_category_mapped.withColumnRenamed(old_name, new_name)

    # Drop duplicates
    return product_category_mapped.dropDuplicates()
