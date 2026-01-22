from pyspark.sql import functions as sf
from pyspark.sql import DataFrame

PRODUCT_SUBCATEGORY_MAPPING = {
    "psk": "ProductKey",  # Correct mapping to match expected schema
    "pck": "ProductSubCategoryKey",
    "epsn": "EnglishProductSubcategoryName",
    "spsn": "SpanishProductSubcategoryName",
    "fpsn": "FrenchProductSubcategoryName"
}

def get_product_subcategory(products_subcategory_raw: DataFrame) -> DataFrame:
    """Map and filter Product Subcategory data.

    :param product_subcategory_raw: Raw Product Subcategory data.
    :return: Mapped and filtered Product Subcategory data.
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

"""
    # Select and cast the columns
    product_subcategory_mapped = (
        product_subcategory_raw
        .select(
            sf.col("psk").cast("int"),
            sf.col("pck").cast("int"),
            sf.col("epsn"),
            sf.col("spsn"),
            sf.col("fpsn")
        )
    )

    # Apply the column renaming
    for old_name, new_name in PRODUCT_SUBCATEGORY_MAPPING.items():
        product_subcategory_mapped = product_subcategory_mapped.withColumnRenamed(old_name, new_name)

    # Drop duplicates
    return product_subcategory_mapped.dropDuplicates()
"""