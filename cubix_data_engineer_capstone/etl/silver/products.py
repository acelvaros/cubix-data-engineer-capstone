from pyspark.sql import functions as sf
from pyspark.sql import DataFrame
from decimal import Decimal

PRODUCTS_MAPPING = {
    "pk": "ProductKey",
    "psck": "ProductSubCategoryKey",
    "name": "ProductName",
    "stancost": "StandardCost",
    "dealerprice": "DealerPrice",
    "listprice": "ListPrice",
    "color": "Color",
    "size": "Size",
    "range": "Range",
    "weight": "Weight",
    "nameofmodel": "ModelName",
    "ssl": "SafetyStockLevel",
    "desc": "Description",
    "extra_col": "ProfitMargin"  # Assuming you want to keep this column
}

def get_products(products_raw: DataFrame) -> DataFrame:
    """Map and filter Products data.

    :param products_raw: Raw Products data.
    :return: Mapped and filtered Products data.
    """
    # Select and cast the columns
    products_mapped = (
        products_raw
        .select(
            sf.col("pk").cast("int"),
            sf.col("psck").cast("int"),
            sf.col("name"),
            sf.col("stancost").cast("decimal(10,2)"),
            sf.col("dealerprice").cast("decimal(10,2)"),
            sf.col("listprice").cast("decimal(10,2)"),
            sf.col("color"),
            sf.col("size").cast("string"),
            sf.when(sf.col("range") == "N/A", sf.lit(None).cast("string")).alias("Range"),
            sf.col("weight").cast("decimal(10,2)"),
            sf.col("nameofmodel"),
            sf.col("ssl").cast("int"),
            sf.col("desc"),
            # Set ProfitMargin to a fixed value
            sf.lit(Decimal("2.00")).cast("decimal(10,2)").alias("ProfitMargin")
        )
    )

    # Apply the column renaming
    for old_name, new_name in PRODUCTS_MAPPING.items():
        products_mapped = products_mapped.withColumnRenamed(old_name, new_name)

    # Drop duplicates
    return products_mapped.dropDuplicates()
