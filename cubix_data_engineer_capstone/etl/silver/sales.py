import pyspark.sql.functions as sf
from pyspark.sql import DataFrame

SALES_MAPPING = {
    "son": "SalesOrderNumber",
    "orderdate": "OrderDate",
    "pk": "ProductKey",
    "ck": "CustomerKey",
    "dateofshipping": "ShipDate",
    "oquantity": "OrderQuantity"
}

def get_sales(sales_raw: DataFrame) -> DataFrame:
    """Map and filter Sales data.

    :param sales_raw:   Raw Sales data.
    :return:            Mapped and filtered Sales data.

    """
    return(
        sales_raw
        .select(
            sf.col("son"),
            sf.col("orderdate").cast("date"),
            sf.col("pk").cast("int"),
            sf.col("ck").cast("int"),
            sf.col("dateofshipping").cast("date"),
            sf.col("oquantity").cast("int")
        )
        .withColumnsRenamed(SALES_MAPPING)
        .dropDuplicates()
    )


"""
    # Select and cast the columns
    sales_mapped = (
        sales_raw
        .select(
            sf.col("son"),
            sf.col("orderdate").cast("date"),
            sf.col("pk").cast("int"),
            sf.col("ck").cast("int"),
            sf.col("dateofshipping").cast("date"),
            sf.col("oquantity").cast("int"),
        )
    )

    # Rename columns according to SALES_MAPPING
    for old_name, new_name in SALES_MAPPING.items():
        sales_mapped = sales_mapped.withColumnRenamed(old_name, new_name)

    # Drop duplicates
    return sales_mapped.dropDuplicates()
"""