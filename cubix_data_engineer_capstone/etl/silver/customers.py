import pyspark.sql.functions as sf
from pyspark.sql import DataFrame

CUSTOMERS_MAPPING = {
    "ck": "CustomerKey",
    "name": "Name",
    "bdate": "BirthDate",
    "ms": "MaritalStatus",
    "gender": "Gender",
    "income": "YearlyIncome",
    "childrenhome": "NumberChildren",
    "occ": "Occupation",
    "hof": "HouseOwnerFlag",
    "nco": "NumberCarsOwned",
    "addr1": "AddresLine1",
    "addr2": "AddresLine2",
    "phone": "Phone"
}

def get_customers(customers_raw: DataFrame) -> DataFrame:
    """Transform and filter Customers data.
    1. Selecting needed columns.
    2. Apply the column name mapping.
    3. Transform MaritalStatus.
    4. Transform Gender.
    5. Create FullAddress column.
    6. Create IncomeCategory column.
    7. Create BithYearcolumn.
    8. Drop duplicates.
    :param customers_raw: Raw Customers data
    :return:              Cleaned, filtered, and transformed Customers data.
    """
    customers_mapped = (
        customers_raw
        .select(
            sf.col("ck").cast("string"),
            sf.col("name"),
            sf.col("bdate").cast("date"),
            sf.when(sf.col("ms") == "M", "1").otherwise(sf.col("ms")).alias("MaritalStatus"),
            sf.when(sf.col("gender") == "F", "0").otherwise(sf.col("gender")).alias("Gender"),
            sf.col("income").cast("int"),
            sf.col("childrenhome").cast("int"),
            sf.col("occ"),
            sf.col("hof").cast("int"),
            sf.col("nco").cast("int"),
            sf.col("addr1"),
            sf.col("addr2"),
            sf.col("phone"),
            sf.concat(sf.col("addr1"), sf.lit(", "), sf.col("addr2")).alias("FullAddress"),
            sf.when(sf.col("income") <= 50000, "Low")
              .when(sf.col("income") <= 100000, "Medium")
              .otherwise("High").alias("IncomeCategory"),
            sf.year(sf.col("bdate")).alias("BirthYear")
        )
    )

    for old_name, new_name in CUSTOMERS_MAPPING.items():
        customers_mapped = customers_mapped.withColumnRenamed(old_name, new_name)

    return customers_mapped.dropDuplicates()