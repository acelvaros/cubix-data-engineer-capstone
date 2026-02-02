import pyspark.sql.functions as sf
from pyspark.sql import DataFrame


def get_daily_product_category_metrics(wide_sales: DataFrame) -> DataFrame:
    """
    Calculates daily product category metrics from the wide_sales DataFrame.

    Step-by-step phases:
    1. Group the input DataFrame by "EnglishProductCategoryName".
    2. Aggregate the following metrics for each group:
        - Sum of "SalesAmount" as "SalesAmountSum"
        - Average of "SalesAmount" (rounded to 2 decimals) as "SalesAmountAvg"
        - Sum of "Profit" as "ProfitSum"
        - Average of "Profit" (rounded to 2 decimals) as "ProfitAvg"
    3. Return the resulting DataFrame with the aggregated metrics.

    :param wide_sales:  Input DataFrame containing wide sales data.
    :return:            DataFrame with daily metrics including "SalesAmountSum", "SalesAmountAvg",
                        "ProfitSum", and "ProfitAvg" grouped by "OrderDate".
    """

    return (
        wide_sales
        .groupBy("EnglishProductCategoryName")
        .agg(
            sf.sum(sf.col("SalesAmount")).alias("SalesAmountSum"),
            sf.round(sf.avg(sf.col("SalesAmount")), 2).alias("SalesAmountAvg"),
            sf.sum(sf.col("Profit")).alias("ProfitSum"),
            sf.round(sf.avg(sf.col("Profit")), 2).alias("ProfitAvg"),
        )
    )
