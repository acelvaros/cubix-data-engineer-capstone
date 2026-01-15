from pyspark.sql import DataFrame, SparkSession

from cubix_data_engineer_capstone.utils.config import STORAGE_ACCOUNT_NAME

def read_parquet_from_datalake(container: str, file_path: str) -> DataFrame:
    """Reads a Parquet file from Azure Data Lake Storage into a Spark DataFrame.
    
    :param container: The name of the ADLS container.
    :param file_path: The path to the Parquet file within the container.
    :param format: The file format to read (default is 'parquet', can be "csv", "json", "delta", etc.).
    :return: A Spark DataFrame containing the data from the Parquet file.
    """

    #file_path = f"abfs://{container_name}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/source_system/calendar.csv"
    file_path = f"abfs://{container_name}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/source_system/{file_path}"

    spark = SparkSession.getActiveSession()
    if not spark:
        raise RuntimeError("No active Spark session found.")
    
    if format == "josn":
        df = spark.read.json(file_path)
        return
    else:
        df = (
            spark
            .read
            .format("csv")
            .option("header", "true")
            .load(file_path, format=format)
        )
    
    """
        df = (
            spark
            .read
            .format("csv")
            .option("header", "true")
            .load("abfs://capstoneproject@dlcubixdataengineerazure.dfs.core.windows.net/source_system/calendar.csv")
        )
    """

    return df