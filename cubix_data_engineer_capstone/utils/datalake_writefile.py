from pyspark.sql import DataFrame

from cubix_data_engineer_capstone.utils.config import STORAGE_ACCOUNT_NAME

def write_file_to_datalake(
    df: DataFrame,
    container_name: str,
    file_path: str,
    format: str,
    mode: str= "overwrite",
    partition_by: list[str] = None
) -> None:
    """Writes a DataFrameto Azure Data Lake as a parquet / csv / delta format.
    :param df: DataFrameto be written.
    :param container_name: The name of the file system (container) in Azure Data Lake.
    :param file_path: The path to the file in the data lake.
    :param format: The format of the file ("csv", "json", "delta", "parquet").
    :param mode: Default "overwrite", write mode.
    :param partition_by: List of column to partition by, default is None.
    """

    if format not in ["csv", "delta", "parquet"]:
        raiseValueError(f"Invalidformat: {format}. Supported formats are: csv, parquet, delta.")
    full_path = f"abfss://{container_name}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{file_path}"
    writer = df.write.mode(mode).format(format)
    if format == "csv":
        writer= writer.option("header", True)

    if partition_by:
        writer= writer.partitionBy(*partition_by)

    writer.save(full_path)
