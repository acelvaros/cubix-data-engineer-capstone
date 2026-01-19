from pyspark.sql import DataFrame, SparkSession

from cubix_data_engineer_capstone.utils.config import STORAGE_ACCOUNT_NAME


def read_file_from_datalake(container_name: str, file_path: str, format: str) -> DataFrame:
    """Reads a file from Azure Data Lake and returns it as a Spark DataFrame.
    :param container_name: The name of the file system (container) in Azure Data Lake.
    :param file_path: The path to the file in the data lake.
    :param format: The format of the file ("csv", "json", "delta", "parquet").
    :return: DataFramewith a loaded data.
    """
    if format not in ["csv", "parquet", "delta", "json"]:
        raise ValueError(
            f"Invalidformat: {format}. Supported formats are: "
            "csv, json, parquet, delta."
        )

    full_path: str = (
        f"abfss://{container_name}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{file_path}"
    )
    spark = SparkSession.getActiveSession()
    if not spark:
        raise RuntimeError("No active SparkSessionfound.")

    if format == "json":
        df = spark.read.json(file_path)
        return df
    else:
        df = (
            spark
            .read
            .format(format)
            .option("header", "true")
            .load(full_path, format=format)
            )

    return df


def write_file_to_datalake(
        df: DataFrame,
        container_name: str,
        file_path: str,
        format: str,
        mode: str = "overwrite",
        partition_by: list[str] | None = None
):

    """Writes a DataFrameto Azure Data Lake as a parquet / csv / delta format.
    :param df: DataFrameto be written.
    :param container_name: The name of the file system (container) in Azure Data Lake.
    :param file_path: The path to the file in the data lake.
    :param format: The format of the file ("csv", "json", "delta", "parquet").
    :param mode: Default "overwrite", write mode.
    :param partition_by: List of column to partition by, default is None.
    """

if format not in ["csv", "parquet", "delta"]:
    raise ValueError(f"Invalid format: {format}. Supported formats are: csv, parquet, delta.")

full_path: str = (f"abfss://{container_name}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{file_path}")

writer = df.write.mode(mode).format(format)

if format == "csv":
    writer = writer.option("header", "true")

if partition_by:
    writer = writer.partitionBy(partition_by)

writer.save(full_path)
