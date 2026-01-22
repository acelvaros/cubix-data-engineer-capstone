from delta.tables import DeltaTable     # type: ignore
from pyspark.sql import DataFrame, SparkSession

from cubix_data_engineer_capstone.utils.config import STORAGE_ACCOUNT_NAME


def scd1(spark: SparkSession, container_name: str, file_path: str, new_data: DataFrame, primary_key: str):
    """Slowly Changing Dimension Type 1 implementation. Compare the master Delta table with the "new_data",
    if there are changes on the given primary key, then update, if it's not found in master, then insert.

    :param spark:           SparkSession.
    :param container_name:  Name of the container holding the Delta table.
    :param file_path:       Path to the Delta table.
    :param new_data:        DataFrame with the new data.
    :param primary_key:     Primary Key to join the Master Delta table with the new data.
    """
    master_path = f"abfss://{container_name}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{file_path}"
    delta_master = DeltaTable.forPath(spark, master_path)

    (
        delta_master
        .alias("master")
        .merge(
            new_data.alias("updates"),
            f"master.{primary_key} = updates.{primary_key}"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def scd1_uc(spark: SparkSession, table_name: str, new_data: DataFrame, primary_key: str):
    """
    Slowly Changing Dimension Type 1 for UC Volumes.
    Compares the master Delta table with new_data, updating or inserting as needed.

    :param spark:       SparkSession.
    :param table_name:  Name of the table.
    :param new_data:    DataFrame with the new data.
    :param primary_key: Column name used as primary key.
    """
    delta_master = DeltaTable.forName(spark, table_name)

    (
        delta_master
        .alias("master")
        .merge(
            new_data.alias("updates"),
            f"master.{primary_key} = updates.{primary_key}"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )