from pyspark.sql import DataFrame, SparkSession

def read_file_from_volume(full_path: str, format: str) -> DataFrame:
    """Reads a file from a UnityCatalog volume and returns it as a Spark DataFrame.
    
    :param full_path:  The full path to the file in the volume.
    :param format:     The format of the file ("csv", "parquet", "delta").
    :return:           DataFrame with the loaded data.
    """
    if format not in ["csv", "parquet", "delta"]:
        raise ValueError(f"Invalid format: {format}. Supported formats are: csv, parquet, delta.")

    spark = SparkSession.getActiveSession()
    
    reader = spark.read.format("csv").option("header", "true")
    if format == "csv":
        reader = reader.option("header", "true")

    return reader.load(full_path)