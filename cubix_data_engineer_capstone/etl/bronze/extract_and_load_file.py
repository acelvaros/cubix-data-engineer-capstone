from cubix_data_engineer_capstone.utils.databricks import read_file_from_volume, write_file_to_volume

def bronze_ingest(
        source_path: str,
        bronze_path: str,
        file_name: str,
        container_name: str,
        partition_by: list[str] = None
):
    """Extract files from the source, and load them to the desired container.

    :param source_path:                 Path to source file.
    :param bronze_path:                 Path to the bronze layer.
    :param file_name:                   Name of the file to ingest.
    :param container_name:              Name of the container holding the files.
    :param partition_by:                Column(s) to partition on. "None" by default.
    """
    df = read_file_from_volume(container_name, f"{source_path}/{file_name}", "csv")

    return write_file_to_volume(
        df=df,
        container_name=container_name,
        file_path=f"{bronze_path}/{file_name}",
        format="csv",
        mode="overwrite",
        partition_by=partition_by
    )


def bronze_ingest_volume(
        source_path: str,
        bronze_path: str,
        file_name: str,
        partition_by: list[str] = None,
):
    """Extract files from the source, and load them to the Bronze volume.

    :param source_path:                 Path to source file.
    :param bronze_path:                 Path to the bronze layer.
    :param file_name:                   Name of the file to ingest.
    :param partition_by:                Column(s) to partition on. "None" by default.
    """
    df = read_file_from_volume(f"{source_path}/{file_name}", "csv")

    return write_file_to_volume(
        df=df,
        full_path=f"{bronze_path}/{file_name}",
        format="csv",
        mode="overwrite",
        partition_by=partition_by
    )
