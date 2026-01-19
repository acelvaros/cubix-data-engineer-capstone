from cubix_data_engineer_capstone.utils.datalake import read_file_from_datalake, write_file_to_datalake

def bronze_ingest(
    source_path: str,
    bronze_path: str,
    file_name: str,
    container_name: str,
    format: str,
    moder: str,
    partition_by: list[str]
):
    """Extract files from the source, and load them to the desired container.

    :param source_path: Path to source file.
    :param bronze_path: Path to the bronze layer.
    :param container_name: Name of the container holding the files.
    :param format: Format of the input file.
    :param mode: Mode "append", "overwrite", "ignore".
    :param partition_by: Column(s) to partition on. "None" by default.
    """
    df = read_file_from_datalake(container_name, f"{source_path}/{file_name}", format)
    
    return write_file_to_datalake(
        df=df,
        container_name=container_name,
        full_path=f"{bronze_path}/{file_name}",
        format=format,
        mode=mode,
        partition_by=partition_by
    )