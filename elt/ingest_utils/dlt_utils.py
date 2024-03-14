import dlt
import pandas as pd
from pathlib import Path
from prefect import flow, task

@task()
def get_csv_files_by_dataset(dataset: str, directory: Path) -> list:
    """Get list of csv files that describe a particular dataset: catch, size, trip"""
    csv_files = list(directory.glob(f"**/{dataset}_*.csv"))

    return csv_files

@flow(log_prints=True)
def load_csv_data_duckdb(dataset: str, directory: Path, schema: str) -> None:

    # Create a dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name = "dlt_load_csv_data",
        destination=dlt.destinations.duckdb(credentials="noaa_dw.duckdb"),
        dataset_name=schema,
        # full_refresh=True,
        progress="log",
    )

    # Load each CSV file into the same table
    csv_files = get_csv_files_by_dataset(dataset, directory)
    for csv_file in csv_files:
        df = pd.read_csv(csv_file, low_memory=False, dtype='object')
        load_info = pipeline.run(
                                df,
                                write_disposition="append", 
                                table_name=dataset
                                )

        print(csv_file)
        print(load_info)