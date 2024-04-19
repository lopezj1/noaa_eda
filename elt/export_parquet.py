import duckdb
from prefect import flow
from pathlib import Path
from ingest_utils import dlt_utils

#Constants
PROJECT_PATH = str(Path(__file__).resolve().parent.parent)
DUCKDB_PATH = PROJECT_PATH + "/noaa_dw.duckdb"
PARQUET_PATH = PROJECT_PATH + "/app/data"

@flow(log_prints=True)
def export_parquet(duckdb_path: str = DUCKDB_PATH, table: str = "analytics.trip_details") -> None:
    print(f'Writing data to parquet file')

    """Write table data to parquet file"""
    query_string = f'SELECT * FROM {table}'
    with duckdb.connect(duckdb_path) as con:
        r = con.sql(query_string)
        r.to_parquet(file_name=f'{PARQUET_PATH}/{table}.parquet')

if __name__ == "__main__":
    table = "analytics.trip_details"
    export_parquet(DUCKDB_PATH, table)
    # dlt_utils.write_duckdb_table_to_parquet(DUCKDB_PATH, table)