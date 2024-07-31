import duckdb
from prefect import flow
from pathlib import Path
from ingest_utils import dlt_utils

#Constants
PROJECT_PATH = str(Path(__file__).resolve().parent.parent)
DUCKDB_PATH = PROJECT_PATH + "/data/noaa_dw.duckdb"
PARQUET_PATH = PROJECT_PATH + "/data"

@flow(log_prints=True)
def export_parquet(duckdb_path: str = DUCKDB_PATH, table: str = "analytics.trip_details", row_group_size: int = 50000) -> None:
    print(f'Writing data to parquet file')

    """Write table data to parquet file"""
    query_string = f"""
                    SET preserve_insertion_order = false;
                    COPY {table} TO '{PARQUET_PATH}/{table}.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE {row_group_size});
                    """
    with duckdb.connect(duckdb_path) as con:
        con.sql(query_string)

if __name__ == "__main__":
    table = "analytics.trip_details"
    row_group_size = 50000
    export_parquet(DUCKDB_PATH, table, row_group_size)