from pathlib import Path
from prefect import flow
from ingest_utils import dataframe_utils, duckdb_utils

@flow(log_prints=True)
def create_dictionary_of_dataset_objects(datasets: list, directory: Path) -> dict:
    """create a dictionary of data objects for catch, size and trip datasets based on optimal processing method"""
    dict_dfs = {}
    for dataset in datasets:
        k = f'{dataset}'
        v = dataframe_utils.csv_to_polars_df(dataset, directory)

        dict_dfs[k] = v

    return dict_dfs

@flow(log_prints=True)
def create_tables_for_datasets(duckdb_path: str, dict_dfs: dict, schema: str) -> None:
    """Create table in db for each dataset"""
    for k, v in dict_dfs.items():
        dataset = k.split('_')[0]
        query_string = duckdb_utils.create_table_query(dataset)
        duckdb_utils.execute_duckdb_query(duckdb_path, query_string, data_object = v)
        duckdb_utils.results_duckdb_query(duckdb_path, "SELECT * FROM INFORMATION_SCHEMA.tables")
        duckdb_utils.results_duckdb_query(duckdb_path, f"SELECT * FROM noaa_dw.{schema}.{dataset}")
        duckdb_utils.results_duckdb_query(duckdb_path, f"SELECT COUNT(*) FROM noaa_dw.{schema}.{dataset}")
        duckdb_utils.results_duckdb_query(duckdb_path, f"DESCRIBE noaa_dw.{schema}.{dataset}")

@flow(log_prints=True)
def drop_db_objects(duckdb_path: str, schema: str) -> None:
    """drop schema and all tables within it"""
    query_string = duckdb_utils.drop_schema_query(schema)
    duckdb_utils.execute_duckdb_query(duckdb_path, query_string)
    duckdb_utils.results_duckdb_query(duckdb_path, "SELECT * FROM INFORMATION_SCHEMA.tables")