import duckdb
import pandas as pd
import polars as pl
from prefect import task

@task()
def create_table_query(dataset: str) -> str:
    """Creates table in raw schema for a particular dataset"""

    query_string = f"""
            CREATE SCHEMA IF NOT EXISTS raw;
            USE noaa_dw.raw;
            CREATE OR REPLACE TABLE {dataset} AS
            SELECT * FROM 'data_object';
            """

    return query_string

@task()
def drop_schema_query(schema: str) -> str:
    """Drop schema recursively"""

    query_string = f"DROP SCHEMA IF EXISTS {schema} CASCADE"

    return query_string

@task()
def execute_duckdb_query(duckdb_path: str, query_string: str, **data_object: pl.DataFrame) -> None:
    print(f'Executing duckdb query: \n{query_string}\n')

    """Execute query in duckdb"""
    with duckdb.connect(duckdb_path) as con:
        for v in data_object.values():
            data_object = v
        try:
            con.execute(query_string) 
        except Exception as e:
            print(f"Error executing query: {e}")       

@task()
def results_duckdb_query(duckdb_path: str, query_string: str) -> None:
    print(f'Returing results for duckdb query: \n{query_string}\n')

    """Execute query in duckdb and show results"""
    with duckdb.connect(duckdb_path) as con:
        con.sql(query_string).show()
