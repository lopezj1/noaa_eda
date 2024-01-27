import duckdb
import time
from pathlib import Path

def create_table_query(con: duckdb.DuckDBPyConnection, dataset: str) -> str:
    """Creates table in raw schema for a particular dataset"""

    query_string = f"""
            CREATE SCHEMA IF NOT EXISTS raw;
            USE noaa_dw.raw;
            CREATE OR REPLACE TABLE {dataset} AS
            SELECT * FROM '{dataset}_df'; 
            """

    return query_string

def drop_schema_query(con: duckdb.DuckDBPyConnection, schema: str) -> str:
    """Drop schema recursively"""

    query_string = f"DROP SCHEMA {schema} CASCADE"

    return query_string

def parse_csv_query(dataset: str, directory: Path) -> str:
    """Print out the file size of all csv files with matching
    dataset and create query for reading that dataset"""

    query_string = f"""
            SELECT * FROM read_csv('{directory}/{dataset}_*.csv',
            normalize_names = true,
            ignore_errors = false,
            union_by_name = true,
            filename =  true,
            auto_detect = true,
            null_padding = true,
            all_varchar = true
            )
            """

    return query_string

def execute_duckdb_query(con: duckdb.DuckDBPyConnection, query_string: str) -> None:
    print(f'Executing duckdb query: \n{query_string}\n')

    """Execute query in duckdb"""
    t0 = time.time()
    con.execute(query_string) 
    t1 = time.time()
    total_time = round(t1 - t0, 2) 
    print(f'DuckDB query execution time: {total_time} seconds\n')

def create_duckdb_relation(con: duckdb.DuckDBPyConnection, dataset: str, directory: Path) -> duckdb.DuckDBPyRelation:
    """Create duckdb relation"""
    print(f'Creating duckdb relation for {dataset} dataset\n')

    query_string = parse_csv_query(dataset, directory)

    t0 = time.time()
    relation = con.sql(query_string)
    t1 = time.time()
    total_time = round(t1 - t0, 2) 
    print(f'DuckDB relation build time: {total_time} seconds\n')    
    
    return relation

def csv_to_duckdb_relation(dataset: str, db: str) -> duckdb.DuckDBPyRelation:
    """process csv files into duckdb relation"""
    with duckdb.connect(db) as con:
        relation = create_duckdb_relation(con, dataset)

    return relation

def clean_duckdb_relation(relation: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    return