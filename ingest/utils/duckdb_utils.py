import duckdb
from pathlib import Path
import pandas as pd
import polars as pl

def create_table_query(dataset: str) -> str:
    """Creates table in raw schema for a particular dataset"""

    query_string = f"""
            CREATE SCHEMA IF NOT EXISTS raw;
            USE noaa_dw.raw;
            CREATE OR REPLACE TABLE {dataset} AS
            SELECT * FROM 'data_object'; 
            """

    return query_string

def drop_schema_query(schema: str) -> str:
    """Drop schema recursively"""

    query_string = f"DROP SCHEMA IF EXISTS {schema} CASCADE"

    return query_string

def parse_csv_query(dataset: str, directory: Path) -> str:
    """Print out the file size of all csv files with matching
    dataset and create query for reading that dataset"""

    query_string = f"""
            SELECT * FROM read_csv('{directory}/{dataset}_*.csv',
            normalize_names = false,
            ignore_errors = false,
            union_by_name = true,
            filename =  false,
            auto_detect = true,
            null_padding = true,
            all_varchar = true
            )
            """

    return query_string

def execute_duckdb_query(duckdb_path: str, query_string: str, **data_object: pl.DataFrame | pd.DataFrame | duckdb.DuckDBPyRelation) -> None:
    print(f'Executing duckdb query: \n{query_string}\n')

    """Execute query in duckdb"""
    with duckdb.connect(duckdb_path) as con:
        for v in data_object.values():
            data_object = v
        try:
            con.execute(query_string) 
        except Exception as e:
            print(f"Error executing query: {e}")       

def results_duckdb_query(duckdb_path: str, query_string: str) -> None:
    print(f'Returing results for duckdb query: \n{query_string}\n')

    """Execute query in duckdb and show results"""
    with duckdb.connect(duckdb_path) as con:
        con.sql(query_string).show()

def create_duckdb_relation(duckdb_path: str, dataset: str, directory: Path) -> duckdb.DuckDBPyRelation:
    """Create duckdb relation"""
    print(f'Creating duckdb relation for {dataset} dataset\n')

    query_string = parse_csv_query(dataset, directory)

    with duckdb.connect(duckdb_path) as con:
        relation = con.sql(query_string)
        print(f'\nData Object Details for {dataset}_duckdb_relation\n')
        print(f'\nShape before cleaning: {relation.shape}\n')
        print(relation.types) 
    
    return relation

def clean_duckdb_relation(duckdb_path: str, relation: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    with duckdb.connect(duckdb_path) as con:
        relation = relation.filter("<> NULL")
        print(f'\nShape after cleaning: {relation.shape}\n')
        print(relation.types) 

    return relation

def csv_to_duckdb_relation(duckdb_path: str, dataset: str, directory: Path) -> duckdb.DuckDBPyRelation:
    """process csv files into duckdb relation"""
    relation = create_duckdb_relation(duckdb_path, dataset, directory)
    relation = clean_duckdb_relation(duckdb_path, relation)

    return relation

def duckdb_relation_to_pandas_df_(duckdb_path: str, relation: duckdb.DuckDBPyRelation, dataset: str, chunk: bool = True) -> pd.DataFrame:
    """Convert duckdb relation into pandas dataframe"""
    print(f'Converting duckdb relation into pandas dataframe for {dataset} dataset\n')

    if chunk == True:
        with duckdb.connect(duckdb_path) as con:
            df = relation.fetch_df_chunk(5)
    elif chunk == False:
        with duckdb.connect(duckdb_path) as con:
            df = relation.df()

    return df

def duckdb_relation_to_polars_df(duckdb_path: str, relation: duckdb.DuckDBPyRelation, dataset: str) -> pl.DataFrame:
    """Convert duckdb relation into polars dataframe"""
    print(f'Converting duckdb relation into polars dataframe for {dataset} dataset\n')

    with duckdb.connect(duckdb_path) as con:
        df = relation.pl()

    return df