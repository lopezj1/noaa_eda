import pandas as pd
import polars as pl
import duckdb
import time
from pathlib import Path
from noaa_utils import parse_csv_query

def create_pandas_df(con: duckdb.DuckDBPyConnection, dataset: str, directory: Path, chunk: bool = True) -> pd.DataFrame:
    """Parse csv files into pandas dataframe"""
    print(f'Creating pandas dataframe for {dataset} dataset\n')
    
    query_string = parse_csv_query(dataset, directory)

    t0 = time.time()
    if chunk == True:
        df = con.execute(query_string).fetch_df_chunk(5)
    elif chunk == False:
        df = con.execute(query_string).df()
    t1 = time.time()
    total_time = round(t1 - t0, 2) 
    print(f'Dataframe (pandas) conversion time: {total_time} seconds\n')

    return df

def clean_pandas_df(df: pd.DataFrame) -> pd.DataFrame:
    """Clean up dataframe to prep for loading into warehouse"""
    return

def csv_to_pandas(dataset: str, db: str, chunk: bool = True) -> pd.DataFrame:
    with duckdb.connect(db) as con:
        """process csv files in pandas dataframe"""
        df = create_pandas_df(con, dataset, chunk)

        return df

def create_polars_df(con: duckdb.DuckDBPyConnection, dataset: str, directory: Path) -> pl.DataFrame:
    """Parse csv files into polars dataframe"""
    print(f'Creating polars dataframe for {dataset} dataset\n')

    query_string = parse_csv_query(dataset, directory)

    t0 = time.time()
    df = con.execute(query_string).pl()
    t1 = time.time()
    total_time = round(t1 - t0, 2) 
    print(f'Dataframe (polars) conversion time: {total_time} seconds\n')

    return df

def clean_polars_df(df: pl.DataFrame) -> pl.DataFrame:
    """Clean up dataframe to prep for loading into warehouse"""
    t0 = time.time()

    lf = df.lazy()
    lf = (
        lf
        .select(col.name for col in lf.null_count() / lf.height if col.item() <= 0.2) #select columns that have < 20% null values
        .filter(~pl.all_horizontal(pl.all().is_null())) #filter out any rows that contain all null values
        #.select(pl.all().is_nan().all().is_not()) #need to type cast first, all values varchar
    )
    print(lf.explain())
    df = lf.collect()

    t1 = time.time()
    total_time = round(t1 - t0, 2) 
    print(f'Polars cleanup time: {total_time} seconds\n')

    return df

def csv_to_polars(dataset: str, db: str) -> pl.DataFrame:
    with duckdb.connect(db) as con:
        """process csv files in polars dataframe"""
        df = create_polars_df(con, dataset)
        df = clean_polars_df(df)
        return df