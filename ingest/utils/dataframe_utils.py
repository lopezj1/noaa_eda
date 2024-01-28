import glob
import pandas as pd
import polars as pl
import duckdb
import time
from pathlib import Path

def get_csv_files_by_dataset(dataset: str, directory: Path) -> list:
    """Get list of csv files that describe a particular dataset: catch, size, trip"""
    csv_files = list(directory.glob(f"**/{dataset}_*.csv"))

    return csv_files

def create_pandas_df(dataset: str, directory: Path) -> pd.DataFrame:
    """Parse csv files into pandas dataframe"""
    print(f'Creating pandas dataframe for {dataset} dataset\n')
    
    t0 = time.time()
    csv_files = get_csv_files_by_dataset(dataset, directory)
    df_list = [pd.read_csv(file) for file in csv_files] #create list of dataframes - 1 for each file
    df = pd.concat(df_list, ignore_index=True) #concat all dataframes in list into 1 dataframe
    t1 = time.time()
    total_time = round(t1 - t0, 2) 
    print(f'Dataframe (pandas) creation time: {total_time} seconds\n')

    return df

def create_polars_df(dataset: str, directory: Path) -> pl.DataFrame:
    """Parse csv files into polars dataframe"""
    print(f'Creating polars dataframe for {dataset} dataset\n')

    t0 = time.time()
    csv_files = get_csv_files_by_dataset(dataset, directory)
    df_list = [pl.read_csv(file, ignore_errors=True) for file in csv_files] #create list of dataframes - 1 for each file
    df = pl.concat(df_list, how="diagonal_relaxed") #concat all dataframes in list into 1 dataframe
    t1 = time.time()
    total_time = round(t1 - t0, 2) 
    print(f'Dataframe (polars) creation time: {total_time} seconds\n')

    return df

def clean_pandas_df(df: pd.DataFrame) -> pd.DataFrame:
    """Clean up dataframe to prep for loading into warehouse"""
    return

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

def csv_to_pandas_df(dataset: str, db: str, chunk: bool = True) -> pd.DataFrame:
    with duckdb.connect(db) as con:
        """process csv files in pandas dataframe"""
        df = create_pandas_df(con, dataset, chunk)
        df = clean_pandas_df(df)

        return df

def csv_to_polars_df(dataset: str, db: str) -> pl.DataFrame:
    with duckdb.connect(db) as con:
        """process csv files in polars dataframe"""
        df = create_polars_df(con, dataset)
        df = clean_polars_df(df)

        return df