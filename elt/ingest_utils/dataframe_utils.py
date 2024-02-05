import pandas as pd
import polars as pl
import numpy as np
from pathlib import Path
from prefect import flow, task

@task()
def get_csv_files_by_dataset(dataset: str, directory: Path) -> list:
    """Get list of csv files that describe a particular dataset: catch, size, trip"""
    csv_files = list(directory.glob(f"**/{dataset}_*.csv"))

    return csv_files

@flow(log_prints=True)
def create_pandas_df(dataset: str, directory: Path) -> pd.DataFrame:
    """Parse csv files into pandas dataframe"""
    print(f'\nCreating pandas dataframe for {dataset} dataset\n')
    
    csv_files = get_csv_files_by_dataset(dataset, directory)
    df_list = [pd.read_csv(file, low_memory=False, dtype='object') for file in csv_files] #create list of dataframes - 1 for each file
    df = pd.concat(df_list, ignore_index=True) #concat all dataframes in list into 1 dataframe
    print(f'\nData Object Details for {dataset}_pandas_df\n')
    print(f'\nShape before cleaning: {df.shape}\n')
    print(df.dtypes)

    return df

@flow(log_prints=True)
def create_polars_df(dataset: str, directory: Path) -> pl.DataFrame:
    """Parse csv files into polars dataframe"""
    print(f'\nCreating polars dataframe for {dataset} dataset\n')

    csv_files = get_csv_files_by_dataset(dataset, directory)
    df_list = [pl.read_csv(file, ignore_errors=True) for file in csv_files] #create list of dataframes - 1 for each file
    df = pl.concat(df_list, how="diagonal_relaxed") #concat all dataframes in list into 1 dataframe
    print(f'\nData Object Details for {dataset}_polars_df\n')
    print(f'\nShape before cleaning: {df.shape}\n')
    print(df.dtypes)

    return df

@task()
def clean_pandas_df(df: pd.DataFrame) -> pd.DataFrame:
    """Clean up pandas dataframe to prep for loading into warehouse"""
    threshold = df.shape[0] * 0.99 #set NaN value threshold to 99% of values
    pattern = r'all|area' #remove these erroneous values
    df = df.fillna(value=np.nan).replace(to_replace=pattern,value=np.nan,regex=True).dropna(axis=1,thresh=threshold).dropna(axis=0,how="all")
    print(f'\nShape after cleaning: {df.shape}\n')
    print(df.dtypes)

    return df

@task()
def clean_polars_df(df: pl.DataFrame) -> pl.DataFrame:
    """Clean up polars dataframe to prep for loading into warehouse"""
    lf = df.lazy()
    lf_query = (
        lf
        .select(col.name for col in df.null_count() / df.height if col.item() <= 0.01) #select columns that have < 1% null values
        .filter(~pl.all_horizontal(pl.all().is_null())) #filter out any rows that contain all null values
        # .select(pl.all().is_nan().all().is_not()) #won't work for columns of dtype = string
    )
    print(f'\nlazy execution plan:\n{lf_query.explain()}')
    df = lf_query.collect()
    print(f'\nShape after cleaning: {df.shape}\n')
    print(df.dtypes)

    return df

@flow(log_prints=True)
def csv_to_pandas_df(dataset: str, directory: Path) -> pd.DataFrame:
    """process csv files in pandas dataframe"""
    df = create_pandas_df(dataset, directory)
    df = clean_pandas_df(df)

    return df

@flow(log_prints=True)
def csv_to_polars_df(dataset: str, directory: Path) -> pl.DataFrame:
    """process csv files in polars dataframe"""
    df = create_polars_df(dataset, directory)
    df = clean_polars_df(df)

    return df