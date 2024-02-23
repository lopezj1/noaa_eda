import polars as pl
from pathlib import Path
from prefect import flow, task

@task()
def get_csv_files_by_dataset(dataset: str, directory: Path) -> list:
    """Get list of csv files that describe a particular dataset: catch, size, trip"""
    csv_files = list(directory.glob(f"**/{dataset}_*.csv"))

    return csv_files

@flow(log_prints=True)
def create_polars_df(dataset: str, directory: Path) -> pl.DataFrame:
    """Parse csv files into polars dataframe"""
    print(f'\nCreating polars dataframe for {dataset} dataset\n')

    csv_files = get_csv_files_by_dataset(dataset, directory)
    df_list = [pl.read_csv(file, infer_schema_length=0) for file in csv_files] #create list of dataframes - 1 for each file, all columns will be pl.String
    df = pl.concat(df_list, how="diagonal_relaxed") #concat all dataframes in list into 1 dataframe
    print(f'\nData Object Details for {dataset}_polars_df\n')
    print(f'\nShape before cleaning: {df.shape}\n')
    print(df.dtypes)
    print(df.columns)
    print(f'First row of data: \n {df.head(1)}')
    print(f'Last row of data: \n {df.tail(1)}')

    return df

@task()
def clean_polars_df(df: pl.DataFrame, null_threshold: float = 0.25) -> pl.DataFrame:
    """Clean up polars dataframe to prep for loading into warehouse"""
    lf = df.lazy()
    # regex_pattern = r'^[0-9]+$'
    # regex_pattern = r'^[0-9]{16}$' #returns true if matches strings that consist entirely of exactly 16 digits
    regex_pattern = r'[0-9]{16}' #returns true if matches any sequence of 16 digits within a string
    lf_query = (
        lf
        # .select(col.name for col in df.null_count() / df.height if col.item() <= null_threshold) 
        # .filter(~pl.all_horizontal(pl.all().is_null())) #filter out any rows that contain all null values
        # # .filter(pl.col('ID_CODE').str.len_chars() == 16)#only select records where id_code is 16 characters long
        # .filter(pl.col('ID_CODE').str.contains(regex_pattern))#only select records where id_code contains 16 subsequent numeric values
        # .with_columns(pl.col('ID_CODE').str.replace(r'[^0-9]', '')) #remove any non digit characters from string
        # # .unique(subset='ID_CODE',keep='none') #only select records where id_code is unique 
        # # .unique(subset='ID_CODE',keep='any') #only select records where id_code is unique 
        
    )
    print(f'\nlazy execution plan:\n{lf_query.explain()}')
    df = lf_query.collect()
    print(f'\nShape after cleaning: {df.shape}\n')
    print(df.dtypes)
    print(df.columns)
    print(f'First row of data: \n {df.head(1)}')
    print(f'Last row of data: \n {df.tail(1)}')

    return df

@flow(log_prints=True)
def csv_to_polars_df(dataset: str, directory: Path) -> pl.DataFrame:
    """process csv files in polars dataframe"""
    df = create_polars_df(dataset, directory)
    df = clean_polars_df(df)

    return df