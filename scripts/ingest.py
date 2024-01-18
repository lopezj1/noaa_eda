# %% To-Do
# inspect tables
# column names
# populate README

# %%
import duckdb
import requests
from pathlib import Path
import pandas as pd
import polars as pl
import polars.selectors as cs
from io import BytesIO
from zipfile import ZipFile, is_zipfile
import os, shutil

# %% remove shape from value
pl.Config.set_tbl_hide_dataframe_shape(True)


# %%
def fetch_folders(base_url: str) -> list:
    """Get list of zip folders containing NOAA data"""

    html = requests.get(base_url).content
    df_list = pd.read_html(html)
    df = df_list[-1]
    lszip = df[df.Name.str.endswith(".zip", na=False)]["Name"].tolist()

    return lszip


def unzip_folder(lszip: list) -> dict:
    """Unzip folder and return list of filenames and list of file content"""

    lsfilename = []
    lsfile = []

    for z in lszip:
        folder_url = f"{base_url[:-1]}/{z}"
        r = requests.get(folder_url)
        zipbytes = BytesIO(r.content)
        if is_zipfile(zipbytes):
            with ZipFile(zipbytes, "r") as myzip:
                for contentfilename in myzip.namelist():
                    contentfile = myzip.read(contentfilename)

                    lsfilename.append(contentfilename)
                    lsfile.append(contentfile)

    dictfile = dict(zip(lsfilename, lsfile))

    return dictfile


def write_csv(dictfile: dict) -> None:
    """Save csv files to tmp folder"""
    tmp_dir = "../tmp"
    Path(tmp_dir).mkdir(parents=True, exist_ok=True)

    for filename, file in dictfile.items():
        folder = filename.split("_")[0]
        trunc_fn = filename.split(".")[0]
        output_file = f"../tmp/{trunc_fn}.csv"
        outfile = open(output_file, "wb")
        outfile.write(file)
        outfile.close()

    return


def delete_csvs(directory: str) -> None:
    """Remove tmp folder"""

    shutil.rmtree(f"{directory}")

    return


def extract_noaa(base_url: str) -> None:
    """Unzip folders from noaa site and save to files to local tmp folder"""

    lszip = fetch_folders(base_url)
    dictfile = unzip_folder(lszip)
    write_csv(dictfile)

    return


def get_dataset_size(dataset: str) -> None:
    """Calculate the total size of the files in GB"""
    dataset_size = 0
    for file in os.listdir("../tmp/"):
        if file.startswith(f"{dataset}"):
            dataset_size += os.path.getsize(f"../tmp/{file}")

    print(f"{dataset} files size: " + str(round(dataset_size / (1024**3), 2)) + " GB")

    return


def run_query(query: str, materialization):
    """Run query and materialize results"""
    with duckdb.connect("../duckdb/noaa_dw.duckdb") as con:
        if materialization == "in-memory":
            return con.sql(query).show()  # run in-memory
        elif materialization == "pandas":
            return con.sql(query).df()  # pandas dataframe
        elif materialization == "polars":
            return con.sql(query).pl()  # polars dataframe


def create_table(
    dataset: str, df: pl.DataFrame, materialization: str = "in-memory"
) -> None:
    """Creates table in raw schema for a particular dataset"""
    query = f"""
            CREATE SCHEMA IF NOT EXISTS raw;
            USE noaa_dw.raw;
            CREATE OR REPLACE TABLE {dataset} AS
            SELECT * FROM {df};
            """
    run_query(query, materialization)
    return


def parse_csv_query(dataset: str, materialization: str = "in-memory") -> None:
    """Print out the file size of all csv files with matching
    dataset and create query for reading that dataset"""

    get_dataset_size(f"{dataset}")
    query = f"""
            SELECT * FROM read_csv('../tmp/{dataset}_*.csv',
            normalize_names = true,
            ignore_errors = false,
            union_by_name = true,
            filename =  true,
            auto_detect = true,
            null_padding = true,
            all_varchar = true
            )
            """
    run_query(query, materialization)
    return


def get_table_columns(dataset: str, materialization: str = "in-memory") -> None:
    """Create query for getting column names for a particular table"""
    query = f"""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.columns 
            WHERE table_name = '{dataset}'
            """
    run_query(query, materialization)
    return


def inspect_df(df: pl.DataFrame) -> None:
    print(df.glimpse())
    print(df.describe())
    print(df.null_count())

    return


def drop_null_cols(df: pl.DataFrame) -> pl.DataFrame:
    dict_nulls = df.null_count().rows(named=True)[
        0
    ]  # get dictionary of column name as key and null counts as value
    print(dict_nulls)

    drop_cols = []  # instantiate list to hold columns to drop
    for k, v in dict_nulls.items():
        if v > 200:  # threshold of null values to drop on
            drop_cols.append(k)

    df = df.drop(drop_cols)

    return df


# %% make main function with init with url as materialization as parameter
# %% extract data from http zip folder
base_url = "https://www.st.nmfs.noaa.gov/st1/recreational/MRIP_Survey_Data/CSV/"
extract_noaa(base_url)

# %% read csv files into polars dataframe based on dataset type
# datasets = ['catch', 'size', 'trip']
# materialization = 'polars'
# dict_dfs = {}

# for dataset in datasets:
#     k = dataset
#     v = run_query(parse_csv_query(dataset), materialization)

#     dict_dfs[k] = v

# %%
materialization = "polars"
catch_df = run_query(parse_csv_query("catch"), materialization)
size_df = run_query(parse_csv_query("size"), materialization)
trip_df = run_query(parse_csv_query("trip"), materialization)

# %% Inspect dataframes
inspect_df(catch_df)
inspect_df(size_df)
inspect_df(trip_df)

# %% Drop columns containing high null count (using threshold of 200)
catch_df = drop_null_cols(
    catch_df
)  # might be able to eliminate this function see next line
catch_df = catch_df.filter(pl.all().is_not_null())
catch_df = catch_df.filter(pl.all().is_not_nan())

# %% Preprocess dataframes

# %% Create table in db for each dataframe
create_table("catch", catch_df)
create_table("size", size_df)
create_table("trip", trip_df)

# %% Check table info in db
run_query("SELECT * FROM INFORMATION_SCHEMA.tables")
run_query("DESCRIBE noaa_dw.raw.catch")

# %% Delete schema and all tables/views within
run_query("DROP SCHEMA raw CASCADE")

# %% Delete tmp folder and contents to free up space on local machine
delete_csvs("../tmp/")
