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
import time


# %%
def fetch_folders(base_url: str) -> list:
    """Get list of zip folders containing NOAA data"""
    # add try/except block for requests
    html = requests.get(base_url).content
    df_list = pd.read_html(html)
    df = df_list[-1]
    lszip = df[df.Name.str.endswith(".zip", na=False)]["Name"].tolist()

    return lszip


def unzip_folder(lszip: list) -> dict:
    """Unzip folder and return list of filenames and list of file content"""
    # add try/except block for requests
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


def delete_csvs(directory: str) -> None:
    """Remove tmp folder"""

    shutil.rmtree(f"{directory}")


def extract_noaa(base_url: str) -> None:
    """Unzip folders from noaa site and save to files to local tmp folder"""

    lszip = fetch_folders(base_url)
    dictfile = unzip_folder(lszip)
    write_csv(dictfile)


def get_dataset_size(dataset: str) -> None:
    """Calculate the total size of the files in GB"""
    dataset_size = 0
    for file in os.listdir("../tmp/"):
        if file.startswith(f"{dataset}"):
            dataset_size += os.path.getsize(f"../tmp/{file}")

    print(f"{dataset} files size: " + str(round(dataset_size / (1024**3), 2)) + " GB")


def convert_query_results(
    relation: duckdb.DuckDBPyRelation, materialization: str
) -> pd.DataFrame | pl.DataFrame:
    """Convert relation to dataframe"""
    t0 = time.time()
    with duckdb.connect("../duckdb/noaa_dw.duckdb") as con:
        if materialization == "pandas":
            df = con.sql("SELECT * FROM relation").df()
        elif materialization == "polars":
            df = con.sql("SELECT * FROM relation").pl()
    t1 = time.time()
    total_time = t1 - t0
    print(f'Dataframe conversion for {materialization} was {total_time} seconds')
    return df


def get_query_results(
    query: str, materialization: str = "in-memory"
) -> pd.DataFrame | pl.DataFrame | None:
    """Run query and return a relation"""

    print("query: " + query)
    print("materialization: " + materialization)

    with duckdb.connect("../duckdb/noaa_dw.duckdb") as con:
        if materialization == "persist":
            con.sql(query) #execute query
        else:
            r = con.sql(query) #store query results in relation

        if materialization == "pandas" or materialization == "polars":
            df = convert_query_results(r, materialization)
            return df
        elif materialization == "in-memory":
            con.sql("SELECT * FROM r").show()


def create_table(dataset: str, materialization: str = "in-memory") -> None:
    """Creates table in raw schema for a particular dataset"""

    query = f"""
            CREATE SCHEMA IF NOT EXISTS raw;
            USE noaa_dw.raw;
            CREATE OR REPLACE TABLE {dataset} AS
            SELECT * FROM '{dataset}_df'; 
            """

    get_query_results(query, materialization)


def parse_csv(dataset: str, materialization: str = "in-memory") -> pd.DataFrame | pl.DataFrame | None:
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

    return get_query_results(query, materialization)


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
#     v = get_query_results(parse_csv_query(dataset), materialization)

#     dict_dfs[k] = v

# %% figure out how to dynamically name dataframes using dataset they reference
materialization = "polars"
catch_df = parse_csv("catch", materialization)
size_df = parse_csv("size", materialization)
trip_df = parse_csv("trip", materialization)

# %% Preprocess dataframes
# drop columns that contain all null values
# cast column types
# check for NaNs
# deduplicate
# make into function
# time it for processing
catch_df.columns  # get list of column names
lazy_catch = catch_df.lazy()
lazy_query = (
            lazy_catch
            .select(col.name for col in catch_df.null_count() / catch_df.height if col.item() <= 0.2)
            .filter(~pl.all_horizontal(pl.all().is_null()))
)
print(lazy_query.explain())

# drop columns % of null counts > 20% of total values
catch_df.select(
    col.name for col in catch_df.null_count() / catch_df.height if col.item() <= 0.2
)

# drop rows containing all nulls
catch_df.filter(~pl.all_horizontal(pl.all().is_null()))

# all values in dataframe set to varchar so this doesn't work, need to type cast first
# catch_df.select(pl.all().is_nan().all().is_not())

# %% Create table in db for each dataframe
materialization = "persist"
create_table("catch", materialization)
create_table("size", materialization)
create_table("trip", materialization)

# %% Check table info in db
get_query_results("SELECT * FROM INFORMATION_SCHEMA.tables")
get_query_results("SELECT * FROM noaa_dw.raw.catch")
get_query_results("DESCRIBE noaa_dw.raw.catch")

# %% Delete schema and all tables/views within
materialization = "persist"
get_query_results("DROP SCHEMA raw CASCADE", materialization)

# %% Delete tmp folder and contents to free up space on local machine
delete_csvs("../tmp/")
