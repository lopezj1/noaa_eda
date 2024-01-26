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

def unzip_folders(lszip: list) -> dict:
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

def write_csvs(dictfile: dict) -> None:
    """Save csv files to tmp folder"""
    tmp_dir = "../tmp"
    Path(tmp_dir).mkdir(parents=True, exist_ok=True)

    for filename, file in dictfile.items():
        # folder = filename.split("_")[0]
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
    print(f'Extracting and copying csv files from noaa website to local tmp folder')
    
    lszip = fetch_folders(base_url)
    dictfile = unzip_folders(lszip)
    write_csvs(dictfile)

def get_dataset_size(dataset: str) -> None:
    """Calculate the total size of the files in GB"""
    dataset_size = 0
    for file in os.listdir("../tmp/"):
        if file.startswith(f"{dataset}"):
            dataset_size += os.path.getsize(f"../tmp/{file}")

    print(f"{dataset} files size: " + str(round(dataset_size / (1024**3), 2)) + " GB")

def create_table_db(dataset: str, df_type: str) -> None:
    """Creates table in raw schema for a particular dataset"""

    query_string = f"""
            CREATE SCHEMA IF NOT EXISTS raw;
            USE noaa_dw.raw;
            CREATE OR REPLACE TABLE {dataset} AS
            SELECT * FROM '{dataset}_{df_type}_df'; 
            """

    duckdb_execute(con, query_string)

def parse_csv(dataset: str) -> str:
    """Print out the file size of all csv files with matching
    dataset and create query for reading that dataset"""

    get_dataset_size(f"{dataset}")
    query_string = f"""
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

    return query_string

def duckdb_execute(con: duckdb.DuckDBPyConnection, query_string: str) -> None:
    print(f'Executing duckdb query: \n{query_string}\n')

    """Execute query in duckdb"""
    t0 = time.time()
    con.execute(query_string) 
    t1 = time.time()
    total_time = round(t1 - t0, 2) 
    print(f'DuckDB query execution time: {total_time} seconds\n')

def duckdb_relation(con: duckdb.DuckDBPyConnection, dataset: str) -> duckdb.DuckDBPyRelation:
    """Create duckdb relation"""
    print(f'Creating duckdb relation for {dataset} dataset\n')

    query_string = parse_csv(dataset)

    t0 = time.time()
    relation = con.sql(query_string)
    t1 = time.time()
    total_time = round(t1 - t0, 2) 
    print(f'DuckDB relation build time: {total_time} seconds\n')    
    
    return relation

def create_pandas_df(con: duckdb.DuckDBPyConnection, dataset: str, chunk: bool = True) -> pd.DataFrame:
    """Parse csv files into pandas dataframe"""
    print(f'Creating pandas dataframe for {dataset} dataset\n')
    
    query_string = parse_csv(dataset)

    t0 = time.time()
    if chunk == True:
        df = con.execute(query_string).fetch_df_chunk()
    elif chunk == False:
        df = con.execute(query_string).df()
    t1 = time.time()
    total_time = round(t1 - t0, 2) 
    print(f'Dataframe (pandas) conversion time: {total_time} seconds\n')

    return df

def create_polars_df(con: duckdb.DuckDBPyConnection, dataset: str) -> pl.DataFrame:
    """Parse csv files into polars dataframe"""
    print(f'Creating polars dataframe for {dataset} dataset\n')

    query_string = parse_csv(dataset)

    t0 = time.time()
    df = con.execute(query_string).pl()
    t1 = time.time()
    total_time = round(t1 - t0, 2) 
    print(f'Dataframe (polars) conversion time: {total_time} seconds\n')

    return df

# %% 
# def ingest_noaa():
# """Extract and load csv files from noaa site into 
#     3 tables: catch, size, trip into schema named RAW
#     in duckdb database named noaa_data"""
base_url = "https://www.st.nmfs.noaa.gov/st1/recreational/MRIP_Survey_Data/CSV/" 
extract_noaa(base_url) #extract data from http zip folder

#%%
with duckdb.connect("../duckdb/noaa_dw.duckdb") as con:
    #duckdb relation
    catch_relation = duckdb_relation(con, "catch")
    catch_relation = duckdb_relation(con, "size")
    catch_relation = duckdb_relation(con, "trip")

    #pandas dataframes
    catch_pandas_df = create_pandas_df(con, "catch")
    size_pandas_df = create_pandas_df(con, "size")
    trip_pandas_df = create_pandas_df(con, "trip")

    #polars dataframes
    catch_polars_df = create_polars_df(con, "catch")
    size_polars_df = create_polars_df(con, "size")
    trip_polars_df = create_polars_df(con, "trip")

# %% Preprocess dataframes - make into function
# make 3 functions for preprocessing: relation, pandas, polars
# drop columns % of null counts > 20% of total values
# cast column types
# check for NaNs
# deduplicate
# time it for processing
catch_polars_df.columns  # get list of column names
lazy_catch = catch_polars_df.lazy()
lazy_query = (
            lazy_catch
            .select(col.name for col in catch_polars_df.null_count() / catch_polars_df.height if col.item() <= 0.2)
            .filter(~pl.all_horizontal(pl.all().is_null()))
)
print(lazy_query.explain())

# all values in dataframe set to varchar so this doesn't work, need to type cast first
# catch_df.select(pl.all().is_nan().all().is_not())

# %% Create table in db for each dataframe
df_type = "polars"
create_table_db("catch", df_type)
create_table_db("size", df_type)
create_table_db("trip", df_type)

# %% Check table info in db
duckdb_execute("SELECT * FROM INFORMATION_SCHEMA.tables")
duckdb_execute("SELECT * FROM noaa_dw.raw.catch")
duckdb_execute("DESCRIBE noaa_dw.raw.catch")

# %% Delete schema and all tables/views within
# make this a function, but don't call it in this script
duckdb_execute("DROP SCHEMA raw CASCADE")

# %% Delete tmp folder and contents to free up space on local machine
delete_csvs("../tmp/")

# if __name__ == "__main__":
#     ingest_noaa()