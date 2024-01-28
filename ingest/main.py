# %%
import timeit
import duckdb
from pathlib import Path
from utils import noaa_utils, duckdb_utils, dataframe_utils

#%% Constants
TMP_DIR = Path(__file__).resolve().parent.parent / "tmp"
DUCKDB_PATH = str(Path(__file__).resolve().parent.parent / "duckdb" / "noaa_dw.duckdb")
NOAA_URL = "https://www.st.nmfs.noaa.gov/st1/recreational/MRIP_Survey_Data/CSV/" 
DATASETS = ['catch', 'size', 'trip']

# %% 
# def ingest_noaa():
# """Extract and load csv files from noaa site into 
#     3 tables: catch, size, trip into schema named RAW
#     in duckdb database named noaa_data"""
noaa_utils.extract_noaa_data(NOAA_URL, TMP_DIR) #extract data from http zip folder

#%% 
def optimal_processing_method(dataset: str, directory: Path, duckdb_path: str) -> str:
    setup_code = f"""from utils import noaa_utils, duckdb_utils, dataframe_utils
from pathlib import Path
import duckdb
dataset = "{dataset}"
directory = Path(r"{directory}")
duckdb_path = r"{duckdb_path}"
"""
    print(setup_code)
    stmt_pandas = "dataframe_utils.create_pandas_df(dataset, directory)"
    time_pandas = timeit.timeit(stmt_pandas, setup=setup_code, number=1)

    stmt_polars = "dataframe_utils.create_polars_df(dataset, directory)"
    time_polars = timeit.timeit(stmt_polars, setup=setup_code, number=1)
    
    stmt_duckdb = f"""with duckdb.connect(duckdb_path) as con:
        duckdb_utils.create_duckdb_relation(con, dataset, directory)
        """
    time_duckdb = timeit.timeit(stmt_duckdb, setup=setup_code, number=1)

    optimal_time= min([time_pandas, time_polars, time_duckdb])

    if optimal_time == time_pandas:
        optimal_method = "pandas"
    elif optimal_time == time_polars:
        optimal_method = "polars"
    elif optimal_time == time_duckdb:
        optimal_method = "duckdb"

    print(f"Optimal processing method is {optimal_method} at {optimal_time} seconds")
    return optimal_method

#%% 
method = optimal_processing_method(DATASETS[0], TMP_DIR, DUCKDB_PATH)
method = "polars" # for testing

# compare columns and types of all 3 processing methods
if method == "pandas":
    df = dataframe_utils.csv_to_pandas_df()
elif method == "polars":
    df = dataframe_utils.csv_to_polars_df()
elif method == "duckdb":
    relation = duckdb_utils.csv_to_duckdb_relation()

#%% Pandas
catch_pandas_df = dataframe_utils.create_pandas_df(DATASETS[0], TMP_DIR)
catch_pandas_df_columns = catch_pandas_df.columns.to_list()

#%% Polars
catch_polars_df = dataframe_utils.create_polars_df(DATASETS[0], TMP_DIR)
catch_polars_df_columns = catch_polars_df.columns

#%% DuckDB
with duckdb.connect(DUCKDB_PATH) as con:
    catch_duckdb_relation = duckdb_utils.create_duckdb_relation(con, DATASETS[0], TMP_DIR) 
    catch_duckdb_relation_shape = catch_duckdb_relation.shape  
    catch_duckdb_relation_columns = catch_duckdb_relation.columns #this has one less row and column count is off
    
# %% Create table in db for each polars dataframe
with duckdb.connect(DUCKDB_PATH) as con:
    for dataset in DATASETS:
        query_string = duckdb_utils.create_table_from_df_query(dataset)
        duckdb_utils.execute_duckdb_query(query_string)

        duckdb_utils.execute_duckdb_query(con, "SELECT * FROM INFORMATION_SCHEMA.tables")
        duckdb_utils.execute_duckdb_query(con, f"SELECT * FROM noaa_dw.raw.{dataset}")
        duckdb_utils.execute_duckdb_query(con, f"DESCRIBE noaa_dw.raw.{dataset}")

# %% Delete tmp folder and contents to free up space on local machine
noaa_utils.delete_csv_files(TMP_DIR)

#%%
# if __name__ == "__main__":
#     ingest_noaa()
# %%
