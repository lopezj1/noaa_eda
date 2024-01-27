# %%
import duckdb
from pathlib import Path
from utils import noaa_utils
from utils import duckdb_utils

#%% directories
TMP_DIR = Path("../tmp")
DUCKDB_PATH = "../duckdb/noaa_dw.duckdb"
NOAA_URL = "https://www.st.nmfs.noaa.gov/st1/recreational/MRIP_Survey_Data/CSV/" 
DATASETS = ['catch', 'size', 'trip']

# %% 
# def ingest_noaa():
# """Extract and load csv files from noaa site into 
#     3 tables: catch, size, trip into schema named RAW
#     in duckdb database named noaa_data"""
noaa_utils.extract_noaa_data(NOAA_URL, TMP_DIR) #extract data from http zip folder

# %% Create table in db for each dataframe
with duckdb.connect(DUCKDB_PATH) as con:
    for dataset in DATASETS:
        query_string = duckdb_utils.create_table_query(dataset)
        duckdb_utils.execute_duckdb_query(query_string)

#%% Check table info in db
with duckdb.connect(DUCKDB_PATH) as con:
    duckdb_utils.execute_duckdb_query(con, "SELECT * FROM INFORMATION_SCHEMA.tables")
    duckdb_utils.execute_duckdb_query(con, "SELECT * FROM noaa_dw.raw.catch")
    duckdb_utils.execute_duckdb_query(con, "DESCRIBE noaa_dw.raw.catch")

# %% Delete tmp folder and contents to free up space on local machine
noaa_utils.delete_csv_files(TMP_DIR)

#%%
# if __name__ == "__main__":
#     ingest_noaa()
# %%
