from pathlib import Path
from prefect import flow
from ingest_utils import noaa_utils, dlt_utils#, main_utils
import time

#Constants
TMP_DIR = Path(__file__).resolve().parent.parent / "tmp"
SCHEMA = 'raw'
# DUCKDB_PATH = str(Path(__file__).resolve().parent.parent / "db" / "noaa_dw.duckdb")
NOAA_URL = "https://www.st.nmfs.noaa.gov/st1/recreational/MRIP_Survey_Data/CSV/" 
DATASETS = ['catch', 'size', 'trip']

@flow(log_prints=True)
def ingest_noaa(start_year: int = 1981, end_year: int = 2023) -> None:
    """Extract and load csv files from noaa site into 
        3 tables: catch, size, trip into schema named RAW
        in duckdb database named noaa_dw"""
    
    noaa_utils.extract_noaa_data(NOAA_URL, TMP_DIR, start_year, end_year) #extract data from http zip folder
    # main_utils.drop_db_objects(DUCKDB_PATH, SCHEMA) #drop db objects
    # dict_dfs = main_utils.create_dictionary_of_dataset_objects(DATASETS, TMP_DIR) #dictionary of polars dataframes
    # main_utils.create_tables_for_datasets(DUCKDB_PATH, dict_dfs, SCHEMA) #create duckdb table for each dataframe 
    
    for dataset in DATASETS:
        start_time = time.time()
        dlt_utils.load_csv_data_duckdb(dataset, TMP_DIR, SCHEMA)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Process csv and insert into db time for {dataset}:", execution_time, " seconds")

    noaa_utils.delete_csv_files(TMP_DIR) #Delete tmp folder and contents to free up space on local machine

if __name__ == "__main__":
    start_year = 1981
    end_year = 2023
    ingest_noaa(start_year, end_year)

