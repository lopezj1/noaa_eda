from pathlib import Path
from prefect import flow
from ingest_utils import noaa_utils, main_utils

#Constants
TMP_DIR = Path(__file__).resolve().parent.parent / "tmp"
SCHEMA = 'raw'
DUCKDB_PATH = str(Path(__file__).resolve().parent.parent / "duckdb" / "noaa_dw.duckdb")
NOAA_URL = "https://www.st.nmfs.noaa.gov/st1/recreational/MRIP_Survey_Data/CSV/" 
DATASETS = ['catch', 'size', 'trip']
PROCESSING_METHODS = ['duckdb', 'polars', 'pandas']

@flow(log_prints=True)
def ingest_noaa(start_year: int = 1981, end_year: int = 2023, processing_method: str = 'polars') -> None:
    """Extract and load csv files from noaa site into 
        3 tables: catch, size, trip into schema named RAW
        in duckdb database named noaa_dw"""
    
    noaa_utils.extract_noaa_data(NOAA_URL, TMP_DIR, start_year, end_year) #extract data from http zip folder
    '''
    # find optimal processing method based on timeit function, options: duckdb, polars, pandas
    # commented out -- polars consistently the fastest processing method
    optimal_methods = [optimal_processing_method(dataset, TMP_DIR, DUCKDB_PATH) for dataset in DATASETS]
    optimal_method = max(set(optimal_methods), key = optimal_methods.count) #get most optimal out of 3
    '''
    main_utils.drop_schema(DUCKDB_PATH, SCHEMA) #drop schema

    #generate objects based on processing_method passed to then Insert into duckdb table
    if processing_method in PROCESSING_METHODS:
        dict_dfs = main_utils.create_dictionary_of_dataset_objects(DUCKDB_PATH, DATASETS, TMP_DIR, processing_method, SCHEMA)
        main_utils.create_table_for_dataset(DUCKDB_PATH, dict_dfs, SCHEMA)

        noaa_utils.delete_csv_files(TMP_DIR) #Delete tmp folder and contents to free up space on local machine
    else:
        print(f"Processing method, {processing_method}, is not in {PROCESSING_METHODS}")

if __name__ == "__main__":
    start_year = 1981
    end_year = 2023
    processing_method = 'polars'
    ingest_noaa(start_year, end_year, processing_method)

