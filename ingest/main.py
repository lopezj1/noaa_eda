import timeit
from pathlib import Path
from prefect import flow, task
from utils import noaa_utils, duckdb_utils, dataframe_utils

#Constants
TMP_DIR = Path(__file__).resolve().parent.parent / "tmp"
SCHEMA = 'raw'
DUCKDB_PATH = str(Path(__file__).resolve().parent.parent / "duckdb" / "noaa_dw.duckdb")
NOAA_URL = "https://www.st.nmfs.noaa.gov/st1/recreational/MRIP_Survey_Data/CSV/" 
DATASETS = ['catch', 'size', 'trip']
PROCESSING_METHODS = ['duckdb', 'polars', 'pandas']

@task()
def optimal_processing_method(dataset: str, directory: Path, duckdb_path: str, schema: str) -> str:
    """Find the optimal processing method to read csv file into an object specific to methods being evaluated"""
    setup_code = f"""from utils import noaa_utils, duckdb_utils, dataframe_utils
from pathlib import Path
import duckdb
dataset = "{dataset}"
directory = Path(r"{directory}")
duckdb_path = r"{duckdb_path}"
schema = "{schema}"
"""
    print(setup_code)
    stmt_pandas = "dataframe_utils.csv_to_pandas_df(dataset, directory)"
    time_pandas = round(timeit.timeit(stmt_pandas, setup=setup_code, number=1), 2)

    stmt_polars = "dataframe_utils.csv_to_polars_df(dataset, directory)"
    time_polars = round(timeit.timeit(stmt_polars, setup=setup_code, number=1), 2)
    
    stmt_duckdb = "duckdb_utils.csv_to_duckdb_relation(duckdb_path, dataset, directory, schema)"
    time_duckdb = round(timeit.timeit(stmt_duckdb, setup=setup_code, number=1), 2)

    optimal_time= min([time_pandas, time_polars, time_duckdb])

    if optimal_time == time_pandas:
        optimal_method = "pandas"
    elif optimal_time == time_polars:
        optimal_method = "polars"
    elif optimal_time == time_duckdb:
        optimal_method = "duckdb"

    print(f"Pandas processing time for {dataset} dataset was {time_pandas} seconds")
    print(f"Polars processing time for {dataset} dataset was {time_polars} seconds")
    print(f"Duckdb processing time for {dataset} dataset was {time_duckdb} seconds")
    print(f"Optimal processing method for {dataset} dataset is {optimal_method} at {optimal_time} seconds")
    return optimal_method

@flow(log_prints=True)
def create_dictionary_of_dataset_objects(duckdb_path: str, datasets: list, directory: Path, method: str, schema: str) -> dict:
    """create a dictionary of data objects for catch, size and trip datasets based on optimal processing method"""
    dict_dfs = {}
    for dataset in datasets:
        k = f'{dataset}_{method}'
        if method == "pandas":
            v = dataframe_utils.csv_to_pandas_df(dataset, directory)
        elif method == "polars":
            v = dataframe_utils.csv_to_polars_df(dataset, directory)
        elif method == "duckdb":
            v = duckdb_utils.csv_to_duckdb_relation(duckdb_path, dataset, directory, schema)

        dict_dfs[k] = v

    return dict_dfs

@flow(log_prints=True)
def create_table_for_dataset(duckdb_path: str, dict_dfs: dict, schema: str) -> None:
    """Create table in db for each dataset"""
    for k, v in dict_dfs.items():
        dataset = k.split('_')[0]
        query_string = duckdb_utils.create_table_query(dataset)
        duckdb_utils.execute_duckdb_query(duckdb_path, query_string, data_object = v)
        duckdb_utils.results_duckdb_query(duckdb_path, "SELECT * FROM INFORMATION_SCHEMA.tables")
        duckdb_utils.results_duckdb_query(duckdb_path, f"SELECT * FROM noaa_dw.{schema}.{dataset}")
        duckdb_utils.results_duckdb_query(duckdb_path, f"DESCRIBE noaa_dw.{schema}.{dataset}")

@flow(log_prints=True)
def ingest_noaa(url: str = NOAA_URL, directory: Path = TMP_DIR, start_year: int = 1981, end_year: int = 2013, processing_method: str = 'polars') -> None:
    """Extract and load csv files from noaa site into 
        3 tables: catch, size, trip into schema named RAW
        in duckdb database named noaa_dw"""
    
    #noaa_utils.extract_noaa_data(url, directory, start_year, end_year) #extract data from http zip folder
    '''
    # find optimal processing method based on timeit function, options: duckdb, polars, pandas
    # commented out -- polars consistently the fastest processing method
    optimal_methods = [optimal_processing_method(dataset, TMP_DIR, DUCKDB_PATH) for dataset in DATASETS]
    optimal_method = max(set(optimal_methods), key = optimal_methods.count) #get most optimal out of 3
    '''
    # drop schema 'raw' and all tables within it
    query_string = duckdb_utils.drop_schema_query(SCHEMA)
    duckdb_utils.execute_duckdb_query(DUCKDB_PATH, query_string)
    duckdb_utils.results_duckdb_query(DUCKDB_PATH, "SELECT * FROM INFORMATION_SCHEMA.tables")

    #generate objects based on processing_method passed to then Insert into duckdb table
    if processing_method in PROCESSING_METHODS:
        dict_dfs = create_dictionary_of_dataset_objects(DUCKDB_PATH, DATASETS, TMP_DIR, processing_method, SCHEMA)
        create_table_for_dataset(DUCKDB_PATH, dict_dfs, SCHEMA)

        #noaa_utils.delete_csv_files(TMP_DIR) #Delete tmp folder and contents to free up space on local machine
    else:
        print(f"Processing method, {processing_method}, is not in {PROCESSING_METHODS}")

if __name__ == "__main__":
    start_year = 1981
    end_year = 2013
    processing_method = 'polars'
    url = NOAA_URL
    directory = TMP_DIR
    ingest_noaa(url, directory, start_year, end_year, processing_method)

