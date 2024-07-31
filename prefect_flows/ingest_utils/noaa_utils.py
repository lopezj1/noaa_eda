import re
from pathlib import Path
import sys
import requests
import pandas as pd
from io import BytesIO
from zipfile import ZipFile, is_zipfile
import shutil
from prefect import flow, task
import time

@task()
def fix_file_names(file_names: list) -> list:
    fixed_names = []
    pattern = r'preliminary'

    for file_name in file_names:
        match = re.search(pattern, file_name)
        if match:
            new_name = re.sub(r'_preliminary.*', '_preliminary_csv.zip', file_name)
            fixed_names.append(new_name)
        else:
            fixed_names.append(file_name)
            
    return fixed_names

@flow()
def fetch_noaa_zip_folders(base_url: str) -> list:
    """Get list of zip folders containing NOAA data"""
    try:
        html = requests.get(base_url).content 
    except requests.RequestException as e:
        print(f"Error fetching data from {base_url}: {e}")

    df_list = pd.read_html(html)
    df = df_list[-1]
    lszip = df[df.Name.str.startswith("ps_", na=False)]["Name"].tolist()
    lszip = fix_file_names(lszip)

    print(f'Zip Folders found: {lszip}')
    return lszip

@task()
def unzip_noaa_folders(lszip: list, base_url: str) -> dict:
    """Unzip folder and return list of filenames and list of file content"""
    lsfilename = []
    lsfile = []

    for z in lszip:
        try:
            folder_url = f"{base_url[:-1]}/{z}"
            r = requests.get(folder_url)
            zipbytes = BytesIO(r.content)
            if is_zipfile(zipbytes):
                with ZipFile(zipbytes, "r") as myzip:
                    for contentfilename in myzip.namelist():
                        contentfile = myzip.read(contentfilename)

                        lsfilename.append(contentfilename)
                        lsfile.append(contentfile)
        except Exception as e:
            print(f"Error processing ZIP file {folder_url}: {e}")

    dictfile = dict(zip(lsfilename, lsfile))
    print(f'Files found: {dictfile.keys()}')
    return dictfile

@task()
def save_csv_files(dictfile: dict, directory: Path) -> None:
    """Save csv files to tmp folder"""
    directory.mkdir(parents=True, exist_ok=True)

    start_time = time.time()
    for filename, file in dictfile.items():
        trunc_fn = filename.split(".")[0]
        output_file = directory / f'{trunc_fn}.csv'
            
        try:
            with open(output_file, "wb") as outfile:
                outfile.write(file)
        except Exception as e:
            print(f"Error writing CSV file {output_file}: {e}")
    end_time = time.time()
    execution_time = end_time - start_time
    print("Write CSVs to /tmp folder:", execution_time, " seconds")

@task()
def delete_csv_files(directory: Path) -> None:
    """Remove tmp folder"""
    try:
        shutil.rmtree(directory)
    except Exception as e:
        print(f"Error removing directory {directory}: {e}")

@task()
def get_dataset_size(dataset: str, directory: Path) -> None:
    """Calculate the total size of the files in GB"""
    dataset_size = 0

    for file in directory.glob(f"{dataset}*"):
        dataset_size += file.stat().st_size

    print(f"{dataset} files size: " + str(round(dataset_size / (1024**3), 2)) + " GB")

@flow(log_prints=True)
def extract_noaa_data(base_url: str, directory: Path, start_year: int, end_year: int) -> None:
    """Unzip folders from noaa site and save to files to local tmp folder"""
    print(f'Extracting and copying csv files from noaa website to local tmp folder')
    
    lszip = fetch_noaa_zip_folders(base_url)
    dictfile = unzip_noaa_folders(lszip, base_url)

    if len(str(start_year)) == 4 and len(str(end_year)) == 4:
        filenames_to_filter = []
        
        for filename in dictfile.keys():
            year = int(filename.split(".")[0].split("_")[1][:-1]) #get list of years
            
            if start_year <= year <= end_year:
                continue
            else:
                filenames_to_filter.append(filename)
        
        #filter out any filenames that don't meet year criteria
        print(f'Files filtered: {filenames_to_filter}')
        for filename in filenames_to_filter: 
            del dictfile[filename] 

        print(f'Files downloaded: {dictfile.keys()}')
        if bool(dictfile): #check that dictfile is not empty
            save_csv_files(dictfile, directory)     
            get_dataset_size
        else:
            print(f'No data available for start year {start_year} and end year {end_year}') #end year is previous calendar year
            sys.exit(1) #exit if years entered don't have data

    else:
        print(f'Start year {start_year} and/or end year {end_year} are not valid')
        sys.exit(1) #exit if years are not valid