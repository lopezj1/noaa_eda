import datetime
from pathlib import Path
import requests
import pandas as pd
from io import BytesIO
from zipfile import ZipFile, is_zipfile
import shutil
from prefect import flow, task

@task()
def fetch_noaa_zip_folders(base_url: str) -> list:
    """Get list of zip folders containing NOAA data"""
    try:
        html = requests.get(base_url).content 
    except requests.RequestException as e:
        print(f"Error fetching data from {base_url}: {e}")

    df_list = pd.read_html(html)
    df = df_list[-1]
    lszip = df[df.Name.str.endswith(".zip", na=False)]["Name"].tolist()

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

    return dictfile

@task()
def save_csv_files(dictfile: dict, directory: Path) -> None:
    """Save csv files to tmp folder"""
    directory.mkdir(parents=True, exist_ok=True)

    for filename, file in dictfile.items():
        trunc_fn = filename.split(".")[0]
        output_file = directory / f'{trunc_fn}.csv'
        
        try:
            with open(output_file, "wb") as outfile:
                outfile.write(file)
        except Exception as e:
            print(f"Error writing CSV file {output_file}: {e}")

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

    # for file in os.listdir(f"{directory}/"):
    #     if file.startswith(f"{dataset}"):
    #         dataset_size += os.path.getsize(f"{directory}/{file}")

    print(f"{dataset} files size: " + str(round(dataset_size / (1024**3), 2)) + " GB")

@flow(log_prints=True)
def extract_noaa_data(base_url: str, directory: Path, start_year: int, end_year: int) -> None:
    """Unzip folders from noaa site and save to files to local tmp folder"""
    print(f'Extracting and copying csv files from noaa website to local tmp folder')
    
    lszip = fetch_noaa_zip_folders(base_url)
    dictfile = unzip_noaa_folders(lszip, base_url)

    if start_year in dictfile.keys() and end_year in dictfile.keys():
        save_csv_files(dictfile, directory)
        get_dataset_size
    else:
        print(f'Please enter a start year >= 1981 and end year <= {datetime.datetime.now().year}')