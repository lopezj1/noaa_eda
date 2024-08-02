# NOAA MRIP Data Project

## Table of Contents
- [NOAA MRIP Data Project](#noaa-mrip-data-project)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Approach](#approach)
  - [How to Run this Project](#how-to-run-this-project)
  - [Project Steps](#project-steps)
    - [Extract \& Load (EL)](#extract--load-el)
    - [Transform (T)](#transform-t)
      - [Models](#models)
      - [Analyses](#analyses)
      - [Seeds](#seeds)
      - [Macros](#macros)
      - [Tests](#tests)
    - [Orchestration](#orchestration)
    - [Web Application for Visualizations](#web-application-for-visualizations)
    - [Deploying Project](#deploying-project)
  - [Conclusion](#conclusion)
  - [Future Considerations](#future-considerations)
    - [Machine Learning](#machine-learning)
  - [References](#references)

## Introduction
NOAA Fisheries’ Marine Recreational Information Program (MRIP) conducts annual recreational saltwater fishing surveys at the national level to estimate total recreational catch.  This data is used to assess and maintain sustainable fish stocks.  Survey data is available from 1981 to 2023.

In this project, survey data will be extracted from an NOAA website and loaded into a data warehouse.  This data will then be transformed as needed to be ready for reporting & analytics.  A Streamlit app will be used to interact with the transformed data and generate insights.

## Approach
An end-to-end data product will be built consisting of extracting, loading, and transforming (ELT) of raw data to generating dynamic and interactive visualizations on a web application.  The high level data flow, with technologies used, can be seen below:

![Alt text](images/noaa_project_data_flow_diagram.jpg)
***update image to show prefect***

## How to Run this Project
1. Go to directory where repo will be cloned to
   - `cd <directory>`
2. Clone repo to directory
   - `git clone https://github.com/lopezj1/noaa_eda.git`
3. Switch to project directory
   - `cd noaa_eda`
4. Run docker compose to spin up container
   - `docker compose up -d`
5. Visit prefect dashboard at http://localhost:4200
   - *Wait 1-2 minutes for Prefect Agent to start and Deployments to be created.*
6. Quick run ingest flow from Deployments
   - *Default year range is from 2018-2023 to have quicker loading time ~ 5 minutes.*
7. Quick run dbt flow from Deployments
   - *Running all models will take about 5 minutes.*
8. Start streamlit app by opening new shell inside the container
   - `docker exec -it noaa-app bash`
   - `streamlit run app/app.py`
9.  Visit streamlit app at http://localhost:8501
10. DBT docs can be seen by opening new shell in container and running following commands:
    - Open new shell inside container
    - `docker exec -it noaa-app bash`
    - `cd dbt_transforms`
    - `dbt docs generate`
    - `dbt docs serve`
11. Visit dbt docs at http://localhost:8080

## Project Steps
### Extract & Load (EL)
Survey data is stored at https://www.st.nmfs.noaa.gov/st1/recreational/MRIP_Survey_Data/CSV/.  Data is stored as csv files inside zip folders cataloged by year and wave (if multiple survey were taken that year).  Python script **ingest_noaa.py** will handle the extract and load (EL) of the data.  The EL pipeline consists of the following general steps:

1. GET request to retrieve folders named by year and wave
2. Unzip folders to extract csv files
3. Copy csv files to /tmp folder in main project directory
4. Write pandas dataframes to tables (1-to-1 relationship) in a persistent **DuckDB** schema named **raw**
- To be memory efficient each individal csv file is processed with the help of the **DLT** python library.

### Transform (T)
After loading the source data into **DuckDB**, data models were created to transform the raw data to feature rich data in a separate schema named **analytics** using **dbt**.  Tests and documentation were also created for the dbt project.

#### Models
- Staging
- Intermediate
- Marts

#### Analyses

#### Seeds

#### Macros

#### Tests

See dbt docs for more in-depth details

### Orchestration
The ELT pipelines were orchestrated using **Prefect**.

### Web Application for Visualizations
A web app was built using **Streamlit** to allow for self serve analytics.  Some interesting observations from the data are the following:

- observation 1
- observation 2

### Deploying Project
This project can be run entirely in one container using **Docker**.  This allows for better reproducibility and easier deployment on GCP, where I am hosting the project at https.

## Conclusion
Summarize project and its importance in fishery health.

## Future Considerations
### Machine Learning
Future implementation could consist of bringing in tidal, weather, lunar data via APIs in conjunction with this survey data to create predictive models on catch success rate.

## References
[About NOAA MRIP](https://www.fisheries.noaa.gov/recreational-fishing-data/about-marine-recreational-information-program)

[NOAA MRIP Survey Data](https://www.st.nmfs.noaa.gov/st1/recreational/MRIP_Survey_Data/CSV/)
