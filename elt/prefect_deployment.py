from prefect.deployments import Deployment
from prefect.flows import Flow
from pathlib import Path
import ingest_noaa, export_parquet
import traceback

path = Path(__file__).resolve().parent
def flow_deploy(flow: Flow, name: str, parameters: dict, work_queue_name: str = "default") -> Deployment:
    if flow == ingest_noaa.ingest_noaa:
        entrypoint = path / 'ingest_noaa.py:ingest_noaa'
    elif flow == export_parquet.export_parquet:
        entrypoint = path / 'export_parquet.py:export_parquet'

    d = Deployment.build_from_flow(
        flow=flow, 
        name=name,
        parameters=parameters,
        path=str(path),
        entrypoint=str(entrypoint),
        work_queue_name=work_queue_name,
        # job_variables={"env.PREFECT_API_URL": "http://host.docker.internal:4200/api"}
        # job_variables=dict("env.PREFECT_API_URL"="http://host.docker.internal:4200/api")
        )
    return  d

try:
    DEPLOYMENTS = []
    DEPLOYMENTS.append(flow_deploy(flow=ingest_noaa.ingest_noaa,name='ingest_noaa_data',parameters={'start_year':1981,'end_year':2023}))
    DEPLOYMENTS.append(flow_deploy(flow=export_parquet.export_parquet,name='export_noaa_parquet',parameters={'table':'analytics.trip_details'}))

    if __name__ == '__main__':
        for deployment in DEPLOYMENTS:
            deployment.apply()

except Exception as e:
    print(f"An error occurred: {e}")
    traceback.print_exc()