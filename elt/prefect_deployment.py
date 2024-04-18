from prefect.deployments import Deployment
from prefect.flows import Flow
from pathlib import Path
import ingest_noaa

path = Path(__file__).resolve().parent
def flow_deploy(flow: Flow, name: str, parameters: dict, work_queue_name: str = "default") -> Deployment:
    if flow == ingest_noaa.ingest_noaa:
        entrypoint = path / 'ingest_noaa.py:ingest_noaa'

    d = Deployment.build_from_flow(
        flow=flow, 
        name=name,
        parameters=parameters,
        path=str(path),
        entrypoint=str(entrypoint),
        work_queue_name=work_queue_name
        )
    return  d

DEPLOYMENTS = []
DEPLOYMENTS.append(flow_deploy(flow=ingest_noaa.ingest_noaa,name='ingest_noaa_data',parameters={'start_year':1981,'end_year':2023}))

if __name__ == '__main__':
    for deployment in DEPLOYMENTS:
        deployment.apply()