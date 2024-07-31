from pathlib import Path
from prefect.task_runners import SequentialTaskRunner
from prefect_dbt_flow import dbt_flow
from prefect_dbt_flow.dbt import DbtProfile, DbtProject

ROOT_DIR=Path().resolve().parent
transform_dbt = dbt_flow(
    project=DbtProject(
        name="transform_dbt",
        project_dir=ROOT_DIR / "app/dbt_transforms",
        profiles_dir=ROOT_DIR / "app/.dbt",
    ),
    profile=DbtProfile(
        target="dev",
    ),
    flow_kwargs={
        "task_runner": SequentialTaskRunner(),
    },
)

if __name__ == "__main__":
    transform_dbt()