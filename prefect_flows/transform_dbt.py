from pathlib import Path
from prefect.task_runners import SequentialTaskRunner
from prefect_dbt_flow import dbt_flow
from prefect_dbt_flow.dbt import DbtProfile, DbtProject

PROJECT_DIR=Path(__file__).resolve().parent.parent
print(f'Root Directory: str({PROJECT_DIR})')
transform_dbt = dbt_flow(
    project=DbtProject(
        name="transform_dbt",
        project_dir=PROJECT_DIR / "dbt_transforms",
        profiles_dir=PROJECT_DIR / ".dbt",
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