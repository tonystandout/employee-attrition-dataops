
from prefect import flow, task
from datetime import datetime


@task
def check_data_pipeline():
    print("Employee Attrition DataOps pipeline executed successfully!")
    print("Execution time:", datetime.now().isoformat())


@flow(
    name="Employee Attrition DataOps Pipeline",
    log_prints=True
)
def employee_attrition_dataops():
    check_data_pipeline()


if __name__ == "__main__":
    employee_attrition_dataops()
