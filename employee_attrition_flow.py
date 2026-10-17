
from prefect import flow, task, get_run_logger
from datetime import datetime, timezone
import pandas as pd
from pathlib import Path


# Update this path to match the CSV filename in your GitHub repo.
DATA_PATH = Path(__file__).parent / "WA_Fn-UseC_-HR-Employee-Attrition.csv"

EXPECTED_COLUMNS = [
    "Age",
    "Attrition",
    "Department",
    "JobRole",
    "MonthlyIncome",
    "OverTime",
    "YearsAtCompany",
]


@task
def validate_employee_data():
    logger = get_run_logger()

    logger.info("Starting employee attrition data validation")
    logger.info("Execution time: %s", datetime.now(timezone.utc).isoformat())

    if not DATA_PATH.exists():
        raise FileNotFoundError(
            f"Dataset not found: {DATA_PATH}. "
            "Add the CSV to the GitHub repository root."
        )

    df = pd.read_csv(DATA_PATH)

    missing_columns = [
        col for col in EXPECTED_COLUMNS if col not in df.columns
    ]
    if missing_columns:
        raise ValueError(f"Missing expected columns: {missing_columns}")

    missing_cells = int(df.isna().sum().sum())
    duplicate_rows = int(df.duplicated().sum())
    invalid_targets = int((~df["Attrition"].isin(["Yes", "No"])).sum())

    logger.info("Dataset rows: %d", len(df))
    logger.info("Dataset columns: %d", len(df.columns))
    logger.info("Missing cells: %d", missing_cells)
    logger.info("Duplicate rows: %d", duplicate_rows)
    logger.info("Invalid Attrition values: %d", invalid_targets)
    logger.info("Attrition counts: %s", df["Attrition"].value_counts().to_dict())

    if len(df) == 0:
        raise ValueError("Dataset is empty")

    if invalid_targets:
        raise ValueError("Attrition contains values other than Yes or No")

    logger.info("Employee attrition data validation passed")
    return {
        "rows": len(df),
        "columns": len(df.columns),
        "missing_cells": missing_cells,
        "duplicate_rows": duplicate_rows,
        "invalid_targets": invalid_targets,
    }


@flow(
    name="Employee Attrition DataOps Pipeline",
    log_prints=True,
)
def employee_attrition_dataops():
    validate_employee_data()


if __name__ == "__main__":
    employee_attrition_dataops()
