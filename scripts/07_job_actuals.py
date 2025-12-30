import os
import pyodbc
import pandas as pd
from datetime import datetime

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
SERVER = "sql.foundationsoft.com,9000"
DATABASE = "Cas_5587"
OUTFILE = "data/job_actuals.csv"

def connect():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={os.environ['FOUNDATION_SQL_USER']};"
        f"PWD={os.environ['FOUNDATION_SQL_PASSWORD']};",
        timeout=30
    )

def normalize_text(series):
    return (
        series.astype(str)
              .str.replace(r"\.0$", "", regex=True)
              .str.strip()
              .replace({"nan": "", "None": ""})
    )

def main():
    print("Exporting job_actuals.csv ...")
    conn = connect()

    # ------------------------------------------------------------
    # 1. Job history
    # ------------------------------------------------------------
    job_history_sql = """
    SELECT
        job_no,
        cost_code_no,
        cost_class_no,
        cost,
        date_posted
    FROM dbo.job_history
    WHERE date_posted IS NOT NULL
    """
    job_hist = pd.read_sql(job_history_sql, conn)

    job_hist["job_no"] = normalize_text(job_hist["job_no"])
    job_hist["cost_code_no"] = normalize_text(job_hist["cost_code_no"])
    job_hist["cost_class_no"] = pd.to_numeric(job_hist["cost_class_no"], errors="coerce")
    job_hist["cost"] = pd.to_numeric(job_hist["cost"], errors="coerce").fillna(0.0)

    # Force pandas datetime (critical)
    job_hist["date_posted"] = pd.to_datetime(job_hist["date_posted"], errors="coerce")

    # ------------------------------------------------------------
    # 2. Cost classes
    # ------------------------------------------------------------
    cost_classes = pd.read_sql(
        "SELECT cost_class_no, description FROM dbo.cost_classes",
        conn
    )
    cost_classes["cost_class_no"] = pd.to_numeric(cost_classes["cost_class_no"], errors="coerce")
    cost_classes["Cost_Class"] = normalize_text(cost_classes["description"])
    cost_classes = cost_classes[["cost_class_no", "Cost_Class"]]

    # ------------------------------------------------------------
    # 3. Cost codes
    # ------------------------------------------------------------
    cost_codes = pd.read_sql(
        "SELECT cost_code_no, description FROM dbo.cost_codes",
        conn
    )
    cost_codes["cost_code_no"] = normalize_text(cost_codes["cost_code_no"])
    cost_codes["Cost_Code_Description"] = normalize_text(cost_codes["description"])
    cost_codes = cost_codes[["cost_code_no", "Cost_Code_Description"]]

    # ------------------------------------------------------------
    # 4. Jobs
    # ------------------------------------------------------------
    jobs = pd.read_sql(
        "SELECT job_no, description, project_manager_no FROM dbo.jobs",
        conn
    )
    jobs["job_no"] = normalize_text(jobs["job_no"])
    jobs["Job_Description"] = normalize_text(jobs["description"])
    jobs["Project_Manager_No"] = normalize_text(jobs["project_manager_no"])
    jobs = jobs[["job_no", "Job_Description", "Project_Manager_No"]]

    # ------------------------------------------------------------
    # 5. Project managers
    # ------------------------------------------------------------
    pms = pd.read_sql(
        "SELECT project_manager_no, description FROM dbo.project_managers",
        conn
    )
    pms["Project_Manager_No"] = normalize_text(pms["project_manager_no"])
    pms["Project_Manager"] = normalize_text(pms["description"])
    pms = pms[["Project_Manager_No", "Project_Manager"]]

    # ------------------------------------------------------------
    # 6–9. Merge all lookups
    # ------------------------------------------------------------
    df = job_hist.merge(jobs, how="left", on="job_no")
    df = df.merge(pms, how="left", on="Project_Manager_No")
    df = df.merge(cost_codes, how="left", on="cost_code_no")
    df = df.merge(cost_classes, how="left", on="cost_class_no")

    # ------------------------------------------------------------
    # 10. Group costs
    # ------------------------------------------------------------
    grouped = (
        df.groupby(
            [
                "job_no",
                "Job_Description",
                "Project_Manager",
                "cost_class_no",
                "Cost_Class",
                "cost_code_no",
                "Cost_Code_Description",
            ],
            as_index=False
        )
        .agg(Actual_Cost=("cost", "sum"))
    )

    # ------------------------------------------------------------
    # 11. Job cost dates (FIXED)
    # ------------------------------------------------------------
    job_dates = (
        job_hist.groupby("job_no", as_index=False)
        .agg(
            Oldest_Cost_Date=("date_posted", "min"),
            Most_Recent_Cost_Date=("date_posted", "max"),
        )
    )

    today = pd.Timestamp(datetime.now().date())

    job_dates["Days_Since_First_Cost"] = (
        today - job_dates["Oldest_Cost_Date"]
    ).dt.days

    job_dates["Days_Since_Last_Cost"] = (
        today - job_dates["Most_Recent_Cost_Date"]
    ).dt.days

    # ------------------------------------------------------------
    # 12. Merge dates
    # ------------------------------------------------------------
    final = grouped.merge(job_dates, how="left", on="job_no")

    # ------------------------------------------------------------
    # 13. Rename + sort
    # ------------------------------------------------------------
    final = final.rename(
        columns={
            "job_no": "Job_No",
            "cost_code_no": "Cost_Code_No",
            "cost_class_no": "Cost_Class_No",
        }
    )

    final = final.sort_values(
        by=[
            "Days_Since_Last_Cost",
            "Job_No",
            "Cost_Class_No",
            "Cost_Code_No",
        ],
        ascending=[False, True, True, True]
    )

    final["Actual_Cost"] = final["Actual_Cost"].round(2)

    final.to_csv(OUTFILE, index=False)
    print(f"Wrote {OUTFILE} ({len(final)} rows, {len(final.columns)} columns)")

if __name__ == "__main__":
    main()
