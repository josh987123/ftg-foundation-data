import os
import pyodbc
import pandas as pd

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
SERVER = "sql.foundationsoft.com,9000"
DATABASE = "Cas_5587"   # must match what worked earlier
OUTFILE = "data/job_budgets.csv"

def connect():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={os.environ['FOUNDATION_SQL_USER']};"
        f"PWD={os.environ['FOUNDATION_SQL_PASSWORD']};",
        timeout=30
    )

def main():
    print("Exporting job_budgets.csv ...")

    sql = """
    SELECT
        j.job_no,
        j.description AS job_description,
        j.customer_no,
        c.name AS customer_name,
        j.job_status,
        j.project_manager_no,
        pm.description AS project_manager_name,

        -- Contract values
        j.original_contract,
        COALESCE(ch.tot_income_adj, 0) AS tot_income_adj,
        j.original_contract + COALESCE(ch.tot_income_adj, 0) AS revised_contract,

        -- Cost values
        j.original_cost,
        COALESCE(ch.tot_cost_adj, 0) AS tot_cost_adj,
        j.original_cost + COALESCE(ch.tot_cost_adj, 0) AS revised_cost

    FROM dbo.jobs j

    -- Aggregate job change orders FIRST
    LEFT JOIN (
        SELECT
            job_no,
            SUM(tot_income_adj) AS tot_income_adj,
            SUM(tot_cost_adj) AS tot_cost_adj
        FROM dbo.job_chg
        WHERE status = 'A'
        GROUP BY job_no
    ) ch
        ON j.job_no = ch.job_no

    LEFT JOIN dbo.project_managers pm
        ON j.project_manager_no = pm.project_manager_no

    LEFT JOIN dbo.customers c
        ON j.customer_no = c.customer_no

    ORDER BY j.job_no
    """

    conn = connect()
    df = pd.read_sql(sql, conn)

    # ------------------------------------------------------------
    # Type normalization (match Power Query behavior)
    # ------------------------------------------------------------
    TEXT_COLS = [
        "job_no",
        "job_description",
        "customer_no",
        "customer_name",
        "job_status",
        "project_manager_no",
        "project_manager_name",
    ]

    for col in TEXT_COLS:
        if col in df.columns:
            df[col] = (
                df[col]
                  .astype(str)
                  .str.replace(r"\.0$", "", regex=True)
                  .str.strip()
                  .replace({"nan": "", "None": ""})
            )

    NUMERIC_COLS = [
        "original_contract",
        "tot_income_adj",
        "revised_contract",
        "original_cost",
        "tot_cost_adj",
        "revised_cost",
    ]

    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    df.to_csv(OUTFILE, index=False)
    print(f"Wrote {OUTFILE} ({len(df)} rows, {len(df.columns)} columns)")

if __name__ == "__main__":
    main()
