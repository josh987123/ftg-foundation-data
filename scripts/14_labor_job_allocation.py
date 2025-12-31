import os
import pyodbc
import pandas as pd

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
SERVER = "sql.foundationsoft.com,9000"
DATABASE = "Cas_5587"
OUTFILE = "data/labor_job_allocation.csv"

# ------------------------------------------------------------
# Connection
# ------------------------------------------------------------
def connect():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={os.environ['FOUNDATION_SQL_USER']};"
        f"PWD={os.environ['FOUNDATION_SQL_PASSWORD']};",
        timeout=30
    )

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def normalize_text(series):
    return (
        series.astype(str)
              .str.replace(r"\.0$", "", regex=True)
              .str.strip()
              .replace({"nan": "", "None": ""})
    )

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    print("Exporting labor_job_allocation.csv ...")
    conn = connect()

    sql = """
    WITH employees AS (
        SELECT
            employee_no,
            last_name + ', ' + first_name AS employee_name,
            hourly_or_salary,
            pay_rate,
            CASE
                WHEN LEFT(hourly_or_salary, 1) = 'H' THEN 'Hourly'
                ELSE 'Salaried'
            END AS pay_type,
            CASE
                WHEN LEFT(hourly_or_salary, 1) = 'H' THEN 'True Rate'
                ELSE 'Allocated Rate'
            END AS labor_rate_type,
            CASE
                WHEN LEFT(hourly_or_salary, 1) = 'H' THEN pay_rate
                ELSE pay_rate / 40.0
            END AS base_effective_hourly_rate
        FROM dbo.v_hr_employees
    ),

    earn_types AS (
        SELECT
            earn_type_no,
            LOWER(description) AS description_lc
        FROM dbo.earn_types
    ),

    timecards AS (
        SELECT
            employee_no,
            job_no,
            cost_code_no,
            cost_class_no,
            earn_type_no,
            DATEADD(day, -DATEPART(weekday, dated) + 2, CAST(dated AS date)) AS week_start,
            SUM(hours) AS employee_hours,
            MAX(src) AS approval_status
        FROM (
            SELECT
                employee_no,
                job_no,
                cost_code_no,
                cost_class_no,
                earn_type_no,
                dated,
                hours,
                'Approved' AS src
            FROM dbo.v_hr_pay_check_timecards

            UNION ALL

            SELECT
                employee_no,
                job_no,
                cost_code_no,
                NULL AS cost_class_no,
                earn_type_no,
                dated,
                hours,
                'Pending' AS src
            FROM dbo.pending_timecards
        ) t
        GROUP BY
            employee_no,
            job_no,
            cost_code_no,
            cost_class_no,
            earn_type_no,
            DATEADD(day, -DATEPART(weekday, dated) + 2, CAST(dated AS date))
    ),

    job_history AS (
        SELECT
            job_no,
            cost_code_no,
            cost_class_no,
            DATEADD(day, -DATEPART(weekday, date_posted) + 2, CAST(date_posted AS date)) AS week_start,
            SUM(cost) AS job_labor_cost_posted
        FROM dbo.job_history
        GROUP BY
            job_no,
            cost_code_no,
            cost_class_no,
            DATEADD(day, -DATEPART(weekday, date_posted) + 2, CAST(date_posted AS date))
    ),

    joined AS (
        SELECT
            t.employee_no,
            e.employee_name,
            e.pay_type,
            e.labor_rate_type,
            e.base_effective_hourly_rate,

            t.job_no,
            t.cost_code_no,
            t.cost_class_no,
            t.week_start,
            t.employee_hours,
            t.approval_status,

            et.description_lc,
            COALESCE(j.job_labor_cost_posted, 0) AS job_labor_cost_posted
        FROM timecards t
        LEFT JOIN employees e
            ON e.employee_no = t.employee_no
        LEFT JOIN earn_types et
            ON et.earn_type_no = t.earn_type_no
        LEFT JOIN job_history j
            ON j.job_no = t.job_no
           AND j.cost_code_no = t.cost_code_no
           AND j.cost_class_no = t.cost_class_no
           AND j.week_start = t.week_start
    ),

    classified AS (
        SELECT *,
            CASE
                WHEN description_lc LIKE '%bonus%' OR description_lc LIKE '%ppp%' THEN 'Bonus'
                WHEN description_lc LIKE '%allowance%'
                  OR description_lc LIKE '%reimburse%'
                  OR description_lc LIKE '%fringe%'
                  OR description_lc LIKE '%per diem%'
                  OR description_lc LIKE '%overhead%'
                  OR description_lc LIKE '%insurance%'
                  OR description_lc LIKE '%auto%'
                  OR description_lc LIKE '%truck%'
                  OR description_lc LIKE '%pda%'
                  OR description_lc LIKE '%retention%' THEN 'Allowance'
                WHEN description_lc LIKE '%double%' THEN 'DoubleTime'
                WHEN description_lc LIKE '%overtime%' THEN 'Overtime'
                WHEN description_lc LIKE '%holiday%'
                  OR description_lc LIKE '%pto%'
                  OR description_lc LIKE '%vacation%'
                  OR description_lc LIKE '%sick%'
                  OR description_lc LIKE '%covid%' THEN 'PaidTimeOff'
                ELSE 'Regular'
            END AS hour_type_group
        FROM joined
    ),

    rates AS (
        SELECT *,
            CASE
                WHEN hour_type_group = 'Overtime' THEN 1.5
                WHEN hour_type_group = 'DoubleTime' THEN 2.0
                WHEN hour_type_group IN ('Bonus','Allowance') THEN NULL
                ELSE 1.0
            END AS rate_multiplier
        FROM classified
    ),

    final_calc AS (
        SELECT
            employee_no,
            employee_name,
            pay_type,
            labor_rate_type,

            job_no,
            cost_code_no,
            cost_class_no,
            week_start,
            hour_type_group,

            SUM(employee_hours) AS total_hours,

            SUM(
                CASE
                    WHEN rate_multiplier IS NULL THEN 0
                    ELSE employee_hours * base_effective_hourly_rate * rate_multiplier
                END
            ) AS labor_cost_estimated,

            SUM(job_labor_cost_posted) AS job_labor_cost_posted
        FROM rates
        GROUP BY
            employee_no,
            employee_name,
            pay_type,
            labor_rate_type,
            job_no,
            cost_code_no,
            cost_class_no,
            week_start,
            hour_type_group
    )

    SELECT *
    FROM final_calc
    ORDER BY
        week_start DESC,
        employee_no,
        job_no,
        cost_code_no
    """

    df = pd.read_sql(sql, conn)

    # ------------------------------------------------------------
    # Normalization
    # ------------------------------------------------------------
    TEXT_COLS = [
        "employee_no",
        "employee_name",
        "pay_type",
        "labor_rate_type",
        "job_no",
        "cost_code_no",
        "hour_type_group",
    ]

    for col in TEXT_COLS:
        if col in df.columns:
            df[col] = normalize_text(df[col])

    MONEY_COLS = [
        "labor_cost_estimated",
        "job_labor_cost_posted",
    ]

    for col in MONEY_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    df["week_start"] = pd.to_datetime(df["week_start"], errors="coerce")

    df.to_csv(OUTFILE, index=False)
    print(f"Wrote {OUTFILE} ({len(df)} rows, {len(df.columns)} columns)")

# ------------------------------------------------------------
if __name__ == "__main__":
    main()
