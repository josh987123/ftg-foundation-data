import json
import os
from datetime import datetime, timezone
from typing import List, Optional

# ==========================================================
# CONFIG
# ==========================================================

OUTPUT_DIR = "public/data"

EXCLUDED_PM = 'josh angelo'

EXCLUDED_AP_VENDORS = [
    'FTG Builders LLC',
    'FTG Builders, LLC',
    'FTG Builders',
    'FTG BUILDERS LLC',
    'CoPower One',
    'Travel costs',
    'Meals and Entertainment',
    'DoorDash Food Delivery',
    'Costco Wholesale',
    'Gas/other vehicle expense'
]

# Fixed aging date per Foundation AR Aging rules
AR_AGING_DATE = datetime(2026, 1, 7)

# ==========================================================
# UTILITIES
# ==========================================================

def excel_to_date(serial):
    from datetime import timedelta

    if not serial:
        return None

    if isinstance(serial, str) and '-' in serial:
        try:
            return datetime.strptime(serial.strip(), '%Y-%m-%d')
        except ValueError:
            pass

    excel_epoch = datetime(1899, 12, 30)
    try:
        return excel_epoch + timedelta(days=int(float(serial)))
    except (ValueError, TypeError):
        return None

# ==========================================================
# JOB METRICS (UNCHANGED)
# ==========================================================

def calculate_job_metrics(job: dict, actual_cost: float, billed: float) -> dict:
    job_no = str(job.get('job_no', ''))
    budget_cost = float(job.get('revised_cost') or 0)
    contract = float(job.get('revised_contract') or 0)
    job_status = job.get('job_status', '')

    is_closed = job_status == 'C'

    percent_complete = min((actual_cost / budget_cost) * 100, 100) if budget_cost else 0
    earned_revenue = (actual_cost / budget_cost) * contract if budget_cost else 0

    backlog = 0 if is_closed else (contract - earned_revenue)
    profit = billed - actual_cost
    margin = (profit / billed * 100) if billed else 0

    return {
        'job_no': job_no,
        'job_description': job.get('job_description', ''),
        'project_manager': job.get('project_manager_name', ''),
        'customer_name': job.get('customer_name', ''),
        'job_status': job_status,
        'contract': contract,
        'budget_cost': budget_cost,
        'actual_cost': actual_cost,
        'billed': billed,
        'percent_complete': round(percent_complete, 2),
        'earned_revenue': round(earned_revenue, 2),
        'backlog': round(backlog, 2),
        'profit': round(profit, 2),
        'margin': round(margin, 2)
    }

# ==========================================================
# AR METRICS (OPTION B FINAL LOGIC)
# ==========================================================

def calculate_ar_invoice_metrics(invoice: dict) -> Optional[dict]:
    """
    OPTION B RULE:
    Audit-cleared retainage invoices DO NOT EXIST AT ALL.
    If there is no collectible AR and only retainage remains, drop the row.
    """

    calc_due = float(invoice.get('calculated_amount_due', 0) or 0)
    retainage = float(invoice.get('retainage_amount', 0) or 0)

    # 🔑 FINAL GUARD — prevents resurrection of audit-cleared invoices
    if calc_due == 0 and retainage > 0:
        return None

    invoice_date = excel_to_date(invoice.get('invoice_date'))
    days_outstanding = (
        max(0, (AR_AGING_DATE - invoice_date).days)
        if invoice_date else 0
    )

    if days_outstanding <= 30:
        aging_bucket = '0-30'
    elif days_outstanding <= 60:
        aging_bucket = '31-60'
    elif days_outstanding <= 90:
        aging_bucket = '61-90'
    else:
        aging_bucket = '90+'

    return {
        'invoice_no': invoice.get('invoice_no'),
        'customer_name': invoice.get('customer_name', '').strip(),
        'project_manager': invoice.get('project_manager_name', '').strip(),
        'job_no': invoice.get('job_no'),
        'job_description': invoice.get('job_description', ''),
        'invoice_date': invoice.get('invoice_date'),
        'invoice_amount': float(invoice.get('invoice_amount', 0) or 0),
        'collectible': round(calc_due, 2),
        'retainage': round(retainage, 2),
        'total_due': round(calc_due + retainage, 2),
        'days_outstanding': days_outstanding,
        'aging_bucket': aging_bucket
    }

# ==========================================================
# RUNNERS
# ==========================================================

def run_jobs_etl() -> List[dict]:
    with open("public/data/financials_jobs.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    budgets = data.get('job_budgets', [])
    actuals = data.get('job_actuals', [])
    billed = data.get('job_billed_revenue', [])

    actual_by_job = {}
    for a in actuals:
        actual_by_job[str(a['Job_No'])] = actual_by_job.get(str(a['Job_No']), 0) + float(a['Actual_Cost'])

    billed_by_job = {str(b['Job_No']): float(b['Billed_Revenue']) for b in billed}

    return [
        calculate_job_metrics(
            job,
            actual_by_job.get(str(job.get('job_no')), 0),
            billed_by_job.get(str(job.get('job_no')), 0)
        )
        for job in budgets
    ]

def run_ar_etl() -> List[dict]:
    with open("public/data/ar_invoices.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    results = []
    for inv in data.get('invoices', []):
        m = calculate_ar_invoice_metrics(inv)
        if m:
            results.append(m)
    return results

def run_ap_etl() -> List[dict]:
    with open("public/data/ap_invoices.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    results = []
    for inv in data.get('invoices', []):
        remaining = float(inv.get('remaining_balance', 0) or 0)
        vendor = inv.get('vendor_name', '').strip()
        if remaining <= 0:
            continue
        if vendor in EXCLUDED_AP_VENDORS:
            continue
        results.append(inv)
    return results

# ==========================================================
# WRITE OUTPUTS
# ==========================================================

def write_metrics_outputs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    jobs = run_jobs_etl()
    ar = run_ar_etl()
    ap = run_ap_etl()

    with open(f"{OUTPUT_DIR}/metrics_jobs.json", "w", encoding="utf-8") as f:
        json.dump(jobs, f, indent=2)

    with open(f"{OUTPUT_DIR}/metrics_ar.json", "w", encoding="utf-8") as f:
        json.dump(ar, f, indent=2)

    with open(f"{OUTPUT_DIR}/metrics_ap.json", "w", encoding="utf-8") as f:
        json.dump(ap, f, indent=2)

    with open(f"{OUTPUT_DIR}/metrics_generated_at.json", "w", encoding="utf-8") as f:
        json.dump(
            {"generated_at": datetime.now(timezone.utc).isoformat()},
            f,
            indent=2
        )

    print("[MetricsETL] Wrote metrics to public/data")

# ==========================================================
# ENTRY POINT
# ==========================================================

if __name__ == "__main__":
    write_metrics_outputs()
