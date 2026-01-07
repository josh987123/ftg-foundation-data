import json
import os
import requests
from datetime import datetime, date, timezone
from typing import Dict, List, Any, Optional

# ==========================================================
# CONFIG
# ==========================================================

OUTPUT_DIR = "public/data"

GITHUB_DATA_BASE = os.getenv(
    "FTG_DATA_BASE_URL",
    "https://raw.githubusercontent.com/josh987123/ftg-foundation-data/main/public/data"
)

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
# DATA LOADING
# ==========================================================

def load_json_file(filename: str) -> dict:
    """
    Load a JSON file from GitHub (canonical source of truth).
    This MUST succeed for the pipeline to be valid.
    """
    url = f"{GITHUB_DATA_BASE}/{filename}"
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return resp.json()

# ==========================================================
# UTILITIES
# ==========================================================

def excel_to_date(serial):
    """Convert Excel serial number or YYYY-MM-DD string to datetime."""
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
# JOB METRICS (UNCHANGED LOGIC)
# ==========================================================

def calculate_job_metrics(job: dict, actual_cost: float, billed: float) -> dict:
    job_no = str(job.get('job_no', ''))
    budget_cost = float(job.get('revised_cost') or 0)
    contract = float(job.get('revised_contract') or 0)
    original_contract = float(job.get('original_contract') or 0)
    original_cost = float(job.get('original_cost') or 0)
    tot_income_adj = float(job.get('tot_income_adj') or 0)
    tot_cost_adj = float(job.get('tot_cost_adj') or 0)
    job_status = job.get('job_status', '')

    has_budget = budget_cost > 0
    is_closed = job_status == 'C'

    if has_budget:
        percent_complete = min((actual_cost / budget_cost) * 100, 100) if budget_cost > 0 else 0
        earned_revenue = (actual_cost / budget_cost) * contract if budget_cost > 0 else 0
    else:
        percent_complete = 0
        earned_revenue = 0

    backlog = 0 if is_closed else (contract - earned_revenue)
    over_under_billing = 0 if is_closed else (billed - earned_revenue)

    if is_closed:
        profit = billed - actual_cost
        margin = (profit / billed * 100) if billed > 0 else 0
        valid_for_profit = billed > 0 and actual_cost > 0
        profit_basis = 'actual'
    else:
        if earned_revenue == 0 and billed > 0:
            profit = billed - actual_cost
            margin = (profit / billed * 100) if billed > 0 else 0
            valid_for_profit = billed > 0 and actual_cost > 0
            profit_basis = 'actual_fallback'
        else:
            profit = contract - budget_cost
            margin = (profit / contract * 100) if contract > 0 else 0
            valid_for_profit = contract > 0 and budget_cost > 0
            profit_basis = 'projected'

    return {
        'job_no': job_no,
        'job_description': job.get('job_description', ''),
        'project_manager': job.get('project_manager_name', ''),
        'customer_name': job.get('customer_name', ''),
        'job_status': job_status,
        'original_contract': original_contract,
        'contract': contract,
        'budget_cost': budget_cost,
        'actual_cost': actual_cost,
        'billed': billed,
        'has_budget': has_budget,
        'percent_complete': round(percent_complete, 2),
        'earned_revenue': round(earned_revenue, 2),
        'backlog': round(backlog, 2),
        'profit': round(profit, 2),
        'margin': round(margin, 2),
        'valid_for_profit': valid_for_profit,
        'profit_basis': profit_basis,
        'over_under_billing': round(over_under_billing, 2)
    }

# ==========================================================
# AR METRICS (FOUNDATION FIX APPLIED)
# ==========================================================

def calculate_ar_invoice_metrics(invoice: dict) -> Optional[dict]:
    calc_due = float(invoice.get('calculated_amount_due', 0) or 0)
    retainage = float(invoice.get('retainage_amount', 0) or 0)

    invoice_amount = float(invoice.get('invoice_amount', 0) or 0)
    cash_applied = float(invoice.get('cash_applied', 0) or 0)

    # --------------------------------------------------
    # FOUNDATION FIX: implicit retainage clearance
    # --------------------------------------------------
    if cash_applied >= invoice_amount:
        retainage = 0.0

    if calc_due <= 0 and retainage <= 0:
        return None

    total_due = calc_due + retainage
    collectible = calc_due

    invoice_date = excel_to_date(invoice.get('invoice_date', ''))
    if invoice_date:
        days_outstanding = max(0, (AR_AGING_DATE - invoice_date).days)
    else:
        days_outstanding = int(float(invoice.get('days_outstanding', 0) or 0))

    if days_outstanding <= 30:
        aging_bucket = '0-30'
    elif days_outstanding <= 60:
        aging_bucket = '31-60'
    elif days_outstanding <= 90:
        aging_bucket = '61-90'
    else:
        aging_bucket = '90+'

    return {
        'invoice_no': invoice.get('invoice_no', ''),
        'customer_name': invoice.get('customer_name', '').strip(),
        'project_manager': invoice.get('project_manager_name', '').strip(),
        'job_no': invoice.get('job_no', ''),
        'job_description': invoice.get('job_description', ''),
        'invoice_date': invoice.get('invoice_date', ''),
        'invoice_amount': float(invoice.get('invoice_amount', 0) or 0),
        'collectible': round(collectible, 2),
        'retainage': round(retainage, 2),
        'total_due': round(total_due, 2),
        'days_outstanding': days_outstanding,
        'aging_bucket': aging_bucket
    }

# ==========================================================
# RUNNERS
# ==========================================================

def run_jobs_etl() -> List[dict]:
    data = load_json_file('financials_jobs.json')
    budgets = data.get('job_budgets', [])
    actuals = data.get('job_actuals', [])
    billed = data.get('job_billed_revenue', [])

    actual_by_job = {}
    for a in actuals:
        job_no = str(int(a['Job_No'])) if isinstance(a['Job_No'], float) else str(a['Job_No'])
        actual_by_job[job_no] = actual_by_job.get(job_no, 0) + float(a['Actual_Cost'])

    billed_by_job = {}
    for b in billed:
        job_no = str(int(b['Job_No'])) if isinstance(b['Job_No'], float) else str(b['Job_No'])
        billed_by_job[job_no] = float(b['Billed_Revenue'])

    results = []
    for job in budgets:
        job_no = str(job.get('job_no'))
        results.append(
            calculate_job_metrics(
                job,
                actual_by_job.get(job_no, 0),
                billed_by_job.get(job_no, 0)
            )
        )
    return results


def run_ar_etl() -> List[dict]:
    data = load_json_file('ar_invoices.json')
    results = []
    for inv in data.get('invoices', []):
        m = calculate_ar_invoice_metrics(inv)
        if m:
            results.append(m)
    return results


def run_ap_etl() -> List[dict]:
    data = load_json_file('ap_invoices.json')
    results = []
    for inv in data.get('invoices', []):
        remaining = float(inv.get('remaining_balance', 0) or 0)
        if remaining <= 0:
            continue
        vendor = inv.get('vendor_name', '').strip()
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

    with open(f"{OUTPUT_DIR}/metrics_jobs.json", "w") as f:
        json.dump(jobs, f, indent=2)

    with open(f"{OUTPUT_DIR}/metrics_ar.json", "w") as f:
        json.dump(ar, f, indent=2)

    with open(f"{OUTPUT_DIR}/metrics_ap.json", "w") as f:
        json.dump(ap, f, indent=2)

    with open(f"{OUTPUT_DIR}/metrics_generated_at.json", "w") as f:
        json.dump(
            {
                "generated_at": datetime.now(timezone.utc).isoformat()
            },
            f,
            indent=2
        )

    print(f"[MetricsETL] Wrote metrics to {OUTPUT_DIR}")

# ==========================================================
# ENTRY POINT
# ==========================================================

if __name__ == "__main__":
    write_metrics_outputs()
