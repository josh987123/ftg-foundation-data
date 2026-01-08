import json
import os
import requests
from datetime import datetime, timezone
from typing import List, Optional, Dict

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

# AR aging date is calculated at runtime (when ETL runs)
# This ensures aging buckets are always current

# ==========================================================
# DATA LOADING
# ==========================================================

def load_json_file(filename: str) -> dict:
    url = f"{GITHUB_DATA_BASE}/{filename}"
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return resp.json()

# ==========================================================
# UTILITIES
# ==========================================================

def safe_float(v) -> float:
    try:
        if v is None:
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def excel_to_date(value):
    from datetime import timedelta

    if not value:
        return None

    if isinstance(value, str) and "-" in value:
        try:
            return datetime.strptime(value.strip(), "%Y-%m-%d")
        except ValueError:
            return None

    try:
        excel_epoch = datetime(1899, 12, 30)
        return excel_epoch + timedelta(days=int(float(value)))
    except Exception:
        return None


def build_receipt_totals_by_invoice(allocations: List[dict]) -> Dict[str, float]:
    """Sum applied_amount by invoice_no to detect fully-paid invoices."""
    totals = {}
    for alloc in allocations:
        inv_no = str(alloc.get("invoice_no", ""))
        if inv_no:
            totals[inv_no] = totals.get(inv_no, 0) + safe_float(alloc.get("applied_amount"))
    return totals

# ==========================================================
# JOB METRICS (UNCHANGED)
# ==========================================================

def calculate_job_metrics(job: dict, actual_cost: float, billed: float) -> dict:
    job_no = str(job.get("job_no", ""))

    budget_cost = safe_float(job.get("revised_cost"))
    contract = safe_float(job.get("revised_contract"))
    original_contract = safe_float(job.get("original_contract"))
    original_cost = safe_float(job.get("original_cost"))

    job_status = job.get("job_status", "")
    is_closed = job_status == "C"
    has_budget = budget_cost > 0

    percent_complete = (
        min((actual_cost / budget_cost) * 100, 100) if has_budget else 0
    )

    earned_revenue = (
        (actual_cost / budget_cost) * contract if has_budget else 0
    )

    backlog = 0 if is_closed else contract - earned_revenue
    over_under_billing = 0 if is_closed else billed - earned_revenue

    if is_closed:
        profit = billed - actual_cost
        margin = (profit / billed * 100) if billed > 0 else 0
        profit_basis = "actual"
        valid_for_profit = billed > 0 and actual_cost > 0
    else:
        if earned_revenue == 0 and billed > 0:
            profit = billed - actual_cost
            margin = (profit / billed * 100) if billed > 0 else 0
            profit_basis = "actual_fallback"
            valid_for_profit = billed > 0 and actual_cost > 0
        else:
            profit = contract - budget_cost
            margin = (profit / contract * 100) if contract > 0 else 0
            profit_basis = "projected"
            valid_for_profit = contract > 0 and budget_cost > 0

    return {
        "job_no": job_no,
        "job_description": job.get("job_description", ""),
        "project_manager": job.get("project_manager_name", ""),
        "customer_name": job.get("customer_name", ""),
        "job_status": job_status,
        "original_contract": original_contract,
        "contract": contract,
        "budget_cost": budget_cost,
        "actual_cost": actual_cost,
        "billed": billed,
        "has_budget": has_budget,
        "percent_complete": round(percent_complete, 2),
        "earned_revenue": round(earned_revenue, 2),
        "backlog": round(backlog, 2),
        "profit": round(profit, 2),
        "margin": round(margin, 2),
        "valid_for_profit": valid_for_profit,
        "profit_basis": profit_basis,
        "over_under_billing": round(over_under_billing, 2),
    }

# ==========================================================
# AR METRICS (FIXED + FOUNDATION-SAFE)
# ==========================================================

def calculate_ar_invoice_metrics(invoice: dict, receipt_totals: Dict[str, float] = None, aging_date: datetime = None) -> Optional[dict]:
    invoice_no = str(invoice.get("invoice_no", ""))
    invoice_amount = safe_float(invoice.get("invoice_amount"))
    collectible = safe_float(invoice.get("calculated_amount_due"))
    retainage = safe_float(invoice.get("retainage_amount"))

    # Check if fully paid via receipts (total receipts >= invoice amount)
    # This excludes invoices where retainage was collected but still shows in export
    if receipt_totals and invoice_amount > 0:
        total_receipts = receipt_totals.get(invoice_no, 0)
        if total_receipts >= invoice_amount:
            return None  # Fully paid - exclude from AR aging

    # HARD GUARD — do not silently drop all AR
    if collectible == 0 and retainage == 0:
        return None

    # Use provided aging date or current date
    if aging_date is None:
        aging_date = datetime.now()

    invoice_date = excel_to_date(invoice.get("invoice_date"))
    if invoice_date:
        days_outstanding = max(0, (aging_date - invoice_date).days)
    else:
        days_outstanding = int(safe_float(invoice.get("days_outstanding")))

    if days_outstanding <= 30:
        aging_bucket = "0-30"
    elif days_outstanding <= 60:
        aging_bucket = "31-60"
    elif days_outstanding <= 90:
        aging_bucket = "61-90"
    else:
        aging_bucket = "90+"

    return {
        "invoice_no": invoice.get("invoice_no", ""),
        "customer_name": invoice.get("customer_name", "").strip(),
        "project_manager": invoice.get("project_manager_name", "").strip(),
        "job_no": invoice.get("job_no", ""),
        "job_description": invoice.get("job_description", ""),
        "invoice_date": invoice.get("invoice_date", ""),
        "invoice_amount": safe_float(invoice.get("invoice_amount")),
        "collectible": round(collectible, 2),
        "retainage": round(retainage, 2),
        "total_due": round(collectible + retainage, 2),
        "days_outstanding": days_outstanding,
        "aging_bucket": aging_bucket,
    }

# ==========================================================
# RUNNERS
# ==========================================================

def run_jobs_etl() -> List[dict]:
    data = load_json_file("financials_jobs.json")

    budgets = data.get("job_budgets", [])
    actuals = data.get("job_actuals", [])
    billed = data.get("job_billed_revenue", [])

    actual_by_job = {}
    for a in actuals:
        job_no = str(int(a["Job_No"])) if isinstance(a["Job_No"], float) else str(a["Job_No"])
        actual_by_job[job_no] = actual_by_job.get(job_no, 0) + safe_float(a["Actual_Cost"])

    billed_by_job = {}
    for b in billed:
        job_no = str(int(b["Job_No"])) if isinstance(b["Job_No"], float) else str(b["Job_No"])
        billed_by_job[job_no] = safe_float(b["Billed_Revenue"])

    return [
        calculate_job_metrics(
            job,
            actual_by_job.get(str(job.get("job_no")), 0),
            billed_by_job.get(str(job.get("job_no")), 0),
        )
        for job in budgets
    ]


def run_ar_etl() -> List[dict]:
    data = load_json_file("ar_invoices.json")
    invoices = data.get("invoices", [])

    # Load AR receipt allocations to detect fully-paid invoices
    receipts_data = load_json_file("ar_receipt_job_allocation.json")
    receipt_totals = build_receipt_totals_by_invoice(receipts_data.get("allocations", []))

    # Use current date for aging calculations (real-time)
    aging_date = datetime.now()
    print(f"[AR ETL] Using aging date: {aging_date.strftime('%Y-%m-%d')}")

    results = []
    excluded_count = 0
    for inv in invoices:
        m = calculate_ar_invoice_metrics(inv, receipt_totals, aging_date)
        if m:
            results.append(m)
        else:
            excluded_count += 1

    if excluded_count > 0:
        print(f"[AR ETL] Excluded {excluded_count} fully-paid invoices from AR aging")

    if not results:
        raise RuntimeError("AR metrics empty — upstream schema or data issue detected")

    return results


def run_ap_etl() -> List[dict]:
    data = load_json_file("ap_invoices.json")
    results = []

    for inv in data.get("invoices", []):
        remaining = safe_float(inv.get("remaining_balance"))
        if remaining <= 0:
            continue

        vendor = inv.get("vendor_name", "").strip()
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
            {"generated_at": datetime.now(timezone.utc).isoformat()},
            f,
            indent=2,
        )

    print("[MetricsETL] Metrics successfully written")

# ==========================================================
# ENTRY POINT
# ==========================================================

if __name__ == "__main__":
    write_metrics_outputs()
