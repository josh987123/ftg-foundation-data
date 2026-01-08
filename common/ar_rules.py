from datetime import date

def compute_ar_row(
    *,
    invoice_amount: float,
    cash_applied: float,
    retainage_amount: float,
    invoice_date: date,
    as_of_date: date,
):
    """
    CANONICAL AR RULES — PDF-FAITHFUL

    This function defines what an AR invoice *is*.

    Rules:
    - total_due = invoice_amount - cash_applied
    - If total_due == 0 → invoice does NOT exist in AR
    - Retainage is DISPLAYED but capped to total_due
    - Collectible = total_due - retainage
    - Aging based on invoice_date vs as_of_date

    This function must be reused everywhere.
    """

    # -------------------------------
    # Total Due (PDF Grand Total)
    # -------------------------------
    total_due = round(invoice_amount - cash_applied, 2)

    # PDF-faithful inclusion rule
    if round(total_due, 2) == 0:
        return None

    # -------------------------------
    # Retainage (display only)
    # -------------------------------
    retainage = min(
        max(retainage_amount, 0),
        max(total_due, 0),
    )

    retainage = round(retainage, 2)

    # -------------------------------
    # Collectible (Net Receivable)
    # -------------------------------
    collectible = round(total_due - retainage, 2)

    # -------------------------------
    # Aging
    # -------------------------------
    days_outstanding = max(0, (as_of_date - invoice_date).days)

    if days_outstanding <= 30:
        aging_bucket = "0-30"
    elif days_outstanding <= 60:
        aging_bucket = "31-60"
    elif days_outstanding <= 90:
        aging_bucket = "61-90"
    else:
        aging_bucket = "90+"

    return {
        "total_due": total_due,
        "retainage": retainage,
        "collectible": collectible,
        "days_outstanding": days_outstanding,
        "aging_bucket": aging_bucket,
    }
