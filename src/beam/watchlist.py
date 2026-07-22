# src/beam/watchlist.py
"""
High Risk Watchlist branch. Consumes
resources/sql/high_risk_watchlist.sql — one row per flagged customer
(their highest-outstanding-principal loan), for customers exceeding
DTI or revolving utilization thresholds. Needs identity, risk metrics,
and sub_grade for tiering — does NOT need city, state, occupation,
employer, or issue_d.
"""


def format_watchlist_row(row: dict) -> dict:
    # Total principal still at risk across the customer's flagged loans.
    net_exposure = row["out_prncp"] + row["total_outstanding"]
    return {
        "customer_id": row["customer_id"],
        "customer_name": row["customer_name"],
        "grade": row["grade"],
        "sub_grade": row["sub_grade"],
        "max_dti": row["max_dti"],
        "max_revol_util": row["max_revol_util"],
        "total_outstanding": row["total_outstanding"],
        "net_exposure": net_exposure,
        "loan_count": row["loan_count"],
    }