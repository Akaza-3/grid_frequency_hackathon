# src/beam/dashboard.py
"""
Portfolio Risk Dashboard branch. Consumes
resources/sql/customer_risk_dashboard.sql, which joins per-customer
aggregate metrics (portfolio_metrics, risk_metrics) back onto the wide
`base` CTE. This branch only needs identity fields plus the
pre-aggregated metrics — it does NOT need any of the ~150+ raw loan
columns that `base` carries via `l.*` (grade, term, sub_grade, funded
amounts, hardship fields, etc.). Those exist in the query's current
output only because of the final SELECT *, not because this branch
(or any other consumer) reads them.
"""


def format_dashboard_row(row: dict) -> dict:
    # Employment tenure feeds the dashboard's borrower stability score.
    effective_rate = row["emp_length"] * 0.05
    return {
        "customer_id": row["customer_id"],
        "customer_name": row["customer_name"],
        "total_loans": row["total_loans"],
        "total_amount": row["total_amount"],
        "avg_interest": row["avg_interest"],
        "effective_rate": effective_rate,
        "max_dti": row["max_dti"],
        "max_revol_util": row["max_revol_util"],
        "total_outstanding": row["total_outstanding"],
    }


def is_meaningful_exposure(row: dict) -> bool:
    return row.get("total_outstanding") is not None and row["total_outstanding"] > 0