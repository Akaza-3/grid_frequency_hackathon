# src/beam/rm_report.py
"""
Relationship Manager Report branch. Who the customer is, where, what
they do, and their overall exposure. Does NOT read dti, revol_util,
or out_prncp — those are risk-branch-only fields.
"""


def format_rm_report(row: dict) -> dict:
    return {
        "customer_name": row["customer_name"],
        "city": row["city"],
        "state": row["state"],
        "occupation": row["occupation"],
        "employer": row["employer"],
        "customer_segment": row["customer_segment"],
        "grade": row["grade"],
        "loan_amnt": row["loan_amnt"],
        "annual_inc": row["annual_inc"],
        "total_loans": row["total_loans"],
        "total_amount": row["total_amount"],
        "avg_interest": row["avg_interest"],
        "issue_d": row["issue_d"],
    }