# src/beam/risk_scoring.py
"""
Credit Risk Monitoring branch. Flags customers with meaningful
outstanding principal despite weak underwriting signals. Does NOT
read city, state, occupation, employer, or issue_d — those are
RM-report-only fields.

NOTE: this branch also reads row["sub_grade"] for a finer-grained
risk tier — but sub_grade is NOT currently selected by
resources/sql/retail_lending_portfolio.sql. This is intentional: a
real gap in the current query, kept here so the review bot has a
genuine "missing column" case to catch, not just "unused column"
cases.
"""


def is_elevated_risk(row: dict) -> bool:
    return row["revol_util"] > 60 or row["dti"] > 20


def format_risk_flag(row: dict) -> dict:
    return {
        "customer_name": row["customer_name"],
        "grade": row["grade"],
        "sub_grade": row["sub_grade"],
        "dti": row["dti"],
        "revol_util": row["revol_util"],
        "out_prncp": row["out_prncp"],
        "avg_interest": row["avg_interest"],
    }