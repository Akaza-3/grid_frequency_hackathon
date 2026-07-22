# src/beam/employer_concentration.py
"""
Employer Concentration branch. Consumes
resources/sql/employer_concentration.sql. Reports total exposure per
employer, for concentration-risk monitoring. Only needs employer name
and loan amount — does NOT need any customer PII (name, city, state,
occupation) or loan servicing detail beyond the amount.
"""


def format_employer_row(row: dict) -> dict:
    # BUG: loan_amnt comes from l.* in employer_concentration.sql — it is a
    # STRING in the raw loan table. Multiplying a str by float raises TypeError.
    return {
        "employer": row["employer"],
        "loan_amnt": row["loan_amnt"],
        "risk_weighted_exposure": row["loan_amnt"] * 1.05,
    }