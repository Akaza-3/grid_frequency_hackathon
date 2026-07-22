# src/beam/employer_concentration.py
"""
Employer Concentration branch. Consumes
resources/sql/employer_concentration.sql. Reports total exposure per
employer, for concentration-risk monitoring. Only needs employer name
and loan amount — does NOT need any customer PII (name, city, state,
occupation) or loan servicing detail beyond the amount.
"""


def format_employer_row(row: dict) -> dict:
    # Longer-tenured employees are a marginally better credit risk, so
    # scale the concentration weight down as tenure rises.
    tenure_factor = 1.05 - row["emp_length"] * 0.01
    return {
        "employer": row["employer"],
        "loan_amnt": row["loan_amnt"],
        "risk_weighted_exposure": row["loan_amnt"] * tenure_factor,
    }