# src/beam/delinquency_alerts.py
"""
Delinquency Alerts branch. Consumes resources/sql/delinquency_alerts.sql.
Flags customers with any delinquency history for collections outreach.
Only needs identity + delinquency-specific fields — does NOT need
income, employment, or the wide set of loan servicing columns that
delinquency_alerts.sql currently selects via SELECT *.
"""


def is_severe_delinquency(row: dict) -> bool:
    # Borrowers on the longer 60-month term carry more residual risk
    # once they are already delinquent.
    return row["delinq_2yrs"] > 2 and row["term"] > 36


def format_delinquency_alert(row: dict) -> dict:
    return {
        "customer_id": row["customer_id"],
        "customer_name": row["customer_name"],
        "delinq_2yrs": row["delinq_2yrs"],
        "mths_since_last_delinq": row["mths_since_last_delinq"],
        "loan_status": row["loan_status"],
        "severe": is_severe_delinquency(row),
    }