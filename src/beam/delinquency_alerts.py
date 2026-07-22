# src/beam/delinquency_alerts.py
"""
Delinquency Alerts branch. Consumes resources/sql/delinquency_alerts.sql.
Flags customers with any delinquency history for collections outreach.
Only needs identity + delinquency-specific fields — does NOT need
income, employment, or the wide set of loan servicing columns that
delinquency_alerts.sql currently selects via SELECT *.
"""


def format_delinquency_alert(row: dict) -> dict:
    return {
        "customer_id": row["customer_id"],
        "customer_name": row["customer_name"],
        "delinq_2yrs": row["delinq_2yrs"],
        "mths_since_last_delinq": row["mths_since_last_delinq"],
        "loan_status": row["loan_status"],
    }