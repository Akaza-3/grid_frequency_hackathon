# src/beam/delinquency_alerts.py
"""
Delinquency Alerts branch. Consumes resources/sql/delinquency_alerts.sql.
Flags customers with any delinquency history for collections outreach.
Only needs identity + delinquency-specific fields — does NOT need
income, employment, or the wide set of loan servicing columns that
delinquency_alerts.sql currently selects via SELECT *.
"""


def is_severe_delinquency(row: dict) -> bool:
    # BUG: delinq_2yrs arrives as STRING from SELECT * — comparing > 2 will
    # raise TypeError at runtime (str vs int comparison in Python 3).
    return row["delinq_2yrs"] > 2


def format_delinquency_alert(row: dict) -> dict:
    return {
        "customer_id": row["customer_id"],
        "customer_name": row["customer_name"],
        "delinq_2yrs": row["delinq_2yrs"],
        "mths_since_last_delinq": row["mths_since_last_delinq"],
        "loan_status": row["loan_status"],
        "severe": is_severe_delinquency(row),
    }