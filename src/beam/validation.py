# src/beam/validation.py
"""
Post-cleaning validation. Rows failing these checks are dropped
before reaching either business-logic branch — neither branch should
ever see a record with a missing customer_name or a negative loan
amount, so these fields matter to the pipeline as a whole, not just
one branch.
"""


def is_valid(row: dict) -> bool:
    if not row.get("customer_name"):
        return False
    if row.get("loan_amnt") is None or row["loan_amnt"] <= 0:
        return False
    if row.get("annual_inc") is None or row["annual_inc"] < 0:
        return False
    return True