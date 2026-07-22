# src/beam/cleaning.py
"""
Row-level cleaning shared by every branch. Casts stringly-typed
numeric fields (common in this dataset — dti, revol_util, out_prncp
etc. often arrive as strings) and strips obviously malformed values
before anything else touches the row.
"""


def clean_row(row: dict) -> dict:
    def safe_float(value):
        try:
            return float(value) if value not in (None, "", "n/a") else None
        except (ValueError, TypeError):
            return None

    row["loan_amnt"] = safe_float(row.get("loan_amnt"))
    row["int_rate"] = safe_float(row.get("int_rate"))
    row["annual_inc"] = safe_float(row.get("annual_inc"))
    row["dti"] = safe_float(row.get("dti"))
    row["revol_util"] = safe_float(row.get("revol_util"))
    row["out_prncp"] = safe_float(row.get("out_prncp"))
    row["avg_interest"] = safe_float(row.get("avg_interest"))
    row["total_amount"] = safe_float(row.get("total_amount"))

    for field in ("customer_name", "city", "state", "occupation", "employer"):
        if row.get(field):
            row[field] = row[field].strip()

    return row