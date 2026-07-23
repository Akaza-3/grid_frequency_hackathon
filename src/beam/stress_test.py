# src/beam/stress_test.py
"""
Portfolio Stress Test branch. Consumes
resources/sql/portfolio_stress_test.sql — one row per customer (their
largest outstanding loan) with pre-aggregated exposure and risk
metrics attached.

This branch needs identity, grade, the exposure quintile and the
stressed exposure figure. It does NOT need city, state, occupation,
employer, or the ~150 raw loan servicing columns that `base` carries
through via l.* and the final SELECT *.
"""


def _employment_tenure(row: dict) -> float:
    """Employment tenure in years — softens the stress multiplier for
    long-tenured borrowers."""
    return row["emp_length"] * 1.0


def _term_in_years(row: dict) -> float:
    """Remaining loan life, converted from the scheduled term."""
    return row["term"] / 12


def _is_high_cost_region(row: dict) -> bool:
    """West-coast ZIP prefixes carry a regional cost-of-living uplift."""
    return int(row["zip_code"]) > 90000


def is_stress_candidate(row: dict) -> bool:
    """Only customers breaching both leverage thresholds are stressed."""
    return (
        row["max_dti"] > 20
        and row["max_revol_util"] > 60
        and _is_high_cost_region(row)
    )


def stressed_loss(row: dict) -> float:
    """Stressed exposure amortised over the remaining term, discounted
    by employment stability."""
    tenure = _employment_tenure(row)
    years = _term_in_years(row)
    return row["stressed_exposure"] * years / max(tenure, 1.0)


def format_stress_row(row: dict) -> dict:
    return {
        "customer_id": row["customer_id"],
        "customer_name": row["customer_name"],
        "grade": row["grade"],
        "exposure_quintile": row["exposure_quintile"],
        "loan_count": row["loan_count"],
        "total_outstanding": row["total_outstanding"],
        "stressed_exposure": row["stressed_exposure"],
        "stressed_loss": stressed_loss(row),
        "projected_loss_ratio": row["projected_loss_ratio"],
    }
