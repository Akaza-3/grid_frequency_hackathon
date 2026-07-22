"""
src/beam/pipeline.py

Six independently-sourced branches, each reading its own SQL query.
This is the "business logic" ground truth the review bot uses to
determine which columns each SQL file actually needs to keep — a
change to any one query should only be checked against the branch(es)
that actually read from it, not the whole pipeline's combined needs.

Run locally with DirectRunner:
    python3 src/beam/pipeline.py
"""
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from cleaning import clean_row
from validation import is_valid
from rm_report import format_rm_report
from risk_scoring import is_elevated_risk, format_risk_flag
from dashboard import format_dashboard_row, is_meaningful_exposure
from delinquency_alerts import format_delinquency_alert
from employer_concentration import format_employer_row
from watchlist import format_watchlist_row

PROJECT_ID = "project-ff7c2ef5-8d88-401a-b86"
LENDING_QUERY_PATH = "resources/sql/retail_lending_portfolio.sql"
DASHBOARD_QUERY_PATH = "resources/sql/customer_risk_dashboard.sql"
DELINQUENCY_QUERY_PATH = "resources/sql/delinquency_alerts.sql"
EMPLOYER_QUERY_PATH = "resources/sql/employer_concentration.sql"
WATCHLIST_QUERY_PATH = "resources/sql/high_risk_watchlist.sql"


def load_query(path: str) -> str:
    with open(path) as f:
        return f.read()


def run():
    options = PipelineOptions(
        project=PROJECT_ID,
        temp_location=f"gs://{PROJECT_ID}-beam-temp",
    )

    with beam.Pipeline(options=options) as p:

        # --- Source 1: retail_lending_portfolio.sql ---
        cleaned = (
            p
            | "ReadRetailLendingPortfolio" >> beam.io.ReadFromBigQuery(
                query=load_query(LENDING_QUERY_PATH), use_standard_sql=True,
            )
            | "CleanRows" >> beam.Map(clean_row)
            | "FilterValid" >> beam.Filter(is_valid)
        )

        (
            cleaned
            | "FormatRMReport" >> beam.Map(format_rm_report)
            | "PrintRMReport" >> beam.Map(lambda r: print("RM_REPORT:", r))
        )

        (
            cleaned
            | "FilterElevatedRisk" >> beam.Filter(is_elevated_risk)
            | "FormatRiskFlag" >> beam.Map(format_risk_flag)
            | "PrintRiskFlags" >> beam.Map(lambda r: print("RISK_FLAG:", r))
        )

        # --- Source 2: customer_risk_dashboard.sql ---
        (
            p
            | "ReadCustomerRiskDashboard" >> beam.io.ReadFromBigQuery(
                query=load_query(DASHBOARD_QUERY_PATH), use_standard_sql=True,
            )
            | "FilterMeaningfulExposure" >> beam.Filter(is_meaningful_exposure)
            | "FormatDashboardRow" >> beam.Map(format_dashboard_row)
            | "PrintDashboard" >> beam.Map(lambda r: print("DASHBOARD:", r))
        )

        # --- Source 3: delinquency_alerts.sql ---
        (
            p
            | "ReadDelinquencyAlerts" >> beam.io.ReadFromBigQuery(
                query=load_query(DELINQUENCY_QUERY_PATH), use_standard_sql=True,
            )
            | "FormatDelinquencyAlert" >> beam.Map(format_delinquency_alert)
            | "PrintDelinquencyAlerts" >> beam.Map(lambda r: print("DELINQUENCY:", r))
        )

        # --- Source 4: employer_concentration.sql ---
        (
            p
            | "ReadEmployerConcentration" >> beam.io.ReadFromBigQuery(
                query=load_query(EMPLOYER_QUERY_PATH), use_standard_sql=True,
            )
            | "FormatEmployerRow" >> beam.Map(format_employer_row)
            | "PrintEmployerConcentration" >> beam.Map(lambda r: print("EMPLOYER:", r))
        )

        # --- Source 5: high_risk_watchlist.sql ---
        (
            p
            | "ReadHighRiskWatchlist" >> beam.io.ReadFromBigQuery(
                query=load_query(WATCHLIST_QUERY_PATH), use_standard_sql=True,
            )
            | "FormatWatchlistRow" >> beam.Map(format_watchlist_row)
            | "PrintWatchlist" >> beam.Map(lambda r: print("WATCHLIST:", r))
        )


if __name__ == "__main__":
    run()