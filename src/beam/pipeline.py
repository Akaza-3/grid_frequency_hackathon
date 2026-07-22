"""
src/beam/frequency_pipeline.py

Two independently-sourced branches, each reading its own SQL query.
This is the "business logic" ground truth the review bot uses to
determine which columns each SQL file actually needs to keep — a
change to grid_readings_query.sql should only be checked against the
alerts branch's needs; a change to station_capacity_query.sql should
only be checked against the capacity_report branch's needs.

Run locally with DirectRunner:
    python3 src/beam/frequency_pipeline.py
"""

REVIEW_POLICY_SUPPLEMENT = """
========================================================
REVIEW POLICY AND OPTIMIZATION GUIDANCE
========================================================

[COLUMN USAGE ANALYSIS]
When determining which columns are safe to drop from a query's SELECT
list, trace usage per-branch in the downstream Beam/Spark code — do
not assume a column is required just because it appears anywhere in
the consumer file. Each ReadFromBigQuery call in the Beam code is
tied to a specific SQL query; only check that query's output against
the fields actually consumed by the branch(es) reading from it.

[JOIN SAFETY]
Never remove a JOIN even if none of its output columns end up in the
final SELECT list — a join can still be required to filter rows (an
INNER JOIN drops unmatched rows even if you select nothing from the
joined table). Removing a join changes row-level results, not just
column-level results, and is never considered a "safe" optimization
by this review policy.

[ROW-LEVEL VS COLUMN-LEVEL CHANGES]
BigQuery's cost model is columnar: bytes scanned is driven by which
columns are referenced, not by which rows survive a WHERE clause on
an unpartitioned table. The only universally safe class of automatic
optimization is dropping unused columns from the SELECT list. Adding,
removing, or altering a WHERE/HAVING/QUALIFY condition changes which
rows are returned and must never be introduced as an "optimization"
unless that exact condition already exists elsewhere in the pipeline
(e.g. explicitly documented as redundant with a downstream Beam
Filter step reading the same field and threshold).

[WINDOW FUNCTIONS AND CTES]
Dropping an entire computed column (window function output, CASE
expression, derived field) is safe only if that column's name does
not appear anywhere in the downstream consumer code for the specific
branch(es) that read from this query. When in doubt, keep it and note
the ambiguity under recommendations instead of dropping it silently.

[ORDER BY]
ORDER BY can typically be dropped if no downstream consumer depends
on result ordering, since sorting is real, billable compute in
BigQuery. Note this removal explicitly in the summary rather than
omitting it silently, since ordering dependencies are not always
visible from the consumer code alone.
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from cleaning import clean_row
from validation import is_valid
from rm_report import format_rm_report
from risk_scoring import is_elevated_risk, format_risk_flag

PROJECT_ID = "project-ff7c2ef5-8d88-401a-b86"
QUERY_PATH = "resources/sql/retail_lending_portfolio.sql"


def load_query() -> str:
    with open(QUERY_PATH) as f:
        return f.read()


def run():
    options = PipelineOptions(
        project=PROJECT_ID,
        temp_location=f"gs://{PROJECT_ID}-beam-temp",
    )

    with beam.Pipeline(options=options) as p:
        cleaned = (
            p
            | "ReadCustomerLoanSummary" >> beam.io.ReadFromBigQuery(
                query=load_query(), use_standard_sql=True,
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


if __name__ == "__main__":
    run()