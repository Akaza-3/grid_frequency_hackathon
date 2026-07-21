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
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

PROJECT_ID = "project-ff7c2ef5-8d88-401a-b86"
ALERTS_QUERY_PATH = "resources/sql/grid_readings_query.sql"
CAPACITY_QUERY_PATH = "resources/sql/station_capacity_query.sql"


def load_query(path: str) -> str:
    with open(path) as f:
        return f.read()


def run():
    options = PipelineOptions(
        project=PROJECT_ID,
        temp_location=f"gs://{PROJECT_ID}-beam-temp",
    )

    with beam.Pipeline(options=options) as p:

        # --- Branch 1: underfrequency alerting ---
        # Sourced from grid_readings_query.sql. Only needs station_id,
        # frequency_hz, region, and timestamp (for ordering the alert).
        (
            p
            | "ReadAlertsData" >> beam.io.ReadFromBigQuery(
                query=load_query(ALERTS_QUERY_PATH),
                use_standard_sql=True,
            )
            | "FilterUnderfrequency" >> beam.Filter(lambda r: r["frequency_hz"] < 49.9)
            | "FormatAlert" >> beam.Map(lambda r: {
                "station_id": r["station_id"],
                "frequency_hz": r["frequency_hz"],
                "region": r["region"],
                "timestamp": r["timestamp"],
            })
            | "PrintAlerts" >> beam.Map(lambda r: print("ALERT:", r))
        )

        # --- Branch 2: regional capacity utilization report ---
        # Sourced from station_capacity_query.sql. Only needs region,
        # station_name, capacity_mw — never touches frequency_hz,
        # timestamp, or voltage.
        (
            p
            | "ReadCapacityData" >> beam.io.ReadFromBigQuery(
                query=load_query(CAPACITY_QUERY_PATH),
                use_standard_sql=True,
            )
            | "KeepCapacityFields" >> beam.Map(lambda r: {
                "region": r["region"],
                "station_name": r["station_name"],
                "capacity_mw": r["capacity_mw"],
            })
            | "PrintCapacity" >> beam.Map(lambda r: print("CAPACITY:", r))
        )


if __name__ == "__main__":
    run()