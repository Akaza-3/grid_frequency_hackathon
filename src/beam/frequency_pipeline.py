"""
src/beam/frequency_pipeline.py
"""
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

PROJECT_ID = "project-ff7c2ef5-8d88-401a-b86"
QUERY_PATH = "resources/sql/grid_readings_query.sql"


def load_query() -> str:
    with open(QUERY_PATH) as f:
        return f.read()


def run():
    options = PipelineOptions(
        project=PROJECT_ID,
        temp_location=f"gs://{PROJECT_ID}-beam-temp",
    )

    with beam.Pipeline(options=options) as p:
        readings = p | "ReadReadings" >> beam.io.ReadFromBigQuery(
            query=load_query(), use_standard_sql=True,
        )

        # Branch 1: underfrequency alerting — only needs station_id,
        # frequency_hz, region, timestamp (for alert ordering).
        alerts = (
            readings
            | "FilterUnderfrequency" >> beam.Filter(lambda r: r["frequency_hz"] < 49.9)
            | "FormatAlert" >> beam.Map(lambda r: {
                "station_id": r["station_id"],
                "frequency_hz": r["frequency_hz"],
                "region": r["region"],
                "timestamp": r["timestamp"],
            })
            | "PrintAlerts" >> beam.Map(lambda r: print("ALERT:", r))
        )

        # Branch 2: regional capacity utilization report — needs
        # region, capacity_mw, station_name. Does NOT need frequency_hz
        # or timestamp at all.
        capacity_report = (
            readings
            | "KeepCapacityFields" >> beam.Map(lambda r: {
                "region": r["region"],
                "station_name": r["station_name"],
                "capacity_mw": r["capacity_mw"],
            })
            | "PrintCapacity" >> beam.Map(lambda r: print("CAPACITY:", r))
        )


if __name__ == "__main__":
    run()