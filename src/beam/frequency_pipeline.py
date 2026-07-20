"""
src/beam/frequency_pipeline.py

Minimal Beam pipeline standing in for a real Dataflow job. This is the
"business logic" ground truth: the review bot reads this file alongside
the SQL in resources/sql/ so it can tell which columns from the query
result are actually consumed downstream, versus just selected and
discarded.

Run locally with DirectRunner:
    python3 src/beam/frequency_pipeline.py
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
        (
            p
            | "ReadReadings" >> beam.io.ReadFromBigQuery(
                query=load_query(),
                use_standard_sql=True,
            )
            # Ground truth: only these three fields are ever touched below.
            # voltage and timestamp (and anything from station_metadata
            # besides station_id) are dead weight in the SQL as written.
            | "KeepUsedFields" >> beam.Map(
                lambda row: {
                    "station_id": row["station_id"],
                    "frequency_hz": row["frequency_hz"],
                    "region": row["region"],
                }
            )
            | "FilterLowFrequency" >> beam.Filter(lambda row: row["frequency_hz"] < 49.9)
            | "Print" >> beam.Map(print)
        )


if __name__ == "__main__":
    run()
