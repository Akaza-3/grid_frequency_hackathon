-- resources/sql/grid_readings_query.sql
-- Feeds src/beam/frequency_pipeline.py, which only ever reads
-- station_id, frequency_hz, and region from this result set.
-- voltage and timestamp are selected here but never consumed downstream.

SELECT *
FROM `project-ff7c2ef5-8d88-401a-b86.grid_data.grid_readings` r
JOIN `project-ff7c2ef5-8d88-401a-b86.grid_data.station_metadata` m
ON r.station_id = m.station_id
WHERE r.region = 'west'
-- trivial change to create a second commit
-- clean final test
