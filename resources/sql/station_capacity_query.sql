-- resources/sql/station_capacity_query.sql
-- Feeds only the capacity_report branch in frequency_pipeline.py.
-- Deliberately wasteful: selects every column from both tables, but
-- the capacity_report branch only ever reads region, station_name,
-- and capacity_mw — frequency_hz, timestamp, voltage, station_id are
-- all dead weight here.

SELECT *
FROM `project-ff7c2ef5-8d88-401a-b86.grid_data.grid_readings` r
JOIN `project-ff7c2ef5-8d88-401a-b86.grid_data.station_metadata` m
  ON r.station_id = m.station_id