-- resources/sql/grid_readings_query.sql
--comment 1
--comment 2
--comment 3
--comment 4
--comment 5
--comment 6
--comment 7
--comment 8
--comment 9
WITH station_stats AS (
  SELECT
    r.*,
    m.station_name,
    m.capacity_mw,
    AVG(r.frequency_hz) OVER (
      PARTITION BY r.station_id
      ORDER BY r.timestamp
      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_freq,
    LAG(r.frequency_hz) OVER (
      PARTITION BY r.station_id ORDER BY r.timestamp
    ) AS prev_freq
  FROM `project-ff7c2ef5-8d88-401a-b86.grid_data.grid_readings` r
  JOIN `project-ff7c2ef5-8d88-401a-b86.grid_data.station_metadata` m
    ON r.station_id = m.station_id
),
flagged AS (
  SELECT *,
    CASE WHEN frequency_hz < 49.9 THEN 1 ELSE 0 END AS is_underfrequency
  FROM station_stats
)
SELECT *
FROM flagged
WHERE region = 'west'
ORDER BY timestamp DESC