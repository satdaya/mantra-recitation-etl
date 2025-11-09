MODEL (
  name mantra_recitation.marts.mantra_recitations_daily,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_date
  ),
  cron '@daily',
  grain (user_id, mantra_name, event_date),
  audits (
    NOT_NULL(columns := (user_id, mantra_name, event_date))
  ),
  physical_properties (
    table_format = 'ICEBERG',
    external_volume = '@var('external_volume')',
    catalog = '@var('catalog')'
  )
);

-- Daily aggregation of mantra recitations by user and mantra
SELECT
  user_id,
  mantra_name,
  DATE(event_timestamp) AS event_date,
  COUNT(*) AS recitation_count,
  SUM(repetitions) AS total_repetitions,
  SUM(duration_seconds) AS total_duration_seconds,
  AVG(duration_seconds) AS avg_duration_seconds,
  MIN(event_timestamp) AS first_recitation_timestamp,
  MAX(event_timestamp) AS last_recitation_timestamp,
  COUNT(DISTINCT device_type) AS unique_devices_used,
  CURRENT_TIMESTAMP() AS processed_at
FROM mantra_recitation.staging.stg_mantra_recitations
WHERE
  DATE(event_timestamp) BETWEEN @start_ds AND @end_ds
GROUP BY
  user_id,
  mantra_name,
  DATE(event_timestamp);
