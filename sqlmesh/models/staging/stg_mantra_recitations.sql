MODEL (
  name mantra_recitation.staging.stg_mantra_recitations,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_timestamp
  ),
  cron '@daily',
  grain (recitation_id, event_timestamp),
  audits (
    NOT_NULL(columns := (recitation_id, user_id, mantra_name, event_timestamp)),
    UNIQUE_VALUES(columns := (recitation_id))
  ),
  physical_properties (
    table_format = 'ICEBERG',
    external_volume = '@var('external_volume')',
    catalog = '@var('catalog')'
  )
);

-- Staging model for raw mantra recitation data
-- This would typically pull from a raw layer or external source
SELECT
  recitation_id::VARCHAR AS recitation_id,
  user_id::VARCHAR AS user_id,
  mantra_name::VARCHAR AS mantra_name,
  repetitions::INTEGER AS repetitions,
  duration_seconds::INTEGER AS duration_seconds,
  event_timestamp::TIMESTAMP AS event_timestamp,
  created_at::TIMESTAMP AS created_at,
  updated_at::TIMESTAMP AS updated_at,
  device_type::VARCHAR AS device_type,
  app_version::VARCHAR AS app_version
FROM mantra_recitation.raw.mantra_recitations
WHERE
  event_timestamp BETWEEN @start_ds AND @end_ds
  AND recitation_id IS NOT NULL;
