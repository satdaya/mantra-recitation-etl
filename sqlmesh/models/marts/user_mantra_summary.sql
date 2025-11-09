MODEL (
  name mantra_recitation.marts.user_mantra_summary,
  kind FULL,
  cron '@daily',
  grain (user_id, mantra_name),
  audits (
    NOT_NULL(columns := (user_id, mantra_name))
  ),
  physical_properties (
    table_format = 'ICEBERG',
    external_volume = '@var('external_volume')',
    catalog = '@var('catalog')'
  )
);

-- User-level summary of mantra recitation activity (lifetime)
SELECT
  user_id,
  mantra_name,
  COUNT(DISTINCT event_date) AS days_active,
  COUNT(*) AS total_sessions,
  SUM(recitation_count) AS total_recitations,
  SUM(total_repetitions) AS total_repetitions,
  SUM(total_duration_seconds) AS total_duration_seconds,
  AVG(total_duration_seconds) AS avg_session_duration,
  MIN(event_date) AS first_activity_date,
  MAX(event_date) AS last_activity_date,
  DATEDIFF('day', MIN(event_date), MAX(event_date)) + 1 AS days_span,
  CURRENT_TIMESTAMP() AS processed_at
FROM mantra_recitation.marts.mantra_recitations_daily
GROUP BY
  user_id,
  mantra_name;
