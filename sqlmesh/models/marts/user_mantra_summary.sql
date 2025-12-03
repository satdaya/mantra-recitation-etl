model (
  name mantra_recitation.marts.user_mantra_summary
  ,kind full
  ,cron '@daily'
  ,grain (user_id, mantra_name)
  ,audits (
    not_null(columns := (user_id, mantra_name))
  )
  ,physical_properties (
    table_format = 'iceberg'
    ,external_volume = '@var('external_volume')'
    ,catalog = '@var('catalog')'
  )
);

-- user-level summary of mantra recitation activity (lifetime)
select
  user_id
  ,mantra_name
  ,count(distinct event_date) as days_active
  ,count(*) as total_sessions
  ,sum(recitation_count) as total_recitations
  ,sum(total_count) as total_count
  ,sum(total_duration_minutes) as total_duration_minutes
  ,avg(total_duration_minutes) as avg_session_duration
  ,min(event_date) as first_activity_date
  ,max(event_date) as last_activity_date
  ,datediff('day', min(event_date), max(event_date)) + 1 as days_span
  ,current_timestamp() as processed_at
from mantra_recitation.marts.mantra_recitations_daily
group by
  user_id
  ,mantra_name
