model (
  name mantra_recitation.marts.mantra_recitations_daily
  ,kind incremental_by_time_range (
    time_column event_date
  )
  ,cron '@daily'
  ,grain (user_id, mantra_name, event_date)
  ,audits (
    not_null(columns := (user_id, mantra_name, event_date))
  )
  ,physical_properties (
    table_format = 'iceberg'
    ,external_volume = '@var('external_volume')'
    ,catalog = '@var('catalog')'
  )
);

-- daily aggregation of mantra recitations by user and mantra
select
  user_id
  ,mantra_name
  ,date(recitation_timestamp) as event_date
  ,count(*) as recitation_count
  ,sum(count) as total_count
  ,sum(duration_minutes) as total_duration_minutes
  ,avg(duration_minutes) as avg_duration_minutes
  ,min(recitation_timestamp) as first_recitation_timestamp
  ,max(recitation_timestamp) as last_recitation_timestamp
  ,current_timestamp() as processed_at
from mantra_recitation.cleansed.cln_mantra_recitation
where
  date(recitation_timestamp) between @start_ds and @end_ds
group by
  user_id
  ,mantra_name
  ,date(recitation_timestamp)
