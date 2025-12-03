model (
  name mantra_recitation.cleansed.cln_mantra_recitation
  ,kind incremental_by_time_range(
    time_column recitation_timestamp
  )
  ,cron '@daily'
  ,grain (id, recitation_timestamp)
  ,audits (
    not_null(columns := (id, user_id, mantra_name, recitation_timestamp))
    ,unique_values(columns := (id))
  )
  ,physical_properties (
    table_format = 'iceberg'
    ,external_volume = '@var('external_volume')'
    ,catalog = '@var('catalog')'
  )
);

-- cleansed mantra recitation data
-- maps raw column names to cleaner naming conventions
with final as (
    select
        id::varchar as id
        ,user_id::varchar as user_id
        ,mantra_name::varchar as mantra_name
        ,count::integer as count
        ,duration_minutes::integer as duration_minutes
        ,recitation_timestamp::timestamp as recitation_timestamp
        ,notes::varchar as notes
        ,created_at::timestamp as created_at
        ,updated_at::timestamp as updated_at
    from
        mantra_recitation.raw.mantra_recitations
    where
        recitation_timestamp between @start_ds and @end_ds
        and id is not null
)
select * from final
