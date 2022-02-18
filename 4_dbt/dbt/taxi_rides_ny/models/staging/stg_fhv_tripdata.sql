{{ config(materialized='view') }}

with tripdata as 
(
    -- select *,
    --         row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
    -- from {{ source('staging', 'fhv_non_partitioned') }}
    -- where dispatching_base_num is not null
    select *
    from {{ source('staging', 'fhv_non_partitioned') }}
)
Select 
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as trip_id,
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    PULocationID as pickup_locationid,
    DOLocationID as dropoff_locationid,
    SR_Flag as sr_flag
    from tripdata
-- where rn = 1
-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}
    limit 100
{% endif %}