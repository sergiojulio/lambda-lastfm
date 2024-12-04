/*
https://docs.getdbt.com/docs/build/incremental-strategy
*/
{{
  config(
    materialized = 'incremental',
    incremental_strategy='insert_overwrite' if target.name == 'prd' else 'delete+insert',
    unique_key = 'date',
    cluster_by = ['date']
  )
}}


with source_data as (

    select 
        -- cast timestamp(6)
        cast(date_format(from_unixtime("timestamp"), '%Y-%m-%d %h:%i:%s') as timestamp) as "datetime",
        artist,
        album,
        track,
        "date"
    from 
        lastfm.silver_tracks 
    {% if is_incremental() %}    

/*
is_incremental() macro

is_incremental() is a flag that identifies the execution mode; false if we are in 
Full Refresh mode and true in Incremental Mode. This macro is used within your model’s 
.sql definition. The most common use case is to add a filter to the where clause to 
limit the quantity of data pulled from the upstream models.
*/

    where 
        "date" = date('{{ var('date') }}')
    {% endif %}
)

select * from source_data

