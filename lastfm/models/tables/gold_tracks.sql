/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below

https://docs.getdbt.com/docs/build/incremental-strategy

*/

{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'date',
    cluster_by = ['date']
  )
}}


with source_data as (

    select 
        date_format(from_unixtime("timestamp"), '%Y-%m-%d %h:%i:%s') as "datetime",
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
        "date" = date('2024-08-04')
    {% endif %}
)

select * from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
