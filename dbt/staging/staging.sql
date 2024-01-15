{{ config(materialized='view') }}

with
    locations as (

        select locationid, zone, borough as zone_code

        from {{ source('staging', 'external_table_zones') }}

    ),

    routes as (
        select
            vendorid as taxi_id,
            date(tpep_pickup_datetime) as pickup_time,
            date(tpep_dropoff_datetime) as dropoff_time,
            passenger_count,
            trip_distance as distance,
            pulocationid,
            dolocationid,
            timestamp_diff(tpep_dropoff_datetime, tpep_pickup_datetime, MINUTE) as trip_duration_in_minutes,
            {{get_payment_desc('payment_type')}} as payment_type,
            total_amount as paid

        from {{ source('staging', 'external_table_trips') }}
    )

select r.taxi_id, r.pickup_time, lpu.zone as pickup_location, lpu.zone as pickup_zone, lpu.zone_code as pickup_zone_code,
r.dropoff_time, ldo.zone as dropoff_location, ldo.zone as dropoff_zone, ldo.zone_code as dropoff_zone_code,
r.trip_duration_in_minutes, r.passenger_count, r.distance, r.payment_type, r.paid
from locations lpu, locations ldo, routes r
where lpu.locationid = r.pulocationid and ldo.locationid = r.dolocationid