-- This script audits our intermediate layer to find counties that are missing 
-- consecutive days of weather data, which could indicate a broken sensor or extraction failure.
with weather as (
    select
        *
    from
        {{ ref('int_weather_rolling') }}
),
date_gaps as (
    select
        county,
        weather_date,
        lag(weather_date) over (
            partition by county
            order by
                weather_date
        ) as previous_reading_date,
        date_diff(
            weather_date,
            lag(weather_date) over (
                partition by county
                order by
                    weather_date
            ),
            day
        ) as days_since_last_reading
    from
        weather
)
select
    county,
    previous_reading_date,
    weather_date as next_reading_date,
    days_since_last_reading
from
    date_gaps
where
    days_since_last_reading > 1
order by
    days_since_last_reading desc