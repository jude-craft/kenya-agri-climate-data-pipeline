with weather as (
    select
        *
    from
        {{ ref('stg_weather') }}
),
rolling_metrics as (
    select
        weather_id,
        weather_date,
        county,
        temp_max_c,
        temp_min_c,
        temp_mean_c,
        precipitation_mm,
        evapotranspiration_mm,
        wind_speed_max_kmh,
        solar_radiation_mj_m2,
        -- 7-Day Average Temperature (Heat Stress)
        round(
            avg(temp_mean_c) over (
                partition by county
                order by
                    weather_date rows between 6 preceding
                    and current row
            ),
            2
        ) as temp_mean_c_7d_avg,
        -- 14-Day Cumulative Rainfall (Short-term moisture)
        round(
            sum(precipitation_mm) over (
                partition by county
                order by
                    weather_date rows between 13 preceding
                    and current row
            ),
            2
        ) as rainfall_mm_14d_sum,
        -- 30-Day Cumulative Rainfall (Drought Indicator)
        round(
            sum(precipitation_mm) over (
                partition by county
                order by
                    weather_date rows between 29 preceding
                    and current row
            ),
            2
        ) as rainfall_mm_30d_sum,
        -- 14-Day Cumulative Rainfall (Crop water demand)
        round(
            avg(evapotranspiration_mm) over (
                partition by county
                order by
                    weather_date rows between 13 preceding
                    and current row
            ),
            2
        ) as evapotranspiration_mm_14d_avg
    from
        weather
)
select
    *
from
    rolling_metrics