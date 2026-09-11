with source as (
    select
        *
    from
        {{ source('bronze_layer', 'raw_weather') }}
),
staged as (
    select
        -- Surrogate Key (Primary Key for daily county weather)
        {{ dbt_utils.generate_surrogate_key(['date', 'county']) }} as weather_id,
        -- Temporal & Spatial Dimensions
        cast(date as date) as weather_date,
        cast(county as string) as county,
        -- Temperature Metrics
        cast(temp_max_c as float64) as temp_max_c,
        cast(temp_min_c as float64) as temp_min_c,
        round(
            cast((temp_max_c + temp_min_c) / 2.0 as float64),
            2
        ) as temp_mean_c,
        -- Precipitation & Moisture Metrics
        cast(precipitation_mm as float64) as precipitation_mm,
        cast(evapotranspiration_mm as float64) as evapotranspiration_mm,
        -- Atmospheric & Energy Metrics
        cast(wind_speed_max_kmh as float64) as wind_speed_max_kmh,
        cast(solar_radiation_mj_m2 as float64) as solar_radiation_mj_m2
    from
        source
)
select
    *
from
    staged