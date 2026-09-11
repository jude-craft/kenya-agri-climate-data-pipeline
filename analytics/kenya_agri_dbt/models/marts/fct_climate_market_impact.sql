with market as (
    select
        *
    from
        {{ ref('stg_market') }}
),
weather as (
    select
        *
    from
        {{ ref('int_weather_rolling') }}
),
joined as (
    select
        -- Identifiers
        m.market_id,
        -- Spatial & Temporal Dimensions
        m.price_date,
        m.county,
        m.market,
        -- Market Dimensions & Metrics
        m.commodity,
        m.unit_of_measure,
        m.price_kes,
        -- Climate Impact Metrics (Trailing & Daily)
        w.temp_mean_c_7d_avg,
        w.rainfall_mm_14d_sum,
        w.rainfall_mm_30d_sum,
        w.evapotranspiration_mm_14d_avg,
        w.wind_speed_max_kmh,
        w.solar_radiation_mj_m2
    from
        market m
        left join weather w on m.county = w.county
        and m.price_date = w.weather_date
)
select
    *
from
    joined