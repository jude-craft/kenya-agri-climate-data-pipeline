with market as (
    select * from {{ ref('stg_market') }}
),

weather as (
    select * from {{ ref('int_weather_rolling') }}
),

trade_routes as (
    select * from {{ ref('trade_routes') }}
),

joined as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'm.market_id',
            'w.weather_id'
        ]) }} as impact_id,

        m.price_date,
        
        -- Consumer Hub Dimensions
        m.county as consumer_county,
        m.market as consumer_market,
        m.commodity,
        m.unit_of_measure,
        m.price_kes,
        
        -- Producer Heartland Dimensions (via Seed)
        w.county as producer_county,
        
        -- Producer Climate Metrics
        w.temp_mean_c_7d_avg as producer_temp_mean_c_7d_avg,
        w.rainfall_mm_14d_sum as producer_rainfall_mm_14d_sum,
        w.rainfall_mm_30d_sum as producer_rainfall_mm_30d_sum,
        w.evapotranspiration_mm_14d_avg as producer_evapotranspiration_mm_14d_avg,
        w.solar_radiation_mj_m2 as producer_solar_radiation_mj_m2,
        w.wind_speed_max_kmh as producer_wind_speed_max_kmh

    from market m
    -- 1. First, lookup the assigned producer for this consumer & commodity
    inner join trade_routes tr 
        on m.county = tr.consumer_county
        and m.commodity = tr.commodity
    -- 2. Then, fetch the weather for that specific producer on that exact date
    inner join weather w 
        on tr.primary_producer_county = w.county
        and m.price_date = w.weather_date
)

select * from joined