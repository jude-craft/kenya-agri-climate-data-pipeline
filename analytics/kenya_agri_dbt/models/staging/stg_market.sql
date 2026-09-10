with source as (
    select * from {{source('bronze_layer', 'raw_market')}}
),

staged as (
    select 
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['date','market','commodity' ]) }} as market_id,

        -- dimensions
        cast(date as date) as price_date,
        cast(county as string) as county,
        cast(market as string) as market,
        cast(commodity as string) as commodity,
        cast(unit as string) as unit_of_measure,

        -- metrics
        cast(price_kes as float64) as price_kes 

    from source

)

select * from staged