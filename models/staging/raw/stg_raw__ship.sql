with

    source as (select * from {{ source("raw", "ship") }}),

    renamed as (

        select 
            orders_id, 
            shipping_fee, 
            -- shipping_fee_1, 
            logcost, 
            ROUND(CAST(ship_cost AS FLOAT64),2) AS ship_cost 
        from source

    )

select *
from renamed
-- order by (shipping_fee_1 - shipping_fee) DESC
-- where shipping_fee <> shipping_fee_1