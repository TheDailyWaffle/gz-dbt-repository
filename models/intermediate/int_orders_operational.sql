
-- Operational_margin = margin + shipping_fee - log_cost - ship_cost

-- int_sales_margin
--     orders_id
--     margin
--     date_date
-- stg_raw__ship
    -- shipping_fee
    -- logcost
    -- ship_cost

with
sales as (
    select
        orders_id as order_id,
        margin,
        date_date, 
    from {{ ref("int_sales_margin") }}
),
ship as (
    select
        orders_id as order_id,
        shipping_fee,
        logcost,
        ship_cost,
    from {{ ref("stg_raw__ship") }}
)
select 
    MAX(m.date_date) as date_date,
    m.order_id,
    ROUND(SUM(m.margin + s.shipping_fee - s.logcost - s.ship_cost), 2) as operational_margin,
    -- m.margin,
    -- s.shipping_fee,
    -- s.logcost,
    -- s.ship_cost,
from sales m
left join ship s using(order_id)
group by m.order_id
order by m.order_id DESC
