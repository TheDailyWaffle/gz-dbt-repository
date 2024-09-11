
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
        date_date, 
        orders_id as order_id,
        margin,
        revenue,
    from {{ ref("int_sales_margin") }}
),
ship as (
    select
        orders_id as order_id,
        shipping_fee,
        logcost,
        ship_cost,
    from {{ ref("stg_raw__ship") }}
),
order_grouped as (
    select 
        MAX(m.date_date) as date_date,
        m.order_id,
        ROUND(SUM(m.margin), 2) as margin,
        ROUND(SUM(m.revenue), 2) as revenue,
        ROUND(AVG(s.shipping_fee), 2) as shipping_fee,
        ROUND(AVG(s.logcost), 2) as logcost,
        ROUND(AVG(s.ship_cost), 2) as ship_cost,
from sales m
left join ship s using(order_id)
group by m.order_id
order by m.order_id DESC
)

select
    date_date,
    order_id,
    ROUND(margin + shipping_fee - logcost - ship_cost, 2) as operational_margin,
    margin,
    revenue,
    shipping_fee,
    logcost,
    ship_cost
from order_grouped
order by order_id DESC



-- GROUPED
-- select 
--     MAX(m.date_date) as date_date,
--     m.order_id,
--     ROUND(SUM(m.margin + s.shipping_fee - s.logcost - s.ship_cost), 2) as operational_margin,
--     ROUND(SUM(m.margin), 2) as margin,
--     ROUND(SUM(m.revenue), 2) as revenue,
--     ROUND(AVG(s.shipping_fee), 2) as shipping_fee,
--     ROUND(AVG(s.logcost), 2) as logcost,
--     ROUND(AVG(s.ship_cost), 2) as ship_cost,
-- from sales m
-- left join ship s using(order_id)
-- group by m.order_id
-- order by m.order_id DESC


-- UNGROUPED
-- select 
--     date_date,
--     m.order_id,
--     ROUND(m.margin + s.shipping_fee - s.logcost - s.ship_cost, 2) as operational_margin,
--     m.margin,
--     m.revenue,
--     s.shipping_fee,
--     s.logcost,
--     s.ship_cost,
-- from sales m
-- left join ship s using(order_id)
-- order by m.order_id DESC
