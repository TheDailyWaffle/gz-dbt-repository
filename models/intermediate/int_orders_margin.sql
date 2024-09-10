
with

int_sales as (
    select *
    from {{ ref('int_sales_margin') }}
)

select
    MAX(date_date) as date_date,
    orders_id,
    ROUND(SUM(revenue),2) as order__revenue,
    ROUND(SUM(quantity)) as quantity,
    ROUND(SUM(purchase_cost), 2) as order_cost,
    ROUND(SUM(margin), 2) as order_margin
from int_sales
group by orders_id
order by orders_id DESC
