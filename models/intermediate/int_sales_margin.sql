
with 
sales as (
    select
        date_date,
        orders_id,
        product_id,
        revenue,
        quantity,
    from {{ ref('stg_raw__sales') }}
),
product as (
    select 
        products_id as product_id,
        purchase_price,
    from {{ ref('stg_raw__product') }}
),
combined as (
    select 
        *,
        (sales.quantity * product.purchase_price) as purchase_cost,
    from sales
    left join product using(product_id)
)

select 
    date_date,
    product_id,
    orders_id,
    revenue,
    quantity,
    purchase_price,
    purchase_cost,
    ROUND((revenue / purchase_cost),2) as margin,
from combined
order by date_date ASC, orders_id ASC