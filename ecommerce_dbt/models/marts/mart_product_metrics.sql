
with transactions as (

    select * from {{ ref('stg_transactions') }}

),

product_metrics as (

    select
        stockcode,
        mode() within group (order by description) as product_description,

    
        count(distinct invoice)         as times_ordered,
        

        count(distinct customer_id)     as unique_customers,
        
        sum(quantity)                   as total_units_sold,
        
        round(sum(line_revenue), 2)     as total_revenue,
        round(avg(price), 2)            as avg_unit_price,
        

        round(avg(line_revenue), 2)     as avg_line_revenue,

        count(distinct country)         as countries_sold_in
        
    from transactions
    group by stockcode

)

select * from product_metrics
order by total_revenue desc
