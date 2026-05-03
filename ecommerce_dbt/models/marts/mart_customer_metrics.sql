
with transactions as (
    select * from {{ ref('stg_transactions') }}
),

customer_metrics as (

    select
        customer_id,
        mode() within group (order by country) as primary_country,

        count(distinct invoice)     as total_orders,
        count(*)                    as total_line_items,

        sum(quantity)               as total_units_purchased,
        
        round(sum(line_revenue), 2)         as total_revenue,
        round(avg(line_revenue), 2)         as avg_line_revenue,
        round(sum(line_revenue) / nullif(count(distinct invoice), 0), 2) as avg_order_value,
      
        min(invoicedate)            as first_order_date,
        max(invoicedate)            as last_order_date,
       

        datediff('day', min(invoicedate), max(invoicedate)) as customer_tenure_days
        

    from transactions
    group by customer_id
    

)

select * from customer_metrics
order by total_revenue desc
