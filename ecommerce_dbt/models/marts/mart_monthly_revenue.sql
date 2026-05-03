
with transactions as (

    select * from {{ ref('stg_transactions') }}

),

monthly_aggregates as (

    select
        invoice_year,
        invoice_month,

        invoice_year || '-' || lpad(cast(invoice_month as varchar), 2, '0') as year_month,

        -- Transaction volume
        count(distinct invoice)         as total_orders,
        count(distinct customer_id)     as active_customers,
      
        round(sum(line_revenue), 2)     as total_revenue,
        round(avg(line_revenue), 2)     as avg_line_revenue,

        
        sum(quantity)                   as total_units_sold

    from transactions
    group by invoice_year, invoice_month

),

with_mom_growth as (

    select
        *,

        lag(total_revenue) over (
            order by invoice_year, invoice_month
        ) as prev_month_revenue,

    
        round(
            (total_revenue - lag(total_revenue) over (order by invoice_year, invoice_month))
            / nullif(lag(total_revenue) over (order by invoice_year, invoice_month), 0)
            * 100,
            2
        ) as mom_revenue_growth_pct

    from monthly_aggregates

)

select * from with_mom_growth
order by invoice_year, invoice_month
