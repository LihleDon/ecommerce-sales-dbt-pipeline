-- mart_monthly_revenue.sql
-- One row per year-month combination
-- Answers: how is revenue trending over time? Which months are seasonal peaks?
-- Materialised as: table

with transactions as (

    select * from {{ ref('stg_transactions') }}

),

monthly_aggregates as (

    select
        invoice_year,
        invoice_month,

        -- Create a readable period label like "2011-03" for March 2011
        -- lpad pads the month number with a leading zero so months sort correctly
        -- Without lpad: "2011-3" would sort before "2011-12" alphabetically
        -- With lpad:    "2011-03" sorts before "2011-12" correctly
        invoice_year || '-' || lpad(cast(invoice_month as varchar), 2, '0') as year_month,

        -- Transaction volume
        count(distinct invoice)         as total_orders,
        count(distinct customer_id)     as active_customers,
        -- active_customers: how many unique customers placed an order this month

        -- Revenue
        round(sum(line_revenue), 2)     as total_revenue,
        round(avg(line_revenue), 2)     as avg_line_revenue,

        -- Units
        sum(quantity)                   as total_units_sold

    from transactions
    group by invoice_year, invoice_month

),

with_mom_growth as (

    select
        *,

        -- Month-over-month revenue growth percentage
        -- lag() looks at the previous row's value — in this case, last month's revenue
        -- We partition by nothing because we want the global previous month
        -- ORDER BY invoice_year, invoice_month ensures correct chronological order
        lag(total_revenue) over (
            order by invoice_year, invoice_month
        ) as prev_month_revenue,

        -- Calculate the percentage change from the previous month
        round(
            (total_revenue - lag(total_revenue) over (order by invoice_year, invoice_month))
            / nullif(lag(total_revenue) over (order by invoice_year, invoice_month), 0)
            * 100,
            2
        ) as mom_revenue_growth_pct
        -- mom = month over month
        -- The formula: (current - previous) / previous * 100
        -- nullif prevents division by zero if the previous month had zero revenue
        -- round(..., 2) rounds to 2 decimal places

    from monthly_aggregates

)

select * from with_mom_growth
order by invoice_year, invoice_month