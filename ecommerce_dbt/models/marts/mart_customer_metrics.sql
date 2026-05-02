-- mart_customer_metrics.sql
-- One row per customer
-- Answers: who are our best customers, how often do they buy, what is their total spend?
-- Materialised as: table (aggregates 779K rows — pre-computing saves query time)

with transactions as (

    -- Pull from the staging model, not directly from raw_transactions
    -- This is the dbt pattern: marts always build on staging, never on raw
    -- If staging logic changes, marts automatically inherit the fix
    select * from {{ ref('stg_transactions') }}
    -- {{ ref('stg_transactions') }} is dbt's Jinja template syntax
    -- ref() does two things:
    --   1. Resolves to the correct table/view name in the database
    --   2. Tells dbt that this model DEPENDS ON stg_transactions
    --      so dbt always builds stg_transactions first

),

customer_metrics as (

    select
        customer_id,

        -- Country: take the most frequently occurring country for this customer
        -- A customer might appear with different countries due to shipping addresses
        -- mode() is an aggregate function that returns the most common value
        mode() within group (order by country) as primary_country,

        -- Volume metrics
        count(distinct invoice)     as total_orders,
        -- count(distinct invoice): count unique invoice numbers, not rows
        -- A single invoice (order) contains multiple line items (rows)
        -- Without distinct, this would count line items, not orders

        count(*)                    as total_line_items,
        -- count(*): count every row for this customer — the total number of products ordered

        sum(quantity)               as total_units_purchased,
        -- sum of all quantities: how many individual items this customer bought in total

        -- Revenue metrics
        round(sum(line_revenue), 2)         as total_revenue,
        -- The customer's total spend across the entire period, rounded to 2 decimal places

        round(avg(line_revenue), 2)         as avg_line_revenue,
        -- Average revenue per line item — a measure of basket composition

        round(sum(line_revenue) / nullif(count(distinct invoice), 0), 2) as avg_order_value,
        -- Total revenue divided by number of orders = average order value (AOV)
        -- nullif(count(distinct invoice), 0) prevents division by zero
        -- nullif returns NULL if the second argument equals the first
        -- Dividing by NULL produces NULL instead of a crash

        -- Time metrics
        min(invoicedate)            as first_order_date,
        max(invoicedate)            as last_order_date,
        -- The date of the customer's first and most recent order

        datediff('day', min(invoicedate), max(invoicedate)) as customer_tenure_days
        -- datediff('day', start, end) calculates the number of days between two dates
        -- This tells us how long the customer has been active

    from transactions
    group by customer_id
    -- group by customer_id means: run all the aggregations above for each unique customer
    -- Every aggregate function (sum, count, avg, min, max) operates within that group

)

select * from customer_metrics
order by total_revenue desc
-- Order the final result by revenue descending so the highest-value customers appear first
-- This makes the table immediately useful when queried without adding an ORDER BY every time