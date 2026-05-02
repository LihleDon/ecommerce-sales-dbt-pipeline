-- mart_product_metrics.sql
-- One row per product (stockcode)
-- Answers: which products sell the most, generate the most revenue, and have the widest reach?
-- Materialised as: table

with transactions as (

    select * from {{ ref('stg_transactions') }}

),

product_metrics as (

    select
        stockcode,

        -- Use the most common description for this product
        -- The same stockcode can appear with slightly different descriptions
        -- due to manual data entry — mode() picks the most consistent one
        mode() within group (order by description) as product_description,

        -- Volume
        count(distinct invoice)         as times_ordered,
        -- How many different orders included this product

        count(distinct customer_id)     as unique_customers,
        -- How many different customers bought this product
        -- A product bought by many customers has wider appeal than one with one big buyer

        sum(quantity)                   as total_units_sold,
        -- Total physical units moved

        -- Revenue
        round(sum(line_revenue), 2)     as total_revenue,
        round(avg(price), 2)            as avg_unit_price,
        -- Average unit price across all transactions for this product
        -- Prices can vary slightly between transactions for the same product

        round(avg(line_revenue), 2)     as avg_line_revenue,

        -- Geography
        count(distinct country)         as countries_sold_in
        -- How many different countries ordered this product
        -- A high number indicates a globally popular item

    from transactions
    group by stockcode

)

select * from product_metrics
order by total_revenue desc