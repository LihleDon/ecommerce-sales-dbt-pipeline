-- stg_transactions.sql
-- Staging model: sits directly on top of the raw_transactions table in DuckDB
-- Purpose: standardise types, rename nothing (names are already clean from transform.py),
--          and add a surrogate key so every row has a unique identifier
-- Materialised as: view (defined in dbt_project.yml)

with source as (

    -- Reference the raw table using dbt's source() function
    -- We will define this source in a sources.yml file next
    -- For now, we reference it directly by table name
    select * from raw_transactions

),

staged as (

    select
        -- Surrogate key: a unique identifier for each transaction row
        -- We concatenate invoice and stockcode because no single column is unique per row
        -- A customer can buy the same product in the same invoice more than once
        -- so we also include row_number() to handle that edge case
        invoice || '-' || stockcode || '-' || cast(row_number() over () as varchar) as transaction_id,

        -- Invoice and product identifiers
        invoice,
        stockcode,
        description,

        -- Customer and geography
        customer_id,
        country,

        -- Transaction timing
        invoicedate,
        invoice_year,
        invoice_month,

        -- Financials
        quantity,
        price,
        line_revenue

    from source

)

select * from staged