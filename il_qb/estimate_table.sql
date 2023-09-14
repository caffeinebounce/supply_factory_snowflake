use database fin_tables;
use schema prod_qb;
create or replace table estimates (
    estimate_id varchar(10),
    qb_id varchar(10),
    transaction_status varchar(255),
    currency varchar(10),
    total_amount float,
    ship_date date,
    transaction_date date,
    customer_id varchar(255), 
    customer varchar(255),
    po_number varchar(255),
    po_number_trim varchar(255),
    store_number varchar(255),
    warehouse varchar(255),
    memo varchar(2048),
    shipping_address_id varchar(10),
    billing_address_id varchar(10),
    created_at timestamp_tz, 
    updated_at timestamp_tz
);

with estimates_raw as (
    select *
    from pc_fivetran_db.quickbooks.estimate
),

customer_raw as (
    select *
    from pc_fivetran_db.quickbooks.customer
)

SELECT
    e.doc_number AS estimate_id,
    e.id AS qb_id,
    e.transaction_status AS transaction_status,
    e.currency_id AS currency,
    e.total_amount AS total_amount,
    e.ship_date AS ship_date,
    e.transaction_date AS transaction_date,
    e.customer_id AS customer_id,
    c.fully_qualified_name AS customer,
    e.custom_purchase_order_number AS po_number,
    CASE 
        WHEN e.custom_purchase_order_number REGEXP '^[0-9]+$'
        THEN LPAD(LTRIM(e.custom_purchase_order_number, '0'), LENGTH(e.custom_purchase_order_number) - (LENGTH(e.custom_purchase_order_number) - LENGTH(LTRIM(e.custom_purchase_order_number, '0'))), ' ')
        ELSE e.custom_purchase_order_number
    END AS po_number_trim,
    e.custom_store_number AS store_number,
    e.custom_warehouse AS warehouse,
    e.private_note AS memo,
    e.billing_address_id AS billing_address_id,
    e.shipping_address_id AS shipping_address_id,
    e.created_at AS created_at,
    e.updated_at AS updated_at
FROM estimates_raw e
LEFT JOIN customer_raw c ON e.customer_id = c.id;

CREATE OR REPLACE PROCEDURE fin_tables.prod_qb.refresh_estimates()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS $$
  // Delete existing contents of the table
  var delete_command = `DELETE FROM fin_tables.prod_qb.estimates`;
  var delete_statement = snowflake.createStatement({sqlText: delete_command});
  delete_statement.execute();

  // Insert new data
  var sql_command = `
    INSERT INTO fin_tables.prod_qb.estimates (
        estimate_id,
        qb_id, 
        transaction_status, 
        currency,
        total_amount, 
        ship_date, 
        transaction_date,
        customer_id, 
        customer,
        po_number,
        po_number_trim, 
        store_number, 
        warehouse, 
        memo, 
        shipping_address_id, 
        billing_address_id, 
        created_at, 
        updated_at
    )

    WITH estimates_raw AS (
        SELECT *
        FROM pc_fivetran_db.quickbooks.estimate
    ),

    customer_raw AS (
        SELECT *
        FROM pc_fivetran_db.quickbooks.customer
    )
        
    SELECT
        e.doc_number AS estimate_id,
        e.id AS qb_id,
        e.transaction_status AS transaction_status,
        e.currency_id AS currency,
        e.total_amount as total_amount,
        e.ship_date AS ship_date,
        e.transaction_date AS transaction_date,
        e.customer_id AS customer_id,
        c.fully_qualified_name AS customer,
        e.custom_purchase_order_number AS po_number,
        CASE 
            WHEN e.custom_purchase_order_number REGEXP '^[0-9]+$'
            THEN LPAD(LTRIM(e.custom_purchase_order_number, '0'), LENGTH(e.custom_purchase_order_number) - (LENGTH(e.custom_purchase_order_number) - LENGTH(LTRIM(e.custom_purchase_order_number, '0'))), ' ')
            ELSE e.custom_purchase_order_number
        END AS po_number_trim,
        e.custom_store_number AS store_number,
        e.custom_warehouse AS warehouse,
        e.private_note AS memo,
        e.billing_address_id AS billing_address_id,
        e.shipping_address_id AS shipping_address_id,
        e.created_at AS created_at,
        e.updated_at AS updated_at
    FROM estimates_raw e
    LEFT JOIN customer_raw c ON e.customer_id = c.id`;

  var statement = snowflake.createStatement({sqlText: sql_command});
  statement.execute();

  return 'Existing data deleted and new data inserted into estimates table successfully.';
$$;

CREATE OR REPLACE TASK prod_qb.REFRESH_ESTIMATES_TABLE
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'using cron */15 * * * * America/New_York'
AS
CALL REFRESH_ESTIMATES();

ALTER TASK prod_qb.REFRESH_ESTIMATES_TABLE
    RESUME;

CALL prod_qb.REFRESH_ESTIMATES();