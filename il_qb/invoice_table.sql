use database fin_tables;
create or replace table invoices (
    invoice_id varchar(255),
    qb_id varchar(10),
    currency varchar(10),
    total_amount float,
    ship_date date,
    transaction_date date,
    due_date date,
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

with invoices_raw as (
    select *
    from pc_fivetran_db.quickbooks.invoice
),

customer_raw as (
    select *
    from pc_fivetran_db.quickbooks.customer
)

SELECT
    i.doc_number AS invoice_id,
    i.id AS qb_id,
    i.currency_id AS currency,
    i.total_amount AS total_amount,
    i.ship_date AS ship_date,
    i.transaction_date AS transaction_date,
    i.customer_id AS customer_id,
    c.fully_qualified_name AS customer,
    i.custom_purchase_order_number AS po_number,
    CASE 
        WHEN i.custom_purchase_order_number REGEXP '^[0-9]+$'
        THEN LPAD(LTRIM(i.custom_purchase_order_number, '0'), LENGTH(i.custom_purchase_order_number) - (LENGTH(i.custom_purchase_order_number) - LENGTH(LTRIM(i.custom_purchase_order_number, '0'))), ' ')
        ELSE i.custom_purchase_order_number
    END AS po_number_trim,
    i.custom_store_number AS store_number,
    i.custom_warehouse AS warehouse,
    i.private_note AS memo,
    i.billing_address_id AS billing_address_id,
    i.shipping_address_id AS shipping_address_id,
    i.created_at AS created_at,
    i.updated_at AS updated_at
FROM invoices_raw i
LEFT JOIN customer_raw c ON i.customer_id = c.id;

CREATE OR REPLACE PROCEDURE fin_tables.prod_qb.refresh_invoices()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS $$
  // Delete existing contents of the table
  var delete_command = `DELETE FROM fin_tables.prod_qb.invoices`;
  var delete_statement = snowflake.createStatement({sqlText: delete_command});
  delete_statement.execute();

  // Insert new data
  var sql_command = `
    INSERT INTO fin_tables.prod_qb.invoices (
        invoice_id,
        qb_id, 
        currency,
        total_amount, 
        ship_date, 
        transaction_date,
        due_date,
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

    with invoices_raw as (
        select *
        from pc_fivetran_db.quickbooks.invoice
    ),

    customer_raw as (
        select *
        from pc_fivetran_db.quickbooks.customer
    )

    SELECT
        i.doc_number AS invoice_id,
        i.id AS qb_id,
        i.currency_id AS currency,
        i.total_amount AS total_amount,
        i.ship_date AS ship_date,
        i.transaction_date AS transaction_date,
        i.due_date as due_date,
        i.customer_id AS customer_id,
        c.fully_qualified_name AS customer,
        i.custom_purchase_order_number AS po_number,
        CASE 
            WHEN i.custom_purchase_order_number REGEXP '^[0-9]+$'
            THEN LPAD(LTRIM(i.custom_purchase_order_number, '0'), LENGTH(i.custom_purchase_order_number) - (LENGTH(i.custom_purchase_order_number) - LENGTH(LTRIM(i.custom_purchase_order_number, '0'))), ' ')
            ELSE i.custom_purchase_order_number
        END AS po_number_trim,
        i.custom_store_number AS store_number,
        i.custom_warehouse AS warehouse,
        i.private_note AS memo,
        i.billing_address_id AS billing_address_id,
        i.shipping_address_id AS shipping_address_id,
        i.created_at AS created_at,
        i.updated_at AS updated_at
    FROM invoices_raw i
    LEFT JOIN customer_raw c ON i.customer_id = c.id;`;

  var statement = snowflake.createStatement({sqlText: sql_command});
  statement.execute();

  return 'Existing data deleted and new data inserted into invoices table successfully.';
$$;

create or replace task prod_qb.refresh_invoices_table
    warehouse = compute_wh
    schedule = 'using cron */15 * * * * America/New_York'
as
call refresh_invoices();


ALTER TASK prod_qb.REFRESH_INVOICES_TABLE
    RESUME;

CALL PROD_QB.REFRESH_INVOICES();