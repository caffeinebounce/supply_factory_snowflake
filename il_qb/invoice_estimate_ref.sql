create or replace table invoice_estimate_ref (
    invoice_id varchar(255),
    qb_invoice_id varchar(10),
    estimate_id varchar(255),
    qb_estimate_id varchar(10),
    customer varchar(255),
    customer_id varchar(255)
);

with invoices_raw as (
    select *
    from pc_fivetran_db.quickbooks.invoice
),

estimates_raw as (
    select *
    from pc_fivetran_db.quickbooks.estimate
),

invoice_estimate_ref as (
    select *
    from pc_fivetran_db.quickbooks.estimate_linked_txn
),

customer_raw as (
    select *
    from pc_fivetran_db.quickbooks.customer
)

SELECT
    i.doc_number AS invoice_id,
    i.id AS qb_invoice_id,
    e.doc_number AS estimate_id,
    ier.estimate_id AS qb_estimate_id,
    c.fully_qualified_name as customer,
    i.customer_id AS customer_id
FROM invoices_raw i
LEFT JOIN customer_raw c ON i.customer_id = c.id
LEFT JOIN invoice_estimate_ref ier ON i.id = ier.invoice_id
LEFT JOIN estimates_raw e ON ier.estimate_id = e.id
WHERE i.doc_number is not NULL;

CREATE OR REPLACE PROCEDURE fin_tables.prod_qb.refresh_invoice_estimate_ref()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS $$
  // Delete existing contents of the table
  var delete_command = `DELETE FROM fin_tables.prod_qb.invoice_estimate_ref`;
  var delete_statement = snowflake.createStatement({sqlText: delete_command});
  delete_statement.execute();

  // Insert new data
  var sql_command = `
    INSERT INTO fin_tables.prod_qb.invoice_estimate_ref (
        invoice_id,
        qb_invoice_id,
        estimate_id,
        qb_estimate_id,
        customer,
        customer_id
    )

    with invoices_raw as (
        select *
        from pc_fivetran_db.quickbooks.invoice
    ),

    estimates_raw as (
        select *
        from pc_fivetran_db.quickbooks.estimate
    ),

    invoice_estimate_ref as (
        select *
        from pc_fivetran_db.quickbooks.estimate_linked_txn
    ),

    customer_raw as (
        select *
        from pc_fivetran_db.quickbooks.customer
    )

    SELECT
        i.doc_number AS invoice_id,
        i.id AS qb_invoice_id,
        e.doc_number AS estimate_id,
        ier.estimate_id AS qb_estimate_id,
        c.fully_qualified_name as customer,
        i.customer_id AS customer_id
    FROM invoices_raw i
    LEFT JOIN customer_raw c ON i.customer_id = c.id
    LEFT JOIN invoice_estimate_ref ier ON i.id = ier.invoice_id
    LEFT JOIN estimates_raw e ON ier.estimate_id = e.id
    WHERE i.doc_number is not NULL;`;

  var statement = snowflake.createStatement({sqlText: sql_command});
  statement.execute();

  return 'Existing data deleted and new data inserted into invoice/estimate ref table successfully.';
$$;

CREATE OR REPLACE TASK REFRESH_INVOICE_ESTIMATE_REF_TABLE
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 9 * * * America/New_York'
AS
CALL REFRESH_INVOICES();

ALTER TASK REFRESH_INVOICES_TABLE
    RESUME;

CALL REFRESH_INVOICE_ESTIMATE_REF();