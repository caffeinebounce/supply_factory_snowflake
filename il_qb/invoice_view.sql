create or replace view
    fin_tables.prod_qb.invoices_view as
with
    invoices_raw as (
        select
            *
        from
            pc_fivetran_db.quickbooks.invoice
    ),
    customer_raw as (
        select
            *
        from
            pc_fivetran_db.quickbooks.customer
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
        WHEN i.custom_purchase_order_number REGEXP '^[0-9]+$' THEN LPAD(
            LTRIM(i.custom_purchase_order_number, '0'),
            LENGTH(i.custom_purchase_order_number) - (
                LENGTH(i.custom_purchase_order_number) - LENGTH(LTRIM(i.custom_purchase_order_number, '0'))
            ),
            ' '
        )
        ELSE i.custom_purchase_order_number
    END AS po_number_trim,
    i.custom_store_number AS store_number,
    i.custom_warehouse AS warehouse,
    i.private_note AS memo,
    i.billing_address_id AS billing_address_id,
    i.shipping_address_id AS shipping_address_id,
    i.created_at AS created_at,
    i.updated_at AS updated_at
FROM
    invoices_raw i
    LEFT JOIN customer_raw c ON i.customer_id = c.id;