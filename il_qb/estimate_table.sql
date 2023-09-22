create or replace view
    FIN_TABLES.PROD_QB.ESTIMATES_VIEW (
        ESTIMATE_ID,
        QB_ID,
        TRANSACTION_STATUS,
        CURRENCY,
        TOTAL_AMOUNT,
        SHIP_DATE,
        TRANSACTION_DATE,
        CUSTOMER_ID,
        CUSTOMER,
        PO_NUMBER,
        PO_NUMBER_TRIM,
        STORE_NUMBER,
        WAREHOUSE,
        MEMO,
        BILLING_ADDRESS_ID,
        SHIPPING_ADDRESS_ID,
        CREATED_AT,
        UPDATED_AT
    ) as
with
    estimates_raw as (
        select
            *
        from
            pc_fivetran_db.quickbooks.estimate
    ),
    customer_raw as (
        select
            *
        from
            pc_fivetran_db.quickbooks.customer
    )
select
    e.doc_number as estimate_id,
    e.id as qb_id,
    e.transaction_status as transaction_status,
    e.currency_id as currency,
    e.total_amount as total_amount,
    e.ship_date as ship_date,
    e.transaction_date as transaction_date,
    e.customer_id as customer_id,
    c.fully_qualified_name as customer,
    e.custom_purchase_order_number as po_number,
    case
        when e.custom_purchase_order_number regexp '^[0-9]+$' then lpad(
            ltrim(e.custom_purchase_order_number, '0'),
            length(e.custom_purchase_order_number) - (
                length(e.custom_purchase_order_number) - length(ltrim(e.custom_purchase_order_number, '0'))
            ),
            ' '
        )
        else e.custom_purchase_order_number
    end as po_number_trim,
    e.custom_store_number as store_number,
    e.custom_warehouse as warehouse,
    e.private_note as memo,
    e.billing_address_id as billing_address_id,
    e.shipping_address_id as shipping_address_id,
    e.created_at as created_at,
    e.updated_at as updated_at
from
    estimates_raw e
    left join customer_raw c on e.customer_id = c.id;