create or replace view sales.ref_mapping_qb as
with invoices as (
    select * 
    from pc_fivetran_db.quickbooks.invoice
),
invoice_lines as (
    select * 
    from pc_fivetran_db.quickbooks.invoice_line
),
qb_items as (
    select *
    from pc_fivetran_db.quickbooks.item
),
sku_ref as (
    select *
    from prod_sf_tables.sku_ref.master
),
ref_mapping_qb as (
    select 
        qi.id as qb_id,
        qi.name as qb_name,
        coalesce(sr1.sku, sr2.sku, sr3.sku) as ref_sku,
        coalesce(sr1.sku_name, sr2.sku_name, sr3.sku_name) as ref_sku_name
    from qb_items qi
    left join sku_ref.master sr1 on sr1.sku = qi.stock_keeping_unit
    left join sku_ref.master sr2 on sr2.upc = qi.stock_keeping_unit
    left join sku_ref.master sr3 on sr3.sku = qi.name
)
select * from ref_mapping_qb;

create or replace table sales.sales_history_sku_retail as;
with invoices as (
    select * 
    from pc_fivetran_db.quickbooks.invoice
),
invoice_lines as (
    select * 
    from pc_fivetran_db.quickbooks.invoice_line
)
select * from invoice_lines where sales_item_item_id is null;
qb_items as (
    select *
    from pc_fivetran_db.quickbooks.item
),
sku_ref as (
    select *
    from prod_sf_tables.sku_ref.master
),
retail_ref as (
    select * 
    from sales.ref_mapping_qb
),
sales_history_qb as (
    select
        il.sales_item_item_id as qb_id,
        qi.name as qb_sku_name,
        i.transaction_date as date_purchased,
        il.amount as quantity
    from invoices i
    left join invoice_lines il on i.id = il.invoice_id
    left join qb_items qi on il.sales_item_item_id = qi.id
),
sales_history_sku_retail as (
    select 
        rm.ref_sku, 
        s.qb_sku_name,
        coalesce(rm.ref_sku_name, s.qb_sku_name) as sku_name,
        s.date_purchased,
        s.quantity
    from sales_history_qb s
    left join retail_ref rm on rm.qb_id = s.qb_id
    where s.qb_id is not null
)
select * from sales_history_sku_retail;