create schema sales;

create or replace view sales.shopify_ref_mapping as
with orders as (
    select * 
    from prod_sf_tables.shopify.orders
),
order_line as (
    select * 
    from prod_sf_tables.shopify.order_lines
),
shopify_products as (
    select *
    from prod_sf_tables.shopify.products
),
sku_ref as (
    select *
    from prod_sf_tables.sku_ref.master
),
sales_items as (
    select distinct
        ol.sku as sku,
        ol.name as shopify_name,
        ol.product_id as shopify_id
    from order_line ol
),
shop_ref_match as (
    select 
        si.shopify_id as shopify_id,
        si.shopify_name as shopify_name,
        sr.sku as ref_sku,
        sr.sku_name as ref_sku_name
    from sales_items si
    left join sku_ref.master sr on sr.sku = si.sku
)
select * from shop_ref_match;

create or replace table sales.sales_history_sku_d2c as
with orders as (
    select * 
    from prod_sf_tables.shopify.orders
),
order_line as (
    select * 
    from prod_sf_tables.shopify.order_lines
),
shopify_products as (
    select *
    from prod_sf_tables.shopify.products
),
sku_ref as (
    select *
    from prod_sf_tables.sku_ref.master
),
sales_items as (
    select distinct
        ol.sku as sku,
        ol.product_id as shopify_id
    from order_line ol
),
shop_ref_match as (
    select *
    from sales.shopify_ref_mapping
),
d2c_sales_history_sku as (
    select 
        ol.sku as sku,
        sr.sku_name as sku_name,
        to_date(o.processed_timestamp) as date_purchased,
        ol.quantity as quantity
    from orders o
    join order_line ol on o.order_id = ol.order_id
    join sku_ref sr on sr.sku = ol.sku
)
select * from d2c_sales_history_sku;
