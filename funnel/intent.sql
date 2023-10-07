create or replace view
    basic_intent as
with
    dates as (
        select
            date
        from
            prod_sf_tables.reporting.date_spine
        where
            date <= current_date
            and date > '2019-12-31'
    ),
    discounts as (
        select
            *
        from
            pc_fivetran_db.shopify.discount_allocation
    ),
    shopify_orders as (
        select
            date_trunc('DAY', o.created_timestamp)::date as date,
            count(o.order_id) as count_orders,
            sum(total_line_items_price) as gross_sales,
            sum(
                case
                    when o.total_discounts > 0 then 1
                    else 0
                end
            ) as orders_with_discounts,
            sum(o.total_discounts) as total_discounts,
            sum(o.total_price) as net_sales,
            sum(o.total_discounts) / nullif(gross_sales, 0) as percent_discounts,
            sum(
                case
                    when o.refund_subtotal > 0 then 1
                    else 0
                end
            ) as orders_with_refunds,
            sum(o.refund_subtotal) as total_refunds
        from
            prod_sf_tables.shopify.orders o
        where
            is_confirmed = true
            and is_test_order = false
            and is_deleted = false
            and source_name != 'amazon-us'
        group by
            date_trunc('DAY', o.created_timestamp)::date
    ),
    daily_shop as (
        select
            *
        from
            prod_sf_tables.shopify.daily_shop
    ),
    ga4_events as (
        select
            *
        from
            pc_fivetran_db.ga4_s2s.engagement_events
    )
select
    d.date as date,
    so.gross_sales as gross_sales,
    so.count_orders as orders,
    so.orders_with_discounts as orders_with_discounts,
    so.orders_with_discounts / nullif(so.count_orders, 0) * 100 as discounted_order_ratio,
    so.total_discounts / nullif(so.gross_sales, 0) * 100 as discount_ratio,
    so.orders_with_refunds as orders_with_refunds,
    so.total_refunds / nullif(so.gross_sales, 0) * 100 as refund_ratio,
    so.orders_with_refunds / nullif(so.count_orders, 0) * 100 as order_refund_ratio,
    (so.count_orders + ds.count_abandoned_checkouts) as total_checkouts,
    ds.count_abandoned_checkouts as checkouts_abandoned,
    ds.count_abandoned_checkouts / (so.count_orders + ds.count_abandoned_checkouts) * 100 as abandoned_checkout_ratio,
    ds.count_customers_abandoned_checkout as customers_abandoned,
    evss.total_users as total_sessions,
    so.count_orders / evss.total_users * 100 as conversion_rate_sessions
from
    dates d
    left join shopify_orders so on d.date = so.date
    left join daily_shop ds on d.date = ds.date_day
    left join ga4_events evss on d.date = evss.date
    and evss.event_name = 'session_start';
