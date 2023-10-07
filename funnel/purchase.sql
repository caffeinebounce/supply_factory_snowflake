create or replace view
    basic_purchase as
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
            sum(
                case
                    when o.new_vs_repeat = 'new' then 1
                    else 0
                end
            ) as orders_from_new,
            sum(
                case
                    when o.new_vs_repeat = 'repeat' then 1
                    else 0
                end
            ) as orders_from_repeat,
            sum(total_line_items_price) as gross_sales,
            sum(
                case
                    when o.new_vs_repeat = 'new' then total_line_items_price
                    else 0
                end
            ) as gross_sales_new,
            sum(
                case
                    when o.new_vs_repeat = 'repeat' then total_line_items_price
                    else 0
                end
            ) as gross_sales_repeat,
            sum(
                case
                    when o.total_discounts > 0 then 1
                    else 0
                end
            ) as orders_with_discounts,
            sum(o.total_discounts) as total_discounts,
            sum(
                case
                    when o.new_vs_repeat = 'new' then o.total_discounts
                    else 0
                end
            ) as total_discounts_new,
            sum(
                case
                    when o.new_vs_repeat = 'repeat' then o.total_discounts
                    else 0
                end
            ) as total_discounts_repeat,
            sum(o.total_price) as net_sales,
            sum(
                case
                    when o.new_vs_repeat = 'new' then o.total_price
                    else 0
                end
            ) as net_sales_new,
            sum(
                case
                    when o.new_vs_repeat = 'repeat' then o.total_price
                    else 0
                end
            ) as net_sales_repeat,
            sum(o.total_discounts) / nullif(sum(total_line_items_price), 0) as percent_discounts,
            sum(
                case
                    when o.refund_subtotal > 0 then 1
                    else 0
                end
            ) as orders_with_refunds,
            sum(o.refund_subtotal) as total_refunds,
            sum(
                case
                    when o.new_vs_repeat = 'new' then o.refund_subtotal
                    else 0
                end
            ) as total_refunds_new,
            sum(
                case
                    when o.new_vs_repeat = 'repeat' then o.refund_subtotal
                    else 0
                end
            ) as total_refunds_repeat
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
    ),
    spend as (
        select
            *
        from
            funnel.awareness.basic_awareness
    )
select
    d.date as date,
    so.count_orders as orders,
    so.orders_from_new as orders_from_new,
    so.orders_from_repeat as orders_from_repeat,
    so.orders_from_new / nullif(so.count_orders, 0) * 100 as new_customer_ratio,
    so.net_sales as net_sales,
    so.net_sales_new as net_sales_new,
    so.net_sales_repeat as net_sales_repeat,
    so.net_sales / nullif(so.count_orders, 0) as AOV,
    coalesce(ba.google_ad_spend, 0) as google_ad_spend,
    coalesce(ba.facebook_ad_spend, 0) as facebook_ad_spend,
    coalesce(ba.google_ad_spend, 0) + coalesce(ba.facebook_ad_spend, 0) as total_paid_media_spend,
    (
        coalesce(ba.google_ad_spend, 0) + coalesce(ba.facebook_ad_spend, 0)
    ) / so.count_orders as blended_CPA
from
    dates d
    left join shopify_orders so on d.date = so.date
    left join daily_shop ds on d.date = ds.date_day
    left join ga4_events evss on d.date = evss.date
    and evss.event_name = 'session_start'
    left join spend ba on d.date = ba.date;