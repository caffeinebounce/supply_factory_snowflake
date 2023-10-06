create schema desire;

create or replace view
    basic_desire as
with
    dates as (
        select
            date
        from
            prod_sf_tables.reporting.date_spine
        where
            date <= current_date
            and date > '2023-2-28'
    ),
    ecom_desire as (
        select
            *
        from
            pc_fivetran_db.ga4_s2s.ecom_item_brand
        where
            item_brand = 'SUNDAY II SUNDAY'
    ),
    ga4_events as (
        select
            *
        from
            pc_fivetran_db.ga4_s2s.engagement_events
    )
select
    d.date,
    evss.total_users as total_sessions,
    evue.total_users as total_engagements,
    evvi.total_users as total_product_views,
    evss.total_users * e.cart_to_view_rate as total_add_to_carts,
    e.items_added_to_cart,
    evue.total_users / evss.total_users * 100 as engagement_rate,
    evue.total_users / evss.total_users * 100 as non_engagement_rate,
    evvi.total_users / evss.total_users * 100 as item_view_rate,
    e.cart_to_view_rate * 100 as add_to_cart_rate
from
    dates d
    left join ecom_desire e on d.date = e.date
    left join ga4_events evue on d.date = evue.date
    and evue.event_name = 'user_engagement'
    left join ga4_events evss on d.date = evss.date
    and evss.event_name = 'session_start'
    left join ga4_events evvi on d.date = evvi.date
    and evvi.event_name = 'view_item';
