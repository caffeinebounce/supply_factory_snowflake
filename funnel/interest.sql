create or replace view
    interest.basic_interest as
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
    ga4_engagement as (
        select
            *
        from
            pc_fivetran_db.ga4_s2s.engagement_content
    ),
    ga4_events as (
        select
            *
        from
            pc_fivetran_db.ga4_s2s.engagement_events
    ),
    instagram as (
        select
            date_trunc('DAY', _fivetran_synced)::date as date,
            max(followers_count) as followers
        from
            pc_fivetran_db.instagram_business_s2s.user_history
        group by
            date
        order by
            date
    ),
    instagram_filled as (
        select
            a.date,
            last_value(b.followers ignore nulls) over (
                order by
                    a.date rows between unbounded preceding
                    and current row
            ) as followers_filled
        from
            dates a
            left join instagram b on a.date = b.date
    ),
    subscriber_status_changes as (
        select
            date_trunc('DAY', created_timestamp)::date as date,
            sum(
                case
                    when (
                        marketing_consent_state = 'unsubscribed'
                        or marketing_consent_state = 'not_subscribed'
                    )
                    and marketing_consent_updated_at > created_timestamp then 1
                    when marketing_consent_state = 'subscribed'
                    and marketing_consent_updated_at >= created_timestamp then 1
                    else 0
                end
            ) as new_subscribers,
            sum(
                case
                    when marketing_consent_state = 'unsubscribed' then 1
                    when marketing_consent_state = 'not_subscribed'
                    and marketing_consent_updated_at is not null then 1
                    else 0
                end
            ) as unsubscribers
        from
            prod_sf_tables.shopify.customers
        group by
            date
    ),
    daily_subscriber_list_size as (
        select
            date,
            SUM(new_subscribers) over (
                order by
                    date rows between unbounded preceding
                    and current row
            ) - SUM(unsubscribers) over (
                order by
                    date rows between unbounded preceding
                    and current row
            ) as list_size
        from
            subscriber_status_changes
    ),
    subscriber_changes as (
        select
            a.date,
            coalesce(
                b.list_size,
                lag(b.list_size) ignore nulls over (
                    order by
                        a.date
                )
            ) as daily_list_size,
            coalesce(new_subscribers, 0) as daily_new_subscribers,
            coalesce(unsubscribers, 0) as daily_unsubscribers,
            coalesce(new_subscribers, 0) - coalesce(unsubscribers, 0) as net_daily_additions
        from
            dates a
            left join subscriber_status_changes on a.date = subscriber_status_changes.date
            left join daily_subscriber_list_size b on a.date = b.date
        order by
            a.date
    )
select
    d.date,
    en.new_users,
    en.total_users,
    evss.event_count as session_starts,
    evfv.event_count as first_visits,
    en.screen_page_views as page_views,
    evsr.event_count as scrolls,
    evcl.event_count as clicks,
    (en.screen_page_views / en.total_users) as page_views_per_user,
    (evsr.event_count / evss.event_count) * 100 as scroll_rate_session,
    (evcl.event_count / evss.event_count) * 100 as click_rate_session,
    en.user_engagement_duration,
    (en.user_engagement_duration / session_starts) as avg_time_on_site_session,
    (en.user_engagement_duration / en.total_users) as avg_time_on_site_user,
    ig.followers_filled as instagram_followers,
    sc.daily_list_size as email_list,
    sc.daily_new_subscribers as email_new_suscribers,
    sc.daily_unsubscribers as email_unsuscribers,
    sc.net_daily_additions as email_net_additions
from
    dates d
    left join ga4_engagement en on d.date = en.date
    left join ga4_events evfv on d.date = evfv.date
    and evfv.event_name = 'first_visit'
    left join ga4_events evss on d.date = evss.date
    and evss.event_name = 'session_start'
    left join ga4_events evsr on d.date = evsr.date
    and evsr.event_name = 'scroll'
    left join ga4_events evcl on d.date = evcl.date
    and evcl.event_name = 'click'
    left join instagram_filled ig on d.date = ig.date
    left join subscriber_changes sc on d.date = sc.date
order by
    date;