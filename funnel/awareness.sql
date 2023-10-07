create or replace view
    awareness.basic_awareness as
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
    google_ads as (
        select
            *
        from
            prod_sf_tables.google_ads.accounts
    ),
    facebook_ads as (
        select
            *
        from
            pc_fivetran_db.facebook_ads_s2s_facebook_ads.facebook_ads__account_report
    ),
    instagram as (
        select
            date,
            impressions,
            website_clicks as clicks
        from
            pc_fivetran_db.instagram_business_s2s.user_insights
    ),
    amazon_ads as (
        select
            date as date_day,
            sum(impressions) as impressions,
            sum(clicks) as clicks,
            sum(cost) as spend
        from
            pc_fivetran_db.s2s_amazo_ads.ad_group_level_report
        group by
            date
    )
select
    d.date as date,
    ga.impressions as google_ad_impressions,
    fb.impressions as facebook_ad_impressions,
    ig.impressions as instagram_impressions,
    az.impressions as amazon_impressions,
    ga.clicks as google_ad_clicks,
    fb.clicks as facebook_ad_clicks,
    ig.clicks as instagram_clicks,
    az.clicks as amazon_clicks,
    ga.spend as google_ad_spend,
    fb.spend as facebook_ad_spend,
    az.spend as amazon_ad_spend,
    1000 * ga.spend / nullif(ga.impressions, 0) as google_ad_cpm,
    1000 * fb.spend / nullif(fb.impressions, 0) as facebook_ad_cpm,
    1000 * az.spend / nullif(az.impressions, 0) as amazon_ad_cpm,
    ga.clicks / nullif(ga.impressions, 0) * 100 as google_ad_ctr,
    fb.clicks / nullif(fb.impressions, 0) * 100 as facebook_ad_ctr,
    ig.clicks / nullif(ig.impressions, 0) * 100 as instagram_ctr,
    az.clicks / nullif(az.impressions, 0) * 100 as amazon_ctr
from
    dates d
    left join google_ads ga on d.date = ga.date_day
    left join facebook_ads fb on d.date = fb.date_day
    left join instagram ig on d.date = ig.date
    left join amazon_ads az on d.date = az.date_day;

CREATE
OR REPLACE PROCEDURE refresh_impressions_clicks_spend () RETURNS STRING LANGUAGE JAVASCRIPT AS $$
var sql_command = `
    DELETE FROM impressions_clicks_spend;
    INSERT INTO impressions_clicks_spend SELECT * FROM q_impressions_clicks_spend;
`;
var statement = snowflake.createStatement({sqlText: sql_command});
statement.execute();
return 'Table refreshed successfully.';
$$;

CREATE TASK refresh_impressions_clicks_spend WAREHOUSE = compute_wh SCHEDULE = '720 minutes' AS
CALL refresh_impressions_clicks_spend ();

alter task refresh_impressions_clicks_spend resume;

CALL refresh_impressions_clicks_spend ();
