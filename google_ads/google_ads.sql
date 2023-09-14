-- Pre-aggregate table for 'ads'
CREATE OR REPLACE TABLE ads_agg AS
SELECT ad_id,
       ad_group_id,
       SUM(impressions) AS total_impressions,
       SUM(clicks) AS total_clicks,
       SUM(spend) AS total_spend
FROM ads
GROUP BY ad_id, ad_group_id;

-- Pre-aggregate table for 'ad_stats'
CREATE OR REPLACE TABLE ad_stats_agg AS
SELECT ad_id,
       SUM(conversions) AS total_conversions,
       SUM(conversions_value) AS total_conversions_value
FROM pc_fivetran_db.google_ads.ad_stats
GROUP BY ad_id, ad_group_id;

-- Pre-aggregate table for 'ad_groups'
CREATE TABLE ad_groups_agg AS
SELECT ad_group_id,
       MAX(ad_group_name) AS ad_group_name
FROM ad_groups
GROUP BY ad_group_id;

CREATE OR REPLACE TABLE ads_summary AS
SELECT 
       ad_id,
       MAX(ad_name) AS ad_name,
       max(ad_type) as ad_type,
       ad_group_id,
       MAX(ad_group_name) AS ad_group_name, -- get the ad_group_name; it should be the same for all rows of a specific ad_id and ad_group_id
       max(source_final_urls) as final_url,
       SUM(impressions) AS total_impressions,
       SUM(clicks) AS total_clicks,
       SUM(spend) AS total_spend,
       (SUM(clicks) / NULLIF(SUM(impressions), 0)) * 100 AS CTR,
       SUM(spend) / NULLIF(SUM(clicks), 0) AS CPC,
       SUM(conversions) AS total_conversions,
       SUM(conversions_value) AS total_conversions_value,
       (SUM(conversions_value) / NULLIF(SUM(spend), 0)) AS ROAS,
       SUM(spend) / NULLIF(SUM(conversions), 0) AS CPCV
FROM ads_modified
GROUP BY ad_id, ad_group_id;

select * from ads_summary;

CREATE OR REPLACE TABLE ads_modified AS (
WITH stats_base AS (
    SELECT * 
    FROM pc_fivetran_db.google_ads_google_ads_source.stg_google_ads__ad_stats_tmp
),

stats AS (
    SELECT 
        customer_id AS account_id, 
        date AS date_day, 
        COALESCE(CAST(ad_group_id AS VARCHAR), SPLIT_PART(ad_group_base_ad_group, 'adGroups/', 2)) AS ad_group_id,
        ad_network_type,
        device,
        ad_id, 
        campaign_id, 
        clicks, 
        cost_micros / 1000000.0 AS spend, 
        impressions,
        conversions,
        conversions_value
    FROM stats_base
),

accounts AS (
    SELECT *
    FROM pc_fivetran_db.google_ads_google_ads_source.stg_google_ads__account_history
    WHERE is_most_recent_record = True
), 

campaigns AS (
    SELECT *
    FROM pc_fivetran_db.google_ads_google_ads_source.stg_google_ads__campaign_history
    WHERE is_most_recent_record = True
), 

ad_groups AS (
    SELECT *
    FROM pc_fivetran_db.google_ads_google_ads_source.stg_google_ads__ad_group_history
    WHERE is_most_recent_record = True
),

ads AS (
    SELECT *
    FROM pc_fivetran_db.google_ads_google_ads_source.stg_google_ads__ad_history
    WHERE is_most_recent_record = True
)

SELECT
    stats.date_day,
    accounts.account_name,
    accounts.account_id,
    accounts.currency_code,
    campaigns.campaign_name,
    campaigns.campaign_id,
    ad_groups.ad_group_name,
    ad_groups.ad_group_id,
    stats.ad_id,
    ads.ad_name,
    ads.ad_status,
    ads.ad_type,
    ads.display_url,
    ads.source_final_urls,
    SUM(stats.spend) AS spend,
    SUM(stats.clicks) AS clicks,
    SUM(stats.impressions) AS impressions,
    SUM(stats.conversions) AS conversions,
    SUM(stats.conversions_value) AS conversions_value
FROM stats
LEFT JOIN ads
    ON stats.ad_id = ads.ad_id
    AND stats.ad_group_id = ads.ad_group_id
LEFT JOIN ad_groups
    ON ads.ad_group_id = ad_groups.ad_group_id
LEFT JOIN campaigns
    ON ad_groups.campaign_id = campaigns.campaign_id
LEFT JOIN accounts
    ON campaigns.account_id = accounts.account_id
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
);
