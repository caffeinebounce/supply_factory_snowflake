CREATE OR REPLACE VIEW
    kingman_orders AS
SELECT
    name AS order_id,
    total_line_items_price,
    total_discounts,
    total_price,
    total_tax,
    shipping_cost,
    created_timestamp AS order_datetime,
    referring_site,
    order_tags,
    order_url_tags,
    new_vs_repeat,
    CASE
        WHEN (
            order_url_tags ILIKE '%SB - RTG Sales (Main)%'
            OR order_url_tags ILIKE '%SB - TOF Sales Sandbox%'
        ) THEN TRUE
        ELSE FALSE
    END AS kingman_sale
FROM
    prod_sf_tables.shopify.orders
WHERE
    cancelled_timestamp IS NULL
    AND financial_status = 'paid'
    AND created_timestamp > '2023-09-01 00:00:00';
