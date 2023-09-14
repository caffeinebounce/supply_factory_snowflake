CREATE OR REPLACE VIEW daily_shop_report AS
SELECT
  ds.date,
  ds.day_of_week,
  ds.day_of_week_text,
  ds.is_weekday,
  ds.last_weekday_of_month,
  ds.week_first_day,
  ds.week_last_day,
  ds.month_first_day,
  ds.month_last_day,
  ds.month_text,
  ds.quarter_first_day,
  ds.quarter_last_day,
  ds.year,
  ds.month_number,
  ds.quarter_number,
  ds.month_year,
  ds.quarter_year,
  dsk.shop_id,
  dsk.currency,
  dsk.order_count,
  dsk.items_sold,
  dsk.items_refunded,
  dsk.avg_items_per_order,
  dsk.total_customers,
  dsk.gross_sales,
  dsk.total_discounts,
  dsk.refund_subtotal,
  dsk.refund_total_tax,
  dsk.shipping_cost,
  dsk.shipping_discount_amount,
  dsk.order_adjustment_amount,
  dsk.order_adjustment_tax_amount,
  dsk.discount_code_applied,
  dsk.count_orders_with_discounts,
  dsk.count_orders_with_refunds,
  dsk.variants_sold,
  dsk.products_sold,
  dsk.quantity_gift_cards_sold,
  dsk.quantity_requiring_shipping,
  dsk.count_abandoned_checkouts,
  dsk.count_customers_abandoned_checkout,
  dsk.count_fulfillment_attempted_delivery,
  dsk.count_fulfillment_delivered,
  dsk.count_fulfillment_failure,
  dsk.count_fulfillment_confirmed
FROM reporting.date_spine ds
JOIN daily_shop_key dsk
ON ds.date = dsk.date_day
ORDER BY
  ds.date;

CREATE OR REPLACE VIEW monthly_shopify_report AS
SELECT
  ds.year,
  ds.month_number,
  ds.month_text,
  ds.month_year,
  dsk.shop_id,
  dsk.currency,
  SUM(dsk.order_count) AS total_orders,
  SUM(dsk.items_sold) AS total_items_sold,
  SUM(dsk.items_refunded) AS total_items_refunded,
  ROUND(AVG(dsk.avg_items_per_order), 2) AS avg_items_per_order,
  SUM(dsk.total_customers) AS total_customers,
  ROUND(SUM(dsk.gross_sales), 2) AS gross_sales,
  ROUND(SUM(dsk.total_discounts), 2) AS total_discounts,
  ROUND(SUM(dsk.refund_subtotal), 2) AS total_refund_subtotal,
  ROUND(SUM(dsk.refund_total_tax), 2) AS total_refund_tax,
  ROUND(SUM(dsk.shipping_cost), 2) AS total_shipping_cost,
  ROUND(SUM(dsk.shipping_discount_amount), 2) AS total_shipping_discount,
  ROUND(SUM(dsk.order_adjustment_amount), 2) AS total_order_adjustment,
  ROUND(SUM(dsk.order_adjustment_tax_amount), 2) AS total_order_adjustment_tax,
  SUM(dsk.discount_code_applied) AS total_discount_codes_applied,
  SUM(dsk.count_orders_with_discounts) AS total_orders_with_discounts,
  SUM(dsk.count_orders_with_refunds) AS total_orders_with_refunds,
  SUM(dsk.variants_sold) AS total_variants_sold,
  SUM(dsk.products_sold) AS total_products_sold,
  SUM(dsk.quantity_gift_cards_sold) AS total_gift_cards_sold,
  SUM(dsk.quantity_requiring_shipping) AS total_items_requiring_shipping,
  SUM(dsk.count_abandoned_checkouts) AS total_abandoned_checkouts,
  SUM(dsk.count_customers_abandoned_checkout) AS total_customers_abandoned_checkout,
  SUM(dsk.count_fulfillment_attempted_delivery) AS total_fulfillment_attempted_delivery,
  SUM(dsk.count_fulfillment_delivered) AS total_fulfillment_delivered,
  SUM(dsk.count_fulfillment_failure) AS total_fulfillment_failure,
  SUM(dsk.count_fulfillment_confirmed) AS total_fulfillment_confirmed
FROM reporting.date_spine ds
JOIN daily_shop_key dsk
ON ds.date = dsk.date_day
GROUP BY
  ds.year,
  ds.month_number,
  ds.month_text,
  ds.month_year,
  dsk.shop_id,
  dsk.currency
ORDER BY
  ds.year,
  ds.month_number;

CREATE OR REPLACE PROCEDURE create_shopify_reports()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  var create_temp_daily_shop_key = `
    CREATE OR REPLACE VIEW daily_shop_key AS (
      SELECT
          date_day,
          shop_id,
          currency,
          count_orders as order_count,
          quantity_sold as items_sold,
          quantity_refunded as items_refunded,
          avg_line_item_count as avg_items_per_order,
          count_customers as total_customers,
          order_adjusted_total as gross_sales,
          total_discounts,
          refund_subtotal,
          refund_total_tax,
          shipping_cost,
          shipping_discount_amount,
          order_adjustment_amount,
          order_adjustment_tax_amount,
          count_discount_codes_applied as discount_code_applied,
          count_orders_with_discounts,
          count_orders_with_refunds,  
          count_variants_sold as variants_sold,
          count_products_sold as products_sold,
          quantity_gift_cards_sold,
          quantity_requiring_shipping,
          count_abandoned_checkouts,
          count_customers_abandoned_checkout,
          count_fulfillment_attempted_delivery,
          count_fulfillment_delivered,
          count_fulfillment_failure,
          count_fulfillment_confirmed
      FROM prod_sf_tables.shopify.daily_shop
      WHERE is_deleted = false
    );`;

  var create_daily_shop_report = `
    CREATE OR REPLACE VIEW daily_shop_report AS
    SELECT
      ds.date,
      ds.day_of_week,
      ds.day_of_week_text,
      ds.is_weekday,
      ds.last_weekday_of_month,
      ds.week_first_day,
      ds.week_last_day,
      ds.month_first_day,
      ds.month_last_day,
      ds.month_text,
      ds.quarter_first_day,
      ds.quarter_last_day,
      ds.year,
      ds.month_number,
      ds.quarter_number,
      ds.month_year,
      ds.quarter_year,
      dsk.shop_id,
      dsk.currency,
      dsk.order_count,
      dsk.items_sold,
      dsk.items_refunded,
      dsk.avg_items_per_order,
      dsk.total_customers,
      dsk.gross_sales,
      dsk.total_discounts,
      dsk.refund_subtotal,
      dsk.refund_total_tax,
      dsk.shipping_cost,
      dsk.shipping_discount_amount,
      dsk.order_adjustment_amount,
      dsk.order_adjustment_tax_amount,
      dsk.discount_code_applied,
      dsk.count_orders_with_discounts,
      dsk.count_orders_with_refunds,
      dsk.variants_sold,
      dsk.products_sold,
      dsk.quantity_gift_cards_sold,
      dsk.quantity_requiring_shipping,
      dsk.count_abandoned_checkouts,
      dsk.count_customers_abandoned_checkout,
      dsk.count_fulfillment_attempted_delivery,
      dsk.count_fulfillment_delivered,
      dsk.count_fulfillment_failure,
      dsk.count_fulfillment_confirmed
    FROM reporting.date_spine ds
    JOIN daily_shop_key dsk
    ON ds.date = dsk.date_day
    ORDER BY
      ds.date;`;

  var create_monthly_shopify_report = `
    CREATE OR REPLACE VIEW monthly_shopify_report AS
    SELECT
      ds.year,
      ds.month_number,
      ds.month_text,
      ds.month_year,
      dsk.shop_id,
      dsk.currency,
      SUM(dsk.order_count) AS total_orders,
      SUM(dsk.items_sold) AS total_items_sold,
      SUM(dsk.items_refunded) AS total_items_refunded,
      AVG(dsk.avg_items_per_order) AS avg_items_per_order,
      SUM(dsk.total_customers) AS total_customers,
      ROUND(SUM(dsk.gross_sales), 2) AS gross_sales,
      ROUND(SUM(dsk.total_discounts), 2) AS total_discounts,
      ROUND(SUM(dsk.refund_subtotal), 2) AS total_refund_subtotal,
      ROUND(SUM(dsk.refund_total_tax), 2) AS total_refund_tax,
      ROUND(SUM(dsk.shipping_cost), 2) AS total_shipping_cost,
      ROUND(SUM(dsk.shipping_discount_amount), 2) AS total_shipping_discount,
      ROUND(SUM(dsk.order_adjustment_amount), 2) AS total_order_adjustment,
      ROUND(SUM(dsk.order_adjustment_tax_amount), 2) AS total_order_adjustment_tax,
      SUM(dsk.discount_code_applied) AS total_discount_codes_applied,
      SUM(dsk.count_orders_with_discounts) AS total_orders_with_discounts,
      SUM(dsk.count_orders_with_refunds) AS total_orders_with_refunds,
      SUM(dsk.variants_sold) AS total_variants_sold,
      SUM(dsk.products_sold) AS total_products_sold,
      SUM(dsk.quantity_gift_cards_sold) AS total_gift_cards_sold,
      SUM(dsk.quantity_requiring_shipping) AS total_items_requiring_shipping,
      SUM(dsk.count_abandoned_checkouts) AS total_abandoned_checkouts,
      SUM(dsk.count_customers_abandoned_checkout) AS total_customers_abandoned_checkout,
      SUM(dsk.count_fulfillment_attempted_delivery) AS total_fulfillment_attempted_delivery,
      SUM(dsk.count_fulfillment_delivered) AS total_fulfillment_delivered,
      SUM(dsk.count_fulfillment_failure) AS total_fulfillment_failure,
      SUM(dsk.count_fulfillment_confirmed) AS total_fulfillment_confirmed
    FROM reporting.date_spine ds
    JOIN daily_shop_key dsk
    ON ds.date = dsk.date_day
    GROUP BY
      ds.year,
      ds.month_number,
      ds.month_text,
      ds.month_year,
      dsk.shop_id,
      dsk.currency
    ORDER BY
      ds.year,
      ds.month_number;`;

  try {
    snowflake.execute({sqlText: create_temp_daily_shop_key});
    snowflake.execute({sqlText: create_daily_shop_report});
    snowflake.execute({sqlText: create_monthly_shopify_report});
    return "Success: Created daily_shop_key temporary table and daily_shop_report and monthly_shopify_report views.";
  } catch (err) {
    return "Failed: " + err;
  }
$$;

create or replace task shopify_reports_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'
AS
CALL create_shopify_reports();

call create_shopify_reports();

alter task shopify_reports_task resume;

CREATE TEMPORARY TABLE tmp_daily_shop_key AS (
      SELECT
          date_day,
          shop_id,
          currency,
          count_orders as order_count,
          quantity_sold as items_sold,
          quantity_refunded as items_refunded,
          avg_line_item_count as avg_items_per_order,
          count_customers as total_customers,
          order_adjusted_total as gross_sales,
          total_discounts,
          refund_subtotal,
          refund_total_tax,
          shipping_cost,
          shipping_discount_amount,
          order_adjustment_amount,
          order_adjustment_tax_amount,
          count_discount_codes_applied as discount_code_applied,
          count_orders_with_discounts,
          count_orders_with_refunds,  
          count_variants_sold as variants_sold,
          count_products_sold as products_sold,
          quantity_gift_cards_sold,
          quantity_requiring_shipping,
          count_abandoned_checkouts,
          count_customers_abandoned_checkout,
          count_fulfillment_attempted_delivery,
          count_fulfillment_delivered,
          count_fulfillment_failure,
          count_fulfillment_confirmed
      FROM prod_sf_tables.shopify.daily_shop
      WHERE is_deleted = false
    );