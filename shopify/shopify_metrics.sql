CREATE OR REPLACE PROCEDURE shopify_metrics_report()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  var create_shopify_metrics_report = `
    CREATE OR REPLACE VIEW shopify_metrics_report AS
    SELECT
      ds.date,
      dsk.shop_id,
      dsk.currency,
      ROUND((SUM(dsk.gross_sales) OVER (PARTITION BY dsk.shop_id, dsk.currency ORDER BY ds.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) / NULLIF(SUM(dsk.order_count) OVER (PARTITION BY dsk.shop_id, dsk.currency ORDER BY ds.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 0)), 2) AS average_order_value_30_day,
      ROUND((SUM(dsk.items_sold) OVER (PARTITION BY dsk.shop_id, dsk.currency ORDER BY ds.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) / NULLIF(SUM(dsk.order_count) OVER (PARTITION BY dsk.shop_id, dsk.currency ORDER BY ds.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 0)), 2) AS items_per_order_30_day,
      ROUND((SUM(dsk.total_discounts) OVER (PARTITION BY dsk.shop_id, dsk.currency ORDER BY ds.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) / NULLIF(SUM(dsk.gross_sales) OVER (PARTITION BY dsk.shop_id, dsk.currency ORDER BY ds.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 0)), 6) AS discount_percentage_30_day,
      ROUND((SUM(dsk.refund_subtotal) OVER (PARTITION BY dsk.shop_id, dsk.currency ORDER BY ds.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) / NULLIF(SUM(dsk.gross_sales) OVER (PARTITION BY dsk.shop_id, dsk.currency ORDER BY ds.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 0)), 6) AS refund_percentage_30_day,
      ROUND((SUM(dsk.shipping_cost) OVER (PARTITION BY dsk.shop_id, dsk.currency ORDER BY ds.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) / NULLIF(SUM(dsk.order_count) OVER (PARTITION BY dsk.shop_id, dsk.currency ORDER BY ds.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 0)), 2) AS shipping_revenue_per_order_30_day
    FROM reporting.date_spine ds
    JOIN daily_shop_key dsk
    ON ds.date = dsk.date_day
    ORDER BY
      ds.date;`;

  try {
    snowflake.execute({sqlText: create_shopify_metrics_report});
    return "Success: Created shopify_metrics_report view.";
  } catch (err) {
    return "Failed: " + err;
  }
$$;

CREATE OR REPLACE PROCEDURE shopify_views_update_all()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  try {
    snowflake.execute({sqlText: "CALL create_shopify_reports()"});
    snowflake.execute({sqlText: "CALL shopify_metrics_report()"});
    return "Success: Updated all Shopify views.";
  } catch (err) {
    return "Failed: " + err;
  }
$$;

// create task to run the update all procedure
CREATE OR REPLACE TASK shopify_views_update_all_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'
as
  CALL shopify_views_update_all();

//enable task
ALTER TASK shopify_views_update_all_task RESUME;


