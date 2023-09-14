use database prod_sf_tables;
CREATE OR REPLACE PROCEDURE truecommerce.update_all()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  var mapping_update = snowflake.execute({sqlText: "CALL prod_sf_tables.truecommerce.update_qb_mapping();"});
  var txr_update = snowflake.execute({sqlText: "CALL truecommerce.update_transaction_register();"});
  var integration_update = snowflake.execute({sqlText: "CALL truecommerce.update_integration_table();"});

  var qb_mapping_rows = mapping_update.next() ? mapping_update.getColumnValue(1) : 0;
  var transaction_register_rows = txr_update.next() ? txr_update.getColumnValue(1) : 0;
  var integration_rows = integration_update.next() ? integration_update.getColumnValue(1) : 0;

  return "Rows inserted: QB Mapping (" + qb_mapping_rows + "), Transaction Register (" + transaction_register_rows + "), Integration (" + integration_rows + ").";
$$;

CALL truecommerce.update_all();

CREATE OR REPLACE TASK update_truecommerce_daily
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 5 12 * * * America/New_York'
    TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9'
    TIMESTAMP_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9'
    TIMEZONE = 'America/New_York'
AS
    CALL update_truecommerce_daily();

ALTER TASK update_truecommerce_daily RESUME;

CALL truecommerce.update_transaction_register();