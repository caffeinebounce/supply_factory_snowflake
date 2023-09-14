call update_po_tracker_po_status();
call po_tracker.update_vendor_mapping();
call update_po_tracker();
call update_po_tracker_average_price();
call update_po_tracker_views();

CREATE OR REPLACE PROCEDURE update_all_po_tracker()
  RETURNS STRING
  LANGUAGE JAVASCRIPT
  AS
  $$
  try {
    // Call the stored procedures one by one
    snowflake.execute({sqlText: "CALL update_po_tracker_po_status();"});
    snowflake.execute({sqlText: "CALL po_tracker.update_vendor_mapping();"});
    snowflake.execute({sqlText: "CALL update_po_tracker();"});
    snowflake.execute({sqlText: "CALL update_po_tracker_average_price();"});
    snowflake.execute({sqlText: "CALL update_po_tracker_views();"});
    
    // Return a success message
    return "All procedures executed successfully.";
  } catch (error) {
    // Return an error message with the details
    return "Error executing procedures: " + error.message;
  }
  $$
;
;
RESUME WAREHOUSE COMPUTE_WH;
CALL update_all_po_tracker();

CREATE OR REPLACE TASK update_po_tracker_daily
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 3 * * * America/New_York'
    TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9'
    TIMESTAMP_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9'
    TIMEZONE = 'America/New_York'
AS
    CALL update_all_po_tracker();

ALTER TASK update_po_tracker_daily RESUME;