
use database prod_sf_tables;
CALL extensiv.update_inventory();
CALL extensiv.update_transaction_register();
CALL extensiv.update_transaction_lines();

CREATE OR REPLACE PROCEDURE extensiv.update_all()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  try {
    var transaction_lines_update = snowflake.execute({sqlText: "CALL prod_sf_tables.extensiv.update_transaction_lines();"});
    var transaction_lines_rows = transaction_lines_update.next() ? transaction_lines_update.getColumnValue(1) : 0;

    var transaction_register_update = snowflake.execute({sqlText: "CALL prod_sf_tables.extensiv.update_transaction_register();"});
    var transaction_register_rows = transaction_register_update.next() ? transaction_register_update.getColumnValue(1) : 0;

    var inventory_update = snowflake.execute({sqlText: "CALL prod_sf_tables.extensiv.update_inventory();"});
    var inventory_rows = inventory_update.next() ? inventory_update.getColumnValue(1) : 0;

    return "Rows inserted: Inventory (" + inventory_rows + "), Transaction Register (" + transaction_register_rows + "), Transaction Lines (" + transaction_lines_rows + ").";
  } catch (error) {
    return "Error occurred: " + error.message;
  }
$$;

CALL extensiv.update_all();

update extensiv.transaction_register
set latest = false; 

where _file = 'Tracking-2.csv';