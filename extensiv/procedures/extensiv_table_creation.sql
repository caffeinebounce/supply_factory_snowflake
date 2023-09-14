use database prod_sf_tables;
use schema extensiv;

-- creating the table for the transaction register. Used to create the table initially. DOES NOT NEED TO 
create or replace table extensiv.transaction_register (
    id VARCHAR(50),
    brand_name VARCHAR(255),
    transaction_date DATE,
    po_number VARCHAR(50),
    period_first_day DATE,
    period_last_day DATE,
    ship_date DATE,
    cancelled BOOLEAN,
    carrier VARCHAR(255),
    customer_ref VARCHAR(255),
    retail BOOLEAN,
    tracking_num VARCHAR(255),
    shipping_recipient VARCHAR(255),
    quantity_in INT,
    quantity_out INT,
    charge_handling FLOAT,
    charge_materials FLOAT,
    charge_storage FLOAT,
    charge_special FLOAT,
    charge_freight FLOAT,
    charge_total FLOAT,
    notes VARCHAR(1024),
    latest BOOLEAN,
    _row_id VARCHAR(255),
    _modified TIMESTAMP_TZ(9),
    _file VARCHAR(255),
    _fivetran_synced TIMESTAMP_TZ(9)
);

create or replace table extensiv.test (
    id VARCHAR(50),
    brand_name VARCHAR(255),
    transaction_date DATE,
    ship_date date
);

-- creating table for transaction lines
create or replace table extensiv.transaction_lines (
    transaction_id VARCHAR(50),
    index INT,
    brand_name VARCHAR(255),
    ship_date DATE,
    carrier VARCHAR(255),
    customer_ref VARCHAR(255),
    tracking_num VARCHAR(255),
    po_number VARCHAR(50),
    sku VARCHAR(255),
    warehouse VARCHAR(255),
    quantity INT,
    latest BOOLEAN,
    _row_id VARCHAR(255),
    _modified TIMESTAMP_TZ(9),
    _file VARCHAR(255),
    _fivetran_synced TIMESTAMP_TZ(9)
);

-- creating table for inventory report
create or replace table extensiv.inventory (
    hdc_sku VARCHAR(255),
    sku VARCHAR(255),
    upc VARCHAR(255),
    misc_supply BOOLEAN,
    sku_name VARCHAR(1024),
    warehouse VARCHAR(255),
    warehouse_location VARCHAR(255),
    brand_name VARCHAR(255),
    on_hand INT,
    on_hold INT,
    available INT,
    uom VARCHAR(255),
    dimension_units VARCHAR(50),
    weight_units VARCHAR(50),
    dim_quantity FLOAT,
    dim_uom VARCHAR(255),
    packed INT,
    cu_ft FLOAT,
    weight FLOAT,
    inv_run  TIMESTAMP_TZ(9),
    latest BOOLEAN,
    month_end BOOLEAN,
    month_end_date DATE,
    _row_id VARCHAR(255),
    _modified TIMESTAMP_TZ(9),
    _file VARCHAR(255),
    _fivetran_synced TIMESTAMP_TZ(9)
);

create or replace table extensiv.inventory_history (
    hdc_sku VARCHAR(255),
    sku VARCHAR(255),
    upc VARCHAR(255),
    misc_supply BOOLEAN,
    sku_name VARCHAR(1024),
    warehouse VARCHAR(255),
    warehouse_location VARCHAR(255),
    brand_name VARCHAR(255),
    on_hand INT,
    on_hold INT,
    available INT,
    uom VARCHAR(255),
    dimension_units VARCHAR(50),
    weight_units VARCHAR(50),
    dim_quantity FLOAT,
    dim_uom VARCHAR(255),
    packed INT,
    cu_ft FLOAT,
    weight FLOAT,
    inv_run  TIMESTAMP_TZ(9),
    month_end BOOLEAN,
    month_end_date DATE,
    _row_id VARCHAR(255),
    _modified TIMESTAMP_TZ(9),
    _file VARCHAR(255),
    _fivetran_synced TIMESTAMP_TZ(9)
)