
create schema hanzo;
create or replace table hanzo.inventory (
    report_date DATE,
    latest BOOLEAN,
    index INT,
    sku VARCHAR(255),
    sku_name VARCHAR(50),
    sku_description VARCHAR(50),
    upc VARCHAR(255),
    quantity_on_hand INT,
    quantity_allocated INT,
    quantity_available INT,
    eaches_per_carton INT,
    _row_id VARCHAR(255),
    _modified TIMESTAMP_TZ(9),
    _file VARCHAR(255),
    _fivetran_synced TIMESTAMP_TZ(9)
);

create or replace table hanzo.shipment (
    report_date DATE,
    latest BOOLEAN,
    index INT,
    ship_date DATE,
    ship_id VARCHAR(50),
    ship_state VARCHAR(50),
    reference VARCHAR(255),
    po_number VARCHAR(255),
    bill_contact VARCHAR(255),
    carrier VARCHAR(255),
    carrier_description VARCHAR(255),
    unit_price FLOAT,
    hanzo_id VARCHAR(50),
    _row_id VARCHAR(255),
    _modified TIMESTAMP_TZ(9),
    _file VARCHAR(255),
    _fivetran_synced TIMESTAMP_TZ(9)
);