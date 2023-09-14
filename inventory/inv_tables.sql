create or replace table inventory.inventory_history (
    sku VARCHAR(255),
    sku_name VARCHAR(1024),
    upc VARCHAR(255),
    product_type VARCHAR(255),
    status VARCHAR(255),
    warehouse VARCHAR(255),
    date date,
    total_on_hand INT,
    total_on_hold INT,
    total_available INT,
    month_end boolean,
    _row_id VARCHAR(255)
);

create or replace table inventory.warehouse_map (
    warehouse_sku VARCHAR(255),
    sku_ref VARCHAR(255)
);
