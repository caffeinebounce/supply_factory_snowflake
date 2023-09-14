create or replace table sku_ref.groups (
    group_name varchar(255),
    product_type varchar(255),
    group_id varchar(50)
);
create or replace table sku_ref.master (
    sku varchar(255),
    upc varchar(255),
    brand varchar(255),
    product_name varchar(1024),
    status varchar(255),
    product_code varchar(50),
    product_group varchar(50),
    product_type varchar(50),
    class varchar(50),
    version varchar(50),
    vendor varchar(255),
    vendor_id varchar(50),
    country varchar(50),
    dimensions_units varchar(50),
    weight_units varchar(50),
    unit varchar(50),
    unit_width float,
    unit_height float,
    unit_length float,
    carton_height float,
    carton_width float,
    carton_length float,
    carton_weight float,
    eaches_per_carton int,
    notes varchar(1024),
    _fivetran_synced  timestamp_tz(9)
);

create or replace table sku_ref.retail_master (
    sku varchar(255),
    retailer varchar(255), 
    retailer_sku varchar(255),
    upc varchar(255),
    srp float,
    retail_discount float,
    retail_srp float
);