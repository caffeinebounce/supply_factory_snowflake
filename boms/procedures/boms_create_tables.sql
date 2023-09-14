create or replace schema boms;
create or replace table boms.boms (
    component_sku varchar(255),
    component_name varchar(1024),
    is_fg boolean,
    usage float,
    fg_sku varchar(255),
    fg_sku_name varchar(1024),
    fg_upc varchar(255),
    fg_product_code varchar(50),
    fg_product_group varchar(50),
    fg_manufacturer varchar(255),
    fg_status varchar(50),
    component_index int,
    component_type varchar(50),
    component_version varchar(50),    
    component_unit varchar(10),
    component_vendor varchar(255),
    component_vendor_id varchar(50),
    index int,
    latest boolean,
    _fivetran_synced timestamp_tz(9)
);

create or replace table boms.sku (
    sku varchar(255),
    name varchar(1024),
    upc varchar(255),
    product_code varchar(50),
    product_group varchar(50),
    status varchar(50),
    total_cost float,
    latest boolean,
    last_updated timestamp_tz(9)
);

create or replace view boms.component_type_matches as
    select distinct
        b.component_type,
        g.group_id as product_type
    from prod_sf_tables.boms.boms b
    left join prod_sf_tables.sku_ref.groups g on b.component_type = g.group_id
    order by component_type;

