create or replace view boms_component_matrix as ( 
with components_and_groups as (
    select
        b.fg_sku as sku,
        b.fg_sku_name as sku_name, 
        lower(g.group_name) as component_group,
        b.component_name,
        b.component_sku,
        b.component_type
    from boms.boms b
    join sku_ref.groups g on b.component_type = g.group_id
),
component_pivot as (
    select
        sku,
        sku_name,
        max(case when component_type = 'FILL' then true end) as product_fill,
        max(case when component_type = 'BULK' then true end) as bulk,
        max(case when component_type = 'LAB' then true end) as label,
        max(case when component_type = 'BAG' then true end) as bag,
        max(case when component_type = 'BOX' then true end) as packaging_box,
        max(case when component_type = 'BTCP' then true end) as bottle_cap,
        max(case when component_type = 'BTL' then true end) as bottle,
        max(case when component_type = 'BTNZ' then true end) as bottle_nozzle,
        max(case when component_type = 'BTPI' then true end) as bottle_pipette,
        max(case when component_type = 'BTPM' then true end) as bottle_treatment_pump,
        max(case when component_type = 'BTTG' then true end) as bottle_trigger_pump,
        max(case when component_type = 'CAP' then true end) as cap,
        max(case when component_type = 'CHI' then true end) as chipper,
        max(case when component_type = 'CLP' then true end) as clip,
        max(case when component_type = 'CMB' then true end) as comb,
        max(case when component_type = 'FRG' then true end) as fragrance,
        max(case when component_type = 'HDB' then true end) as headband,
        max(case when component_type = 'INS' then true end) as "INSERT",
        max(case when component_type = 'JAR' then true end) as jar,
        max(case when component_type = 'LABOR' then true end) as labor,
        max(case when component_type = 'PUM' then true end) as pump,
        max(case when component_type = 'PUS' then true end) as pump_sprayer,
        max(case when component_type = 'SCR' then true end) as scrunchie,
        max(case when component_type = 'SHI' then true end) as shipper,
        max(case when component_type = 'TBE' then true end) as tube,
        max(case when component_type = 'TUB' then true end) as tub,
        max(case when component_type = 'TWL' then true end) as towel,
        max(case when component_type = 'UNC' then true end) as unit_carton,
        max(case when component_type = 'UPC' then true end) as upc_label
    from components_and_groups
    group by sku, sku_name
)
select * from component_pivot
);

