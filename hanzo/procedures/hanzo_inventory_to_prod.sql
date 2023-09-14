insert into hanzo.inventory (
    report_date,
    latest,
    index,
    sku,
    sku_name,
    sku_description,
    upc,
    quantity_on_hand,
    quantity_allocated,
    quantity_available,
    eaches_per_carton,
    _row_id,
    _modified,
    _file,
    _fivetran_synced
)
with inventory as (
    select *
    from pc_fivetran_db.s2s_hanzo.inventory
),

inventory_transformation as (
    select
        to_date(left(replace(_file, '_hanzo__infinite_looks_inventory_report_229_', ''), '10'), 'yyyy-mm-dd') as report_date,
        _line as index,
        product_code as sku,
        product_description as sku_name,
        product_description_2 as sku_description,
        case 
            when upc_code = '000000000000' then null
            else upc_code
        end as upc,
        quantity_on_hand as quantity_on_hand,
        quantity_allocated as quantity_allocated,
        available_inv_oh as quantity_available,
        unit_case_eaches_carton as eaches_per_carton,
        lower(to_varchar(sha2(_file|| '-' || _line, 256))) as _row_id, 
        _modified as _modified,
        _file as _file,
        _fivetran_synced as _fivetran_synced
    from inventory
), 

final_inventory as (
    select
        *,
        case
            when report_date = max(report_date) over () then true
            else false
        end as latest
    from inventory_transformation
)
select 
    report_date,
    latest,
    index,
    sku,
    sku_name,
    sku_description,
    upc,
    quantity_on_hand,
    quantity_allocated,
    quantity_available,
    eaches_per_carton,
    _row_id,
    _modified,
    _file,
    _fivetran_synced
from final_inventory
where not exists (
    select 1
    from hanzo.inventory i
    where i._row_id = final_inventory._row_id
);

create or replace view hanzo.sku_match as (
    select distinct 
        h.sku as hanzo_sku,
        h.sku_name as hanzo_name,
        h.upc as hanzo_upc,
        case
            when sr1.sku is not null then true
            else false
        end as sku_matched,
        sr1.sku_name,
        case
            when sr2.sku is not null then true
            else false
        end as upc_matched
    from hanzo.inventory h
    left join sku_ref.master sr1 on sr1.sku = h.sku
    left join sku_ref.master sr2 on sr2.upc = h.upc 
)