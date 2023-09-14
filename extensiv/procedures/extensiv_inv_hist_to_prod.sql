insert into extensiv.inventory_history (
    hdc_sku,
    sku,
    upc,
    misc_supply,
    sku_name,
    warehouse,
    warehouse_location,
    brand_name,
    on_hand,
    on_hold,
    available,
    uom,
    dimension_units,
    weight_units,
    dim_quantity,
    dim_uom,
    packed,
    cu_ft,
    weight,
    inv_run,
    month_end,
    month_end_date,
    _row_id,
    _modified,
    _file,
    _fivetran_synced
)
with inv_hist_raw as (
    select *
    from pc_fivetran_db.s2s_extensiv.inventory_history
    where customer is not null
), 
inv_hist_transformations as (
    select
        sku as hdc_sku,
        case
            when sku like '%SKU#:%' then regexp_replace(replace((split_part(split_part(trim(replace(replace(sku, '-DELETE', ''), '.', '')), 'SKU#:', 1), '/', 1)), ' ', ''), '\\s', '')
            when left(sku, 1) = '.' then regexp_replace(replace(replace(replace(sku, '-DELETE', ''), '.', ''), ' ', ''), '\\s', '')
            when regexp_replace(replace(replace(replace(sku, '-DELETE', ''), '.', ''), ' ', ''), '\\s', '') <> '' then regexp_replace(replace(replace(replace(sku, '-DELETE', ''), '.', ''), ' ', ''), '\\s', '')
            else sku
        end as sku,
        case
            when sku like '%SKU#:%' and not regexp_replace(split_part(split_part(sku, 'SKU#:', 2), '/', 2), '\\s', '') = '' then regexp_replace(split_part(split_part(sku, 'SKU#:', 2), '/', 2), '\\s', '')
            when sku like '%SKU#:%' and regexp_replace(split_part(sku, 'SKU#:', 2), '\\s', '') not like '%___-__%' then regexp_replace(split_part(sku, 'SKU#:', 2), '\\s', '')
            else null
        end as upc,
        case
            when sku like 'MISC SUPPLY%' then true
            else false
        end as misc_supply,
        case
            when replace(item_description, 'DELETED', '') like '___-__%' then regexp_replace(replace(item_description, 'DELETED', ''), '^.{7}', '')
            else initcap(replace(item_description, 'DELETED', '')) 
        end as sku_name,
        split_part(replace(warehouse, 'Warehouse: ', ''), 'From', 1) as warehouse,
        case
            when qualifier is not null then qualifier
            when replace(item_description, 'DELETED', '') like '___-__ %'then left(replace(item_description, 'DELETED', ''), 6)
            when sku like '%SKU#:%' and sku like '%___-__%' then split_part(split_part(sku, 'SKU#:', 2), '/', 1)
            else null
        end as warehouse_location,
        'Sunday II Sunday' as brand_name,
        try_to_double(replace(detail_on_hand, ',', '')) as on_hand,
        try_to_double(replace(detail_quarantine_on_hold, ',', '')) as on_hold,
        try_to_double(replace(detail_allocated, ',', '')) as allocated,
        try_to_double(replace(detail_available, ',', '')) as available,
        detail_primary_uom as uom,
        volume_unit as dimension_units,
        weight_unit as weight_units,
        try_to_double(replace(detail_cartons, ',', '')) as dim_quantity,
        detail_dim_uom as dim_uom,
        try_to_double(replace(detail_packed, ',', '')) as packed,
        try_to_double(replace(detail_cu_ft, ',', '')) as cu_ft,
        try_to_double(replace(detail_weight, ',', '')) as weight,
        try_to_date(left(right(warehouse, 22), 10)) as inv_run,
        case
            when inv_run = last_day(inv_run) then true
            else false
        end as month_end,
        last_day(inv_run) as month_end_date,
        lower(to_varchar(sha2(_file|| '-' || _line, 256))) as _row_id, 
        _modified as _modified,
        _file as _file,
        _fivetran_synced
    from inv_hist_raw
),
final_inv as (
    select *
    from inv_hist_transformations
)
select
    hdc_sku,
    sku,
    upc,
    misc_supply,
    sku_name,
    warehouse,
    warehouse_location,
    brand_name,
    on_hand,
    on_hold,
    available,
    uom,
    dimension_units,
    weight_units,
    dim_quantity,
    dim_uom,
    packed,
    cu_ft,
    weight,
    inv_run,
    month_end,
    month_end_date,
    _row_id,
    _modified,
    _file,
    _fivetran_synced
from final_inv
where not exists (
    select 1
    from extensiv.inventory_history i
    where i._row_id = final_inv._row_id
);

CREATE OR REPLACE PROCEDURE extensiv.update_inventory_history()
RETURNS FLOAT
LANGUAGE JAVASCRIPT
as
$$
    var count_before_command = `SELECT COUNT(*) FROM extensiv.inventory_history;`;
    var insert_command = `
        insert into extensiv.inventory_history (
            hdc_sku,
            sku,
            upc,
            misc_supply,
            sku_name,
            warehouse,
            warehouse_location,
            brand_name,
            on_hand,
            on_hold,
            available,
            uom,
            dimension_units,
            weight_units,
            dim_quantity,
            dim_uom,
            packed,
            cu_ft,
            weight,
            inv_run,
            month_end,
            month_end_date,
            latest,
            _row_id,
            _modified,
            _file,
            _fivetran_synced
        )
        with inv_hist_raw as (
            select *
            from pc_fivetran_db.s2s_extensiv.inventory_history
            where customer is not null
        ), 
        inv_hist_transformations as (
            select
                sku as hdc_sku,
                case
                    when sku like '%SKU#:%' then split_part(split_part(trim(replace(replace(sku, '-DELETE', ''), '.', '')), 'SKU#:', 1), '/', 1)
                    when left(sku, 1) = '.' then trim(replace(replace(sku, '-DELETE', ''), '.', ''))
                    when trim(replace(replace(sku, '-DELETE', ''), '.', '')) <> '' then trim(replace(replace(sku, '-DELETE', ''), '.', ''))
                    else sku
                end as sku,
                case
                    when sku like '%SKU#:%' and not split_part(split_part(sku, 'SKU#:', 2), '/', 2) = '' then split_part(split_part(sku, 'SKU#:', 2), '/', 2)
                    when sku like '%SKU#:%' and split_part(sku, 'SKU#:', 2) not like '%___-__%' then split_part(sku, 'SKU#:', 2)
                    else null
                end as upc,
                case
                    when sku like 'MISC SUPPLY%' then true
                    else false
                end as misc_supply,
                case
                    when replace(item_description, 'DELETED', '') like '___-__%' then regexp_replace(replace(item_description, 'DELETED', ''), '^.{7}', '')
                    else initcap(replace(item_description, 'DELETED', '')) 
                end as sku_name,
                split_part(replace(warehouse, 'Warehouse: ', ''), 'From', 1) as warehouse,
                case
                    when qualifier is not null then qualifier
                    when replace(item_description, 'DELETED', '') like '___-__ %'then left(replace(item_description, 'DELETED', ''), 6)
                    when sku like '%SKU#:%' and sku like '%___-__%' then split_part(split_part(sku, 'SKU#:', 2), '/', 1)
                    else null
                end as warehouse_location,
                'Sunday II Sunday' as brand_name,
                try_to_double(replace(detail_on_hand, ',', '')) as on_hand,
                try_to_double(replace(detail_quarantine_on_hold, ',', '')) as on_hold,
                try_to_double(replace(detail_allocated, ',', '')) as allocated,
                try_to_double(replace(detail_available, ',', '')) as available,
                detail_primary_uom as uom,
                volume_unit as dimension_units,
                weight_unit as weight_units,
                try_to_double(replace(detail_cartons, ',', '')) as dim_quantity,
                detail_dim_uom as dim_uom,
                try_to_double(replace(detail_packed, ',', '')) as packed,
                try_to_double(replace(detail_cu_ft, ',', '')) as cu_ft,
                try_to_double(replace(detail_weight, ',', '')) as weight,
                try_to_date(left(right(warehouse, 22), 10)) as inv_run,
                case
                    when inv_run = last_day(inv_run) then true
                    else false
                end as month_end,
                last_day(inv_run) as month_end_date,
                case
                    when _modified = max(_modified) over () then true
                    else false
                end as latest,
                lower(to_varchar(sha2(_file|| '-' || _line, 256))) as _row_id, 
                _modified as _modified,
                _file as _file,
                _fivetran_synced
            from inv_hist_raw
        ),
        final_inv as (
            select *
            from inv_hist_transformations
        )
        select
            hdc_sku,
            sku,
            upc,
            misc_supply,
            sku_name,
            warehouse,
            warehouse_location,
            brand_name,
            on_hand,
            on_hold,
            available,
            uom,
            dimension_units,
            weight_units,
            dim_quantity,
            dim_uom,
            packed,
            cu_ft,
            weight,
            inv_run,
            month_end,
            month_end_date,
            latest,
            _row_id,
            _modified,
            _file,
            _fivetran_synced
        from final_inv
        where not exists (
            select 1
            from extensiv.inventory_history i
            where i._row_id = final_inv._row_id
        );
    `;

    var count_after_command = `SELECT COUNT(*) FROM extensiv.inventory_history;`;

    var count_before_stmt = snowflake.createStatement({sqlText: count_before_command});
    var insert_stmt = snowflake.createStatement({sqlText: insert_command});
    var count_after_stmt = snowflake.createStatement({sqlText: count_after_command});

    var count_before_result = count_before_stmt.execute();
    count_before_result.next();
    var count_before = count_before_result.getColumnValue(1);

    insert_stmt.execute();

    var count_after_result = count_after_stmt.execute();
    count_after_result.next();
    var count_after = count_after_result.getColumnValue(1);

    return count_after - count_before;
$$;


-- creating a task to schedule the above
CREATE TASK extensiv.update_inventory_task
  WAREHOUSE COMPUTE_WH
  SCHEDULE 'USING CRON 0 0 * * * UTC'
  as
    CALL extensiv.update_inventory();

UPDATE extensiv.inventory_history
SET sku = REGEXP_REPLACE(sku, '\\s', '')

UPDATE extensiv.inventory_history
SET sku = REGEXP_REPLACE(sku, '\\s', '');


create or replace view extensiv.inv_hist_sku_match as (
    select distinct 
        e.hdc_sku,
        e.sku as extensiv_sku,
        e.sku_name as extensiv_name,
        e.upc as extensiv_upc,
        case
            when sr1.sku is not null then true
            else false
        end as sku_matched,
        sr1.sku_name,
        case
            when sr2.sku is not null then true
            else false
        end as upc_matched
    from extensiv.inventory_history e
    left join sku_ref.master sr1 on sr1.sku = e.sku
    left join sku_ref.master sr2 on sr2.upc = e.upc 
);

select * from extensiv.inv_hist_sku_match ;