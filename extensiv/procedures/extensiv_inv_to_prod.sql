create or replace function extensiv.final_inv()
returns table (
    hdc_sku string,
    sku string,
    upc string,
    misc_supply boolean,
    sku_name string,
    warehouse string,
    warehouse_location string,
    brand_name string,
    on_hand float,
    on_hold float,
    available float,
    uom string,
    dimension_units string,
    weight_units string,
    dim_quantity float,
    dim_uom string,
    packed float,
    cu_ft float,
    weight float,
    inv_run date,
    month_end boolean,
    month_end_date date,
    latest boolean,
    _row_id string,
    _modified timestamp_tz(9),
    _file string,
    _fivetran_synced timestamp_tz(9)
)
language sql
as '
    select
        sku as hdc_sku,
        case
            when sku like ''%SKU#:%'' then replace(split_part(split_part(sku, ''SKU#:'', 1), ''/'', 1), '' '', '''')
            when sku like ''.%'' then replace(right(sku, length(sku) - 1), ''-DELETE'', '''')
            when sku like ''%-DELETE'' then replace(sku, ''-DELETE'', '''')
            when sku like ''SKU#:%'' then split_part(split_part(sku, ''SKU#:'', 2), ''/'', 1)
            else sku
        end as sku,
        case
            when sku like ''%SKU#:%'' then split_part(split_part(sku, ''SKU#:'', 2), ''/'', 2)
            else null
        end as upc,
        case
            when sku like ''MISC SUPPLY%'' then true
            else false
        end as misc_supply,
        item_description as sku_name,
        warehouse,
        case
            when sku like ''%SKU#:%'' then split_part(split_part(sku, ''SKU#:'', 2), ''/'', 1)
            else null
        end as warehouse_location,
        customer as brand_name,
        try_to_double(replace(detail_on_hand, '','', '''')) as on_hand,
        try_to_double(replace(textbox_39, '','', '''')) as on_hold,
        try_to_double(replace(detail_available, '','', '''')) as available,
        textbox_15 as uom,
        textbox_18 as dimension_units,
        textbox_42 as weight_units,
        try_to_double(replace(detail_cartons, '','', '''')) as dim_quantity,
        textbox_28 as dim_uom,
        try_to_double(replace(detail_packed, '','', '''')) as packed,
        try_to_double(replace(detail_cu_ft, '','', '''')) as cu_ft,
        try_to_double(replace(detail_weight, '','', '''')) as weight,
        to_date(try_to_timestamp(end_date, ''MM/DD/YYYY HH12:MI:SS AM'')) as inv_run,
        case
            when inv_run = last_day(inv_run) then true
            else false
        end as month_end,
        last_day(inv_run) as month_end_date,
        true as latest,
        lower(to_varchar(sha2(_file || ''-'' || _line, 256))) as _row_id,
        _modified,
        _file,
        _fivetran_synced
    from pc_fivetran_db.s2s_extensiv.inventory
    where customer is not null
';

CREATE OR REPLACE PROCEDURE extensiv.update_inventory()
RETURNS FLOAT
LANGUAGE JAVASCRIPT
as
$$
    // Pre-check count
    var count_before_command = `SELECT COUNT(*) FROM extensiv.inventory;`;
    var count_before_stmt = snowflake.createStatement({sqlText: count_before_command});
    var count_before_result = count_before_stmt.execute();
    count_before_result.next();
    var count_before = count_before_result.getColumnValue(1);

    // SQL to check how many new rows would be inserted
    var check_new_rows_command = `
        select count(*)
        from table(extensiv.final_inv()) f
        where not exists (
            select 1
            from extensiv.inventory i
            where i._row_id = f._row_id
        );
        `;

    var check_new_rows_stmt = snowflake.createStatement({sqlText: check_new_rows_command});
    var check_new_rows_result = check_new_rows_stmt.execute();
    check_new_rows_result.next();
    var new_row_count = check_new_rows_result.getColumnValue(1);

    if (new_row_count > 0) {
        // Set existing 'latest' to false
        var set_latest_to_false_command = `update extensiv.inventory set latest = false;`;
        var set_latest_to_false_stmt = snowflake.createStatement({sqlText: set_latest_to_false_command});
        set_latest_to_false_stmt.execute();

        // insert new rows
        var insert_command = `
            insert into extensiv.inventory (
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
            from table(extensiv.final_inv()) f
            where not exists (
                select 1
                from extensiv.inventory i
                where i._row_id = f._row_id
            );
        `;

    var insert_stmt = snowflake.createStatement({sqlText: insert_command});
        insert_stmt.execute();
    }

    // Post-check count
    var count_after_command = `SELECT COUNT(*) FROM extensiv.inventory;`;
    var count_after_stmt = snowflake.createStatement({sqlText: count_after_command});
    var count_after_result = count_after_stmt.execute();
    count_after_result.next();
    var count_after = count_after_result.getColumnValue(1);

    return count_after - count_before;
$$;


-- creating a task to schedule the above
CREATE TasK extensiv.update_inventory_task
  WAREHOUSE COMPUTE_WH
  SCHEDULE 'USING CRON 0 0 * * * UTC'
  as
    CALL extensiv.update_inventory();

    CALL extensiv.update_inventory();

