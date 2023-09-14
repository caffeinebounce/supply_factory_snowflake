use schema extensiv;
CREATE OR REPLACE PROCEDURE extensiv.update_transaction_register()
RETURNS FLOAT
LANGUAGE JAVASCRIPT
AS
$$
    var count_before_command = `SELECT COUNT(*) FROM extensiv.transaction_register;`;
    var check_new_rows_command = `
        select count(*)
        from (
            select lower(to_varchar(sha2(_file|| '-' || _line, 256))) as new_row_id
            from pc_fivetran_db.s2s_extensiv.transaction_register
        ) as new_rows
        where not exists (
            select 1
            from extensiv.transaction_register tr
            where tr._row_id = new_rows.new_row_id
        );
    `;

    // Execute the new row check
    var check_new_rows_stmt = snowflake.createStatement({sqlText: check_new_rows_command});
    var check_new_rows_result = check_new_rows_stmt.execute();
    check_new_rows_result.next();
    var new_rows_count = check_new_rows_result.getColumnValue(1);

    if (new_rows_count > 0) {
        var update_command = `update extensiv.transaction_register set latest = false;`;
        var update_stmt = snowflake.createStatement({sqlText: update_command});
        update_stmt.execute();
    }

    // Step 1: Create Temporary Table for Date Conversions
    var create_temp_table_command = `
        create or replace temporary table temp_date_conversion as
        with txr_raw as (
            select *,
                date as date_col
            from pc_fivetran_db.s2s_extensiv.transaction_register
            where transaction_id is not null
        )
        
        select 
            transaction_id as id,
            case 
                when date_col regexp '\\d{1,2}/\\d{1,2}/\\d{2}$' then 
                    to_date(concat('20', right(date_col, 2), '-', substring(date_col, 1, 2), '-', substring(date_col, 4, 2)), 'yyyy-mm-dd')
                when date_col regexp '\\d{1,2}/\\d{1,2}/\\d{4}$' then 
                    to_date(date_col, 'mm/dd/yyyy')
                else null
            end as transaction_date,
            case
                when replace(ship_date, '- CAN', '') regexp '\\d{1,2}/\\d{1,2}/\\d{2}$' then 
                    to_date(concat('20', right(replace(ship_date, '- CAN', ''), 2), '-', substring(replace(ship_date, '- CAN', ''), 1, 2), '-', substring(replace(ship_date, '- CAN', ''), 4, 2)), 'yyyy-mm-dd')
                else 
                    to_date(replace(ship_date, '- CAN', ''), 'mm/dd/yyyy')
            end as ship_date
        from txr_raw;
    `;
    var create_temp_table_stmt = snowflake.createStatement({sqlText: create_temp_table_command});
    create_temp_table_stmt.execute();

    var insert_command = `
        insert into extensiv.transaction_register (
            id, 
            brand_name, 
            transaction_date,
            po_number,
            period_first_day,
            period_last_day, 
            ship_date, 
            cancelled,
            carrier, 
            customer_ref, 
            retail,
            tracking_num, 
            shipping_recipient,
            quantity_in, 
            quantity_out, 
            charge_handling, 
            charge_materials, 
            charge_storage, 
            charge_special,
            charge_freight, 
            charge_total, 
            notes, 
            latest,
            _row_id,
            _modified,
            _file,
            _fivetran_synced
        )

        with txn_lines as (
            select * 
            from prod_sf_tables.extensiv.transaction_lines
        ),

        txr_raw as (
            select *,
                date as date_col
            from pc_fivetran_db.s2s_extensiv.transaction_register
            where transaction_id is not null
        ), 

        parse as (
            select
                transaction_id as id,
                case 
                    when date_col REGEXP '\\d{1,2}/\\d{1,2}/\\d{2}$' then 
                        to_date(concat('20', right(date_col, 2), '-', substring(date_col, 1, 2), '-', substring(date_col, 4, 2)), 'yyyy-mm-dd')
                    when date_col REGEXP '\\d{1,2}/\\d{1,2}/\\d{4}$' then 
                        to_date(date_col, 'mm/dd/yyyy')
                    else null
                end as transaction_date,
                case
                    when replace(ship_date, '- CAN', '') regexp '\\d{1,2}/\\d{1,2}/\\d{2}$' then 
                        to_date(concat('20', right(replace(ship_date, '- CAN', ''), 2), '-', substring(replace(ship_date, '- CAN', ''), 1, 2), '-', substring(replace(ship_date, '- CAN', ''), 4, 2)), 'yyyy-mm-dd')
                    else 
                        to_date(replace(ship_date, '- CAN', ''), 'mm/dd/yyyy')
                end as ship_date
            from txr_raw
        ),

        tl_aggregated as (
            select 
                transaction_id, 
                array_agg(po_number)[1] as po_number
            from txn_lines
            group by transaction_id
        ),

        parse_and_tl as (
            select 
                p.id,
                cast(p.transaction_date as date) as transaction_date,
                cast(p.ship_date as date) as ship_date,
                tl.po_number
            from parse p
            left join tl_aggregated tl on p.id = tl.transaction_id
            group by p.id, p.transaction_date, p.ship_date, tl.po_number
        ),

        final_txr as (
            select
                ptl.id as id,
                t.name as brand_name,
                ptl.transaction_date as transaction_date,
                ptl.po_number as po_number,
                date_trunc('MONTH', cast(ptl.transaction_date as date)) as period_first_day,
                last_day(cast(ptl.transaction_date as date)) as period_last_day,
                ptl.ship_date as ship_date,
                case
                    when t.ship_date like '%-CAN%' or t.ship_date like '%- CAN%' then true
                    else false
                end as cancelled,
                t.carrier as carrier,
                t.customer_ref_ as customer_ref,
                case when ptl.po_number is null then false else true end as retail,
                t.tracking_number as tracking_num,
                t.ship_to_company as shipping_recipient,
                try_to_number(t.qty_in) as quantity_in,
                try_to_number(t.qty_out) as quantity_out,
                try_cast(t.handling as double) as charge_handling,
                try_cast(t.materials as double) as charge_materials,
                try_cast(t.storage as double) as charge_storage,
                try_cast(t.special as double) as charge_special,
                try_cast(t.freight_pp as double) as charge_freight,
                try_cast(t.total as double) as charge_total,
                t.notes as notes,
                true as latest,
                lower(to_varchar(sha2(_file || '-' || _line, 256))) as _row_id,
                _modified as _modified,
                _file as _file,
                t._fivetran_synced as _fivetran_synced
            from parse_and_tl ptl
            left join txr_raw t on ptl.id = t.transaction_id
        )

        select
            tdc.id, 
            ft.brand_name, 
            tdc.transaction_date,
            ft.po_number,
            ft.period_first_day,
            ft.period_last_day, 
            tdc.ship_date, 
            ft.cancelled,
            ft.carrier, 
            ft.customer_ref, 
            ft.retail,
            ft.tracking_num, 
            ft.shipping_recipient,
            ft.quantity_in, 
            ft.quantity_out, 
            ft.charge_handling, 
            ft.charge_materials, 
            ft.charge_storage, 
            ft.charge_special,
            ft.charge_freight, 
            ft.charge_total, 
            ft.notes, 
            ft.latest,
            ft._row_id,
            ft._modified,
            ft._file,
            ft._fivetran_synced
        from final_txr ft
        left join temp_date_conversion tdc on ft.id = tdc.id
        where not exists (
            select 1
            from extensiv.transaction_register tr
            where tr._row_id = ft._row_id
        );
    `;
    var count_after_command = `SELECT COUNT(*) FROM extensiv.transaction_register;`;

    var count_before_stmt = snowflake.createStatement({sqlText: count_before_command});
    var insert_stmt = snowflake.createStatement({sqlText: insert_command});
    var count_after_stmt = snowflake.createStatement({sqlText: count_after_command});

    var count_before_result = count_before_stmt.execute();
    count_before_result.next();
    var count_before = count_before_result.getColumnValue(1);

    // Insert new rows with 'latest = true'
    insert_stmt.execute();

    var count_after_result = count_after_stmt.execute();
    count_after_result.next();
    var count_after = count_after_result.getColumnValue(1);

    return count_after - count_before;
$$;

CREATE OR REPLACE PROCEDURE extensiv.update_transaction_register()
RETURNS FLOAT
LANGUAGE JAVASCRIPT
AS
$$
    var create_temp_table_stmt = snowflake.createStatement({sqlText: create_temp_table_command});
    create_temp_table_stmt.execute();

    var insert_command = `
        insert into extensiv.transaction_register (
            id, 
            brand_name, 
            transaction_date,
            po_number,
            period_first_day,
            period_last_day, 
            ship_date, 
            cancelled,
            carrier, 
            customer_ref, 
            retail,
            tracking_num, 
            shipping_recipient,
            quantity_in, 
            quantity_out, 
            charge_handling, 
            charge_materials, 
            charge_storage, 
            charge_special,
            charge_freight, 
            charge_total, 
            notes, 
            latest,
            _row_id,
            _modified,
            _file,
            _fivetran_synced
        )

        with txn_lines as (
            select * 
            from prod_sf_tables.extensiv.transaction_lines
        ),

        txr_raw as (
            select *,
                date as date_col
            from pc_fivetran_db.s2s_extensiv.transaction_register
            where transaction_id is not null
        ), 

        parse as (
            select
                transaction_id as id,
                case 
                    when date_col REGEXP '\\d{1,2}/\\d{1,2}/\\d{2}$' then 
                        to_date(concat('20', right(date_col, 2), '-', substring(date_col, 1, 2), '-', substring(date_col, 4, 2)), 'yyyy-mm-dd')
                    when date_col REGEXP '\\d{1,2}/\\d{1,2}/\\d{4}$' then 
                        to_date(date_col, 'mm/dd/yyyy')
                    else null
                end as transaction_date,
                case
                    when replace(ship_date, '- CAN', '') regexp '\\d{1,2}/\\d{1,2}/\\d{2}$' then 
                        to_date(concat('20', right(replace(ship_date, '- CAN', ''), 2), '-', substring(replace(ship_date, '- CAN', ''), 1, 2), '-', substring(replace(ship_date, '- CAN', ''), 4, 2)), 'yyyy-mm-dd')
                    else 
                        to_date(replace(ship_date, '- CAN', ''), 'mm/dd/yyyy')
                end as ship_date
            from txr_raw
        ),

        tl_aggregated as (
            select 
                transaction_id, 
                array_agg(po_number)[1] as po_number
            from txn_lines
            group by transaction_id
        ),

        parse_and_tl as (
            select 
                p.id,
                cast(p.transaction_date as date) as transaction_date,
                cast(p.ship_date as date) as ship_date,
                tl.po_number
            from parse p
            left join tl_aggregated tl on p.id = tl.transaction_id
            group by p.id, p.transaction_date, p.ship_date, tl.po_number
        ),

        final_txr as (
            select
                ptl.id as id,
                t.name as brand_name,
                ptl.transaction_date as transaction_date,
                ptl.po_number as po_number,
                date_trunc('MONTH', cast(ptl.transaction_date as date)) as period_first_day,
                last_day(cast(ptl.transaction_date as date)) as period_last_day,
                ptl.ship_date as ship_date,
                case
                    when t.ship_date like '%-CAN%' or t.ship_date like '%- CAN%' then true
                    else false
                end as cancelled,
                t.carrier as carrier,
                t.customer_ref_ as customer_ref,
                case when ptl.po_number is null then false else true end as retail,
                t.tracking_number as tracking_num,
                t.ship_to_company as shipping_recipient,
                try_to_number(t.qty_in) as quantity_in,
                try_to_number(t.qty_out) as quantity_out,
                try_cast(t.handling as double) as charge_handling,
                try_cast(t.materials as double) as charge_materials,
                try_cast(t.storage as double) as charge_storage,
                try_cast(t.special as double) as charge_special,
                try_cast(t.freight_pp as double) as charge_freight,
                try_cast(t.total as double) as charge_total,
                t.notes as notes,
                true as latest,
                lower(to_varchar(sha2(_file || '-' || _line, 256))) as _row_id,
                _modified as _modified,
                _file as _file,
                t._fivetran_synced as _fivetran_synced
            from parse_and_tl ptl
            left join txr_raw t on ptl.id = t.transaction_id
        )

        select *
        from final_txr ft
        left join temp_date_conversion tdc on ft.id = tdc.id
        where not exists (
            select 1
            from extensiv.transaction_register tr
            where tr._row_id = ft._row_id
        );
    `;
    var count_after_command = `SELECT COUNT(*) FROM extensiv.transaction_register;`;

    var count_before_stmt = snowflake.createStatement({sqlText: count_before_command});
    var insert_stmt = snowflake.createStatement({sqlText: insert_command});
    var count_after_stmt = snowflake.createStatement({sqlText: count_after_command});

    var count_before_result = count_before_stmt.execute();
    count_before_result.next();
    var count_before = count_before_result.getColumnValue(1);

    // Insert new rows with 'latest = true'
    insert_stmt.execute();

    var count_after_result = count_after_stmt.execute();
    count_after_result.next();
    var count_after = count_after_result.getColumnValue(1);

    return count_after - count_before;
$$;

insert into extensiv.test (
            id, 
            brand_name, 
            transaction_date,
            ship_date 
        )

        with txn_lines as (
            select * 
            from prod_sf_tables.extensiv.transaction_lines
        ),

        txr_raw as (
            select *,
                date as date_col
            from pc_fivetran_db.s2s_extensiv.transaction_register
            where transaction_id is not null
        ), 

        parse as (
            select
                transaction_id as id,
                case 
                    when date_col REGEXP '\\d{1,2}/\\d{1,2}/\\d{2}$' then 
                        to_date(concat('20', right(date_col, 2), '-', substring(date_col, 1, 2), '-', substring(date_col, 4, 2)), 'yyyy-mm-dd')
                    when date_col REGEXP '\\d{1,2}/\\d{1,2}/\\d{4}$' then 
                        to_date(date_col, 'mm/dd/yyyy')
                    else null
                end as transaction_date,
                case
                    when replace(ship_date, '- CAN', '') regexp '\\d{1,2}/\\d{1,2}/\\d{2}$' then 
                        to_date(concat('20', right(replace(ship_date, '- CAN', ''), 2), '-', substring(replace(ship_date, '- CAN', ''), 1, 2), '-', substring(replace(ship_date, '- CAN', ''), 4, 2)), 'yyyy-mm-dd')
                    else 
                        to_date(replace(ship_date, '- CAN', ''), 'mm/dd/yyyy')
                end as ship_date
            from txr_raw
        ),

        tl_aggregated as (
            select 
                transaction_id, 
                array_agg(po_number)[1] as po_number
            from txn_lines
            group by transaction_id
        ),

        parse_and_tl as (
            select 
                p.id,
                cast(p.transaction_date as date) as transaction_date,
                cast(p.ship_date as date) as ship_date,
                tl.po_number
            from parse p
            left join tl_aggregated tl on p.id = tl.transaction_id
            group by p.id, p.transaction_date, p.ship_date, tl.po_number
        ),

        final_txr as (
            select
                ptl.id as id,
                t.name as brand_name,
                ptl.transaction_date as transaction_date,
                ptl.ship_date as ship_date,
        )

        select *
        from final_txr
        where not exists (
            select 1
            from extensiv.transaction_register tr
            where tr._row_id = ft._row_id
        );