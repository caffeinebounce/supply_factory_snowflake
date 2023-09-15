create or replace procedure prod_sf_tables.extensiv.update_transaction_register()
returns float
language javascript
as
$$
    // create temporary table with formatted data
    var create_temp_table_query = `
        create or replace temporary table temp_txr as
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
        where not exists (
            select 1
            from extensiv.transaction_register tr
            where tr._row_id = ft._row_id
        );
    `;
    var stmt = snowflake.createStatement({ sqlText: create_temp_table_query });
    stmt.execute();

    // check for new rows
    var check_new_rows_query = `
        select count(*)
        from temp_txr
        where not exists (
            select 1
            from extensiv.transaction_register tr
            where tr._row_id = temp_txr._row_id
        )
    `;
    stmt = snowflake.createStatement({ sqlText: check_new_rows_query });
    var result = stmt.execute();
    var new_row_count = 0;
    if (result.next()) {
        new_row_count = result.getColumnValue(1);
    }

    if (new_row_count > 0) {
        // update existing 'latest' flags to false
        var update_existing_latest_query = `
            update extensiv.transaction_register
            set latest = false
        `;
        stmt = snowflake.createStatement({ sqlText: update_existing_latest_query });
        stmt.execute();

        // insert new data
        var insert_new_data_query = `
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
            select *
            from temp_txr
            where not exists (
                select 1
                from extensiv.transaction_register tr
                where tr._row_id = temp_txr._row_id
            )
        `;
        stmt = snowflake.createStatement({ sqlText: insert_new_data_query });
        stmt.execute();

        return "updated extensiv.transaction_register (" + new_row_count + ")";
    } else {
        return "no new rows to insert.";
    }
$$;