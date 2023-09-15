create or replace procedure extensiv.update_transaction_lines()
returns float
language javascript
as
$$
    // create temporary table with formatted data
    var create_temp_table_query = `
        create or replace temporary table temp_txn_lines as
        with txn_lines_raw as (
            select * 
            from pc_fivetran_db.s2s_extensiv.transaction_lines
            where qty_in is not null
        ),
        txn_lines as (
            select
                tl.qty_in as transaction_id,
                row_number() over (partition by tl.qty_in order by tl.textbox_12) - 1 as index,
                tl.name as brand_name,
                case 
                    when replace(ship_date, '- CAN','') REGEXP '\\d{1,2}/\\d{1,2}/\\d{2}$' THEN to_date(CONCAT('20', RIGHT(REPLACE(ship_date, '- CAN',''), 2), LEFT(REPLACE(ship_date, '- CAN',''), 2), SUBSTRING(REPLACE(ship_date, '- CAN',''), 4, 2)), 'yyyymmdd')
                    ELSE to_date(REPLACE(ship_date, '- CAN',''), 'mm/dd/yyyy')
                END AS ship_date,
                tl.textbox_15 as carrier,
                tl.transaction_id as customer_ref,
                try_cast(tl.textbox_14 as decimal(38, 0)) as tracking_num,
                tl.tracking_number as po_number,
                case
                    when left(tl.textbox_12, 1) = '.' then trim(replace(replace(tl.textbox_12, '-DELETE', ''), '.', ''))
                    else trim(replace(replace(tl.textbox_12, '-DELETE', ''), '.', ''))
                end as sku,
                tl.textbox_67 as warehouse,
                tl.materials as quantity,
                true as latest,
                lower(to_varchar(sha2(tl._file|| '-' || tl._line, 256))) as _row_id, 
                tl._modified as _modified,
                tl._file as _file,
                tl._fivetran_synced as _fivetran_synced
            from txn_lines_raw tl
        )
        select *
        from txn_lines
    `;
    stmt = snowflake.createStatement({ sqlText: create_temp_table_query });
    stmt.execute();

    // check for new rows
    var check_new_rows_query = `
        with not_exists as (
            select _row_id
            from temp_txn_lines
            where not exists (
                select 1
                from extensiv.transaction_lines tl
                where tl._row_id = temp_txn_lines._row_id
            )
        )
        select count(*) as new_row_count
        from not_exists
    `;
    stmt = snowflake.createStatement({ sqlText: check_new_rows_query });
    var result = stmt.execute();
    var new_row_count = 0;
    if (result.next()) {
        new_row_count = result.getColumnValue(1);
    }

    if (new_row_count > 0) {
        // set all 'latest' flags to false
        var set_existing_to_false_query = `
            update extensiv.transaction_lines
            set latest = false
            where _row_id in (
                select _row_id
                from temp_txn_lines
            )
        `;
        stmt = snowflake.createStatement({ sqlText: set_existing_to_false_query });
        stmt.execute();

        // insert new data
        var insert_new_data_query = `
            insert into extensiv.transaction_lines (
                transaction_id,
                index,
                brand_name,
                ship_date,
                carrier,
                customer_ref,
                tracking_num,
                po_number,
                sku,
                warehouse,
                quantity,
                latest,
                _row_id,
                _modified,
                _file,
                _fivetran_synced
            )
            select
                transaction_id,
                index,
                brand_name,
                ship_date,
                carrier,
                customer_ref,
                tracking_num,
                po_number,
                sku,
                warehouse,
                quantity,
                latest,
                _row_id,
                _modified,
                _file,
                _fivetran_synced
            from temp_txn_lines
            where not exists (
                select 1
                from extensiv.transaction_lines tl
                where tl._row_id = temp_txn_lines._row_id
            )
        `;
        stmt = snowflake.createStatement({ sqlText: insert_new_data_query });
        stmt.execute();

        return "updated extensiv.transaction_lines (" + new_row_count + ")";
    } else {
        return "no new rows to insert.";
    }
$$;