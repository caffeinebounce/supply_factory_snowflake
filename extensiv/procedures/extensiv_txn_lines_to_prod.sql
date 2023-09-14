use database prod_sf_tables;
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
);

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
),

final_txn_lines as (
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
    from txn_lines
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
from final_txn_lines;
where not exists (
    select 1
    from extensiv.transaction_lines tl
    where tl._row_id = final_txn_lines._row_id
)
;
CREATE OR REPLACE PROCEDURE extensiv.update_transaction_lines()
RETURNS FLOAT
LANGUAGE JAVASCRIPT
AS
$$
    var count_before_command = `SELECT COUNT(*) FROM extensiv.transaction_lines;`;

    var check_new_rows_command = `
        select count(*) 
        from (
            select lower(to_varchar(sha2(_file|| '-' || _line, 256))) as new_row_id 
            from pc_fivetran_db.s2s_extensiv.transaction_lines
        ) as new_rows
        where not exists (
            select 1
            from extensiv.transaction_lines tl
            where tl._row_id = new_rows.new_row_id
        );
    `;

    var check_new_rows_stmt = snowflake.createStatement({sqlText: check_new_rows_command});
    var check_new_rows_result = check_new_rows_stmt.execute();
    check_new_rows_result.next();
    var new_rows_count = check_new_rows_result.getColumnValue(1);

    if (new_rows_count > 0) {
        var update_command = `update extensiv.transaction_lines set latest = false;`;
        var update_stmt = snowflake.createStatement({sqlText: update_command});
        update_stmt.execute();
    }

    var insert_command = `
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
        ),

        final_txn_lines as (
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
            from txn_lines
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
        from final_txn_lines
        where not exists (
            select 1
            from extensiv.transaction_lines tl
            where tl._row_id = final_txn_lines._row_id
        )
        ;
    `;

    var count_after_command = `SELECT COUNT(*) FROM extensiv.transaction_lines;`;

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
