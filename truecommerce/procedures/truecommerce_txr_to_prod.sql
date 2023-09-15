create or replace procedure truecommerce.update_transaction_register()
returns string
language javascript
as
$$
// create temporary table with data
var create_temp_table_query = `
    create or replace temporary table temp_final as
    with txr_raw as (
        select *
        from pc_fivetran_db.s2s_truecommerce.txr
    ),
    tc_qb_map as (
        select * 
        from truecommerce.qb_mapping
    ),
    txr_join as (
        select
            lower(to_varchar(sha2(tr.textbox_23|| '-' || tr.textbox_17 || '-' || tr.textbox_19, 256))) as transaction_id,
            (_line + 1) as index,
            tr.textbox_23 as trading_partner,
            tq.qb_id as trading_partner_qb_id,
            substring(tr.textbox_21, 5) as transaction_type,
            left(tr.textbox_21, 3) as transaction_type_id,
            tr.textbox_19 as doc_number, 
            tr.alt_doc_num as alt_doc_number,
            try_cast(replace(replace(tr.txn_amount,'$', ''), ',', '') as double) as amount,
            to_date(tr.textbox_17, 'mm/dd/yyyy') as creation_date,
            tr.textbox_15 as folder,
            date_trunc('MONTH', to_date(tr.textbox_17, 'mm/dd/yyyy')) as period_first_day,
            last_day(to_date(tr.textbox_17, 'mm/dd/yyyy')) as period_last_day,
            true as latest,
            lower(to_varchar(sha2(_file|| '-' || _line, 256))) as _row_id, 
            _modified as _modified,
            _file as _file,
            tr._fivetran_synced as _fivetran_synced
        from txr_raw tr
        left join tc_qb_map tq on tr.textbox_23 = tq.tc_name
    )
    select *
    from txr_join
    where transaction_id is not null
`;
stmt = snowflake.createStatement({ sqlText: create_temp_table_query });
stmt.execute();

// check for new rows
var check_new_rows_query = `
    with not_exists as (
        select _row_id
        from temp_final
        where not exists (
            select 1
            from truecommerce.transaction_register tr
            where tr._row_id = temp_final._row_id
        )
    )
    select count(*) as new_row_count
    from not_exists
`;

var stmt = snowflake.createStatement({ sqlText: check_new_rows_query });
var result = stmt.execute();
var new_row_count = 0;
if (result.next()) {
    new_row_count = result.getColumnValue(1);
}

if (new_row_count > 0) {
// set all 'latest' flags to false
    var set_existing_to_false_query = `
        update truecommerce.transaction_register
        set latest = false
        where _row_id in (
            select _row_id
            from temp_final
        );
    `;
stmt = snowflake.createStatement({ sqlText: set_existing_to_false_query });
stmt.execute();

// insert the new data
var insert_new_data_query = `
    insert into truecommerce.transaction_register (
        transaction_id,
        index,
        trading_partner,
        trading_partner_qb_id,
        transaction_type,
        transaction_type_id,
        doc_number, 
        alt_doc_number,
        amount,
        creation_date,
        folder,
        period_first_day,
        period_last_day,
        latest,
        _row_id,
        _modified,
        _file,
        _fivetran_synced
    )
    select 
        transaction_id,
        index,
        trading_partner,
        trading_partner_qb_id,
        transaction_type,
        transaction_type_id,
        doc_number, 
        alt_doc_number,
        amount,
        creation_date,
        folder,
        period_first_day,
        period_last_day,
        latest,
        _row_id,
        _modified,
        _file,
        _fivetran_synced
    from temp_final
    where not exists (
        select 1
        from truecommerce.transaction_register tr
        where tr._row_id = temp_final._row_id
    );
`;
stmt = snowflake.createStatement({ sqlText: insert_new_data_query });
stmt.execute();

    return "updated truecommerce transaction_register (" + new_row_count + ")";
  } else {
    return "no new rows to insert.";
  }
$$;