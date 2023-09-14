CREATE OR REPLACE PROCEDURE update_integration_table()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  // Step 1: Check if there are new _row_id's to insert
    var check_new_rows_query = `
        with integration_raw as (
            select *
            from pc_fivetran_db.s2s_truecommerce.integration
        ),
        tc_qb_map as (
            select * 
            from truecommerce.qb_mapping
        ),
        integration_join as (
            select
                lower(to_varchar(sha2(i.edipartner_name || '-' || i.document_date|| '-' || alt_doc_id, 256))) as transaction_id,
                (_line + 1) as index,
                i.edipartner_name as trading_partner,
                tq.qb_id as trading_partner_qb_id,
                i.txn_type as transaction_type,
                i.alt_doc_id as alt_doc_number,
                i.account_id as tc_account_id,
                try_cast(replace(replace(i.amount,'$', ''), ',', '') as double) as amount,
                to_timestamp(i.document_date, 'MM/DD/YYYY HH12:MI:SS AM') as document_date,
                to_timestamp(i.log_time, 'MM/DD/YYYY HH12:MI:SS AM') as log_time,
                description as description,
                date_trunc('MONTH', to_timestamp(i.document_date, 'MM/DD/YYYY HH12:MI:SS AM'))::DATE as period_first_day,
                last_day(to_timestamp(i.document_date, 'MM/DD/YYYY HH12:MI:SS AM'))::DATE as period_last_day,
                true as latest,
                lower(to_varchar(sha2(i._file|| '-' || i._line, 256))) as _row_id, 
                i._modified as _modified,
                i._file as _file,
                i._fivetran_synced as _fivetran_synced
            from integration_raw i
            left join tc_qb_map tq on i.edipartner_name = tq.tc_name
        ),
        final as (
            select
                transaction_id,
                index,
                trading_partner,
                trading_partner_qb_id,
                transaction_type,
                alt_doc_number,
                tc_account_id,
                amount,
                document_date,
                log_time,
                description,
                period_first_day,
                period_last_day,
                latest,
                _row_id,
                _modified,
                _file,
                _fivetran_synced
            from integration_join
        ),
        not_exists AS (
            SELECT _row_id
            FROM final
            WHERE NOT EXISTS (
                SELECT 1
                FROM truecommerce.transaction_register tr
                WHERE tr._row_id = final._row_id
            )
        )
        SELECT COUNT(*) AS new_row_count
        FROM not_exists;
        `;

    var stmt = snowflake.createStatement({ sqlText: check_new_rows_query });
    var result = stmt.execute();
    var new_row_count;
    if (result.next()) {
        new_row_count = result.getColumnValue(1);
    } else {
        new_row_count = 0;
    }

    if (new_row_count > 0) {
        // Step 2: Set the 'latest' flag of everything in the database to false
        var update_latest_flag_query = `
        UPDATE truecommerce.transaction_register
        SET latest = FALSE
        WHERE latest = TRUE;
        `;
        stmt = snowflake.createStatement({ sqlText: update_latest_flag_query });
        stmt.execute();

    // Step 3: Insert the new data
    var insert_new_data_query = `
        insert into truecommerce.integration (
            transaction_id,
            index,
            trading_partner,
            trading_partner_qb_id,
            transaction_type,
            alt_doc_number,
            tc_account_id,
            amount,
            document_date,
            log_time,
            description,
            period_first_day,
            period_last_day,
            latest,
            _row_id,
            _modified,
            _file,
            _fivetran_synced
        )
        with integration_raw as (
            select *
            from pc_fivetran_db.s2s_truecommerce.integration
        ),
        tc_qb_map as (
            select * 
            from truecommerce.qb_mapping
        ),
        integration_join as (
            select
                lower(to_varchar(sha2(i.edipartner_name || '-' || i.document_date|| '-' || alt_doc_id, 256))) as transaction_id,
                (_line + 1) as index,
                i.edipartner_name as trading_partner,
                tq.qb_id as trading_partner_qb_id,
                i.txn_type as transaction_type,
                i.alt_doc_id as alt_doc_number,
                i.account_id as tc_account_id,
                try_cast(replace(replace(i.amount,'$', ''), ',', '') as double) as amount,
                to_timestamp(i.document_date, 'MM/DD/YYYY HH12:MI:SS AM') as document_date,
                to_timestamp(i.log_time, 'MM/DD/YYYY HH12:MI:SS AM') as log_time,
                description as description,
                date_trunc('MONTH', to_timestamp(i.document_date, 'MM/DD/YYYY HH12:MI:SS AM'))::DATE as period_first_day,
                last_day(to_timestamp(i.document_date, 'MM/DD/YYYY HH12:MI:SS AM'))::DATE as period_last_day,
                true as latest,
                lower(to_varchar(sha2(i._file|| '-' || i._line, 256))) as _row_id, 
                i._modified as _modified,
                i._file as _file,
                i._fivetran_synced as _fivetran_synced
            from integration_raw i
            left join tc_qb_map tq on i.edipartner_name = tq.tc_name
        ),
        final as (
            select
                transaction_id,
                index,
                trading_partner,
                trading_partner_qb_id,
                transaction_type,
                alt_doc_number,
                tc_account_id,
                amount,
                document_date,
                log_time,
                description,
                period_first_day,
                period_last_day,
                latest,
                _row_id,
                _modified,
                _file,
                _fivetran_synced
            from integration_join
        )
        select 
            transaction_id,
            index,
            trading_partner,
            trading_partner_qb_id,
            transaction_type,
            alt_doc_number,
            tc_account_id,
            amount,
            document_date,
            log_time,
            description,
            period_first_day,
            period_last_day,
            latest,
            _row_id,
            _modified,
            _file,
            _fivetran_synced
        from final
        where not exists (
            select 1
            from truecommerce.integration i
            where i._row_id = final._row_id
        );
    `;
    stmt = snowflake.createStatement({ sqlText: insert_new_data_query });
    stmt.execute();

    return "Updated integration table with " + new_row_count + " new rows.";
  } else {
    return "No new rows to insert.";
  }
$$;

call truecommerce.update_integration_table();

update truecommerce.integration
set latest = false;

insert into truecommerce.integration (
    transaction_id,
    index,
    trading_partner,
    trading_partner_qb_id,
    transaction_type,
    alt_doc_number,
    tc_account_id,
    amount,
    document_date,
    log_time,
    description,
    period_first_day,
    period_last_day,
    latest,
    _row_id,
    _modified,
    _file,
    _fivetran_synced
)
with integration_raw as (
    select *
    from pc_fivetran_db.s2s_truecommerce.integration
),
tc_qb_map as (
    select * 
    from truecommerce.qb_mapping
),
integration_join as (
    select
        lower(to_varchar(sha2(i.edipartner_name || '-' || i.document_date|| '-' || alt_doc_id, 256))) as transaction_id,
        (_line + 1) as index,
        i.edipartner_name as trading_partner,
        tq.qb_id as trading_partner_qb_id,
        i.txn_type as transaction_type,
        i.alt_doc_id as alt_doc_number,
        i.account_id as tc_account_id,
        try_cast(replace(replace(i.amount,'$', ''), ',', '') as double) as amount,
        to_timestamp(i.document_date, 'MM/DD/YYYY HH12:MI:SS AM') as document_date,
        to_timestamp(i.log_time, 'MM/DD/YYYY HH12:MI:SS AM') as log_time,
        description as description,
        date_trunc('MONTH', to_timestamp(i.document_date, 'MM/DD/YYYY HH12:MI:SS AM'))::DATE as period_first_day,
        last_day(to_timestamp(i.document_date, 'MM/DD/YYYY HH12:MI:SS AM'))::DATE as period_last_day,
        true as latest,
        lower(to_varchar(sha2(i._file|| '-' || i._line, 256))) as _row_id, 
        i._modified as _modified,
        i._file as _file,
        i._fivetran_synced as _fivetran_synced
    from integration_raw i
    left join tc_qb_map tq on i.edipartner_name = tq.tc_name
),
final as (
    select
        transaction_id,
        index,
        trading_partner,
        trading_partner_qb_id,
        transaction_type,
        alt_doc_number,
        tc_account_id,
        amount,
        document_date,
        log_time,
        description,
        period_first_day,
        period_last_day,
        latest,
        _row_id,
        _modified,
        _file,
        _fivetran_synced
    from integration_join
)
select 
    transaction_id,
    index,
    trading_partner,
    trading_partner_qb_id,
    transaction_type,
    alt_doc_number,
    tc_account_id,
    amount,
    document_date,
    log_time,
    description,
    period_first_day,
    period_last_day,
    latest,
    _row_id,
    _modified,
    _file,
    _fivetran_synced
from final
where not exists (
    select 1
    from truecommerce.integration i
    where i._row_id = final._row_id
);