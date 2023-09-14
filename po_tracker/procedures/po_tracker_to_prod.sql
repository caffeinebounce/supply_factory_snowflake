CREATE OR REPLACE PROCEDURE update_po_tracker_po_status()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // Truncate the po_tracker_po_status table
    var truncate_sql = "TRUNCATE TABLE po_tracker.po_status;";
    snowflake.execute({ sqlText: truncate_sql });

    // Insert data into the po_tracker_po_status table
    var insert_sql = `
        INSERT INTO po_tracker.po_status (status, type)
        WITH status AS (
            SELECT *
            FROM pc_fivetran_db.po_tracker.po_tracker_siis_statuses
        )
        SELECT
            po_statuses AS status,
            type AS type
        FROM status;
    `;
    snowflake.execute({ sqlText: insert_sql });

    // Return a success message
    return "The po_tracker_po_status table has been successfully updated.";
$$;
CALL update_po_tracker_po_status();

            insert into po_tracker.tracker (
                master_po_num,
                brand,
                status,
                status_type,
                po_type,
                fg_po_association,
                po_date,
                vendor_sku,
                sku,
                sku_name,
                upc,
                max_lead_time,
                ship_date_lead_time,
                ship_date_req,
                ship_date_actual,
                req_receipt_date,
                delivery_date_actual,
                invoice_due_date,
                ship_from,
                ship_from_id,
                ship_to,
                ship_to_id,
                total_po_quantity_sku,
                quantity,
                quantity_received,
                moq,
                per_unit_order,
                per_unit_effective,
                total,
                payment_terms,
                pmt_upfront_percent,
                pmt_upfront_net,
                pmt_upfront_addl_percent,
                pmt_upfront_addl_net,
                pmt_ship_percent,
                pmt_ship_net,
                pmt_delivery_percent,
                pmt_delivery_net,
                pmt_cc,
                pmt_cc_percent,
                pmt_check,
                new_launch,
                launch_date,
                latest,
                _row_id,
                _line,
                _fivetran_synced
                )
                with tracker_raw as (
                    select *
                    from pc_fivetran_db.po_tracker.po_tracker_siis_po_tracker
                ),
                latest_synced as (
                    select max(date_trunc('minute', _fivetran_synced)) as max_synced from tracker_raw
                ),
                vendor_mapping as (
                    select *
                    from prod_sf_tables.po_tracker.vendor_mapping
                ),
                status as (
                    select *
                    from prod_sf_tables.po_tracker.po_status
                ),
                sku_ref as (
                    select * 
                    from prod_sf_tables.sku_ref.master
                ),
                po_ref_match as (
                    select
                        t.sku,
                        sr.product_type,
                        sr.sku_name,
                        sr.upc,
                        row_number() over (partition by t.sku order by t._line) as row_num
                    from tracker_raw t
                    left join sku_ref sr on t.sku = sr.sku
                ),
                po_ref_filtered as (
                    select *
                    from po_ref_match
                    where row_num = 1
                ),
                final_po_tracker as (
                    select
                        master_po_num,
                        t.brand,
                        r.product_type as po_type,
                        trim(fg_po_association) as fg_po_association,
                        po_date,
                        trim(t.vendor_sku) as vendor_sku,
                        t.sku as sku,
                        r.sku_name as sku_name,
                        r.upc as upc,
                        max_lead_time,
                        to_date(dateadd(day, ship_date_lead_time, '1899-12-30')) as ship_date_lead_time,
                        try_to_date(left(ship_date_req, 10)) as ship_date_req,
                        try_to_date(left(ship_date_actual, 10)) as ship_date_actual,
                        try_to_date(left(req_receipt_date, 10)) as req_receipt_date,
                        try_to_date(left(delivery_date_actual, 10)) as delivery_date_actual,
                        try_to_date(left(invoice_due_date, 10)) as invoice_due_date,
                        ship_from,
                        sf.id as ship_from_id,
                        ship_to,
                        st.id as ship_to_id,
                        total_po_quantity_sku,
                        quantity,
                        quantity_received,
                        t.moq,
                        round(t.price_per_unit, 2) as per_unit_order,
                        case
                            when t.status = 'COMPLETE' then round(t.total / t.quantity_received, 2)
                            else null
                        end as per_unit_effective, 
                        round(t.total, 2) as total,
                        payment_terms,
                        pmt_upfront_percent,
                        pmt_upfront_net,
                        pmt_upfront_addl_percent,
                        pmt_upfront_addl_net,
                        pmt_ship_percent,
                        pmt_ship_net,
                        pmt_delivery_percent,
                        pmt_delivery_net,
                        pmt_cc,
                        pmt_cc_percent,
                        pmt_check,
                        new_launch,
                        try_to_date(left(launch_date, 10)) as launch_date,
                        t.status,
                        case
                            when date_trunc('minute', t._fivetran_synced) = ls.max_synced then true
                            else false
                        end as latest,
                        pos.type as status_type,
                        lower(to_varchar(sha2(t._fivetran_synced|| '-' || t._line, 256))) as _row_id, 
                        _line,
                        t._fivetran_synced
                    from tracker_raw t
                    left join latest_synced ls on 1=1
                    left join po_ref_filtered r on t.sku = r.sku
                    left join vendor_mapping sf on t.ship_from = sf.po_name
                    left join vendor_mapping st on t.ship_to = st.po_name
                    left join status pos on t.status = pos.status
                )
                select 
                    master_po_num,
                    brand,
                    status,
                    status_type,
                    po_type,
                    fg_po_association,
                    po_date,
                    vendor_sku,
                    sku,
                    sku_name,
                    upc,
                    max_lead_time,
                    ship_date_lead_time,
                    ship_date_req,
                    ship_date_actual,
                    req_receipt_date,
                    delivery_date_actual,
                    invoice_due_date,
                    ship_from,
                    ship_from_id,
                    ship_to,
                    ship_to_id,
                    total_po_quantity_sku,
                    quantity,
                    quantity_received,
                    moq,
                    per_unit_order,
                    per_unit_effective,
                    total,
                    payment_terms,
                    pmt_upfront_percent,
                    pmt_upfront_net,
                    pmt_upfront_addl_percent,
                    pmt_upfront_addl_net,
                    pmt_ship_percent,
                    pmt_ship_net,
                    pmt_delivery_percent,
                    pmt_delivery_net,
                    pmt_cc,
                    pmt_cc_percent,
                    pmt_check,
                    new_launch,
                    launch_date,
                    latest,
                    _row_id,
                    _line,
                    _fivetran_synced
                from final_po_tracker
            ;`
create or replace view po_tracker.tracker_clean as 
    select
        master_po_num,
        status,
        brand, 
        sku,
        sku_name, 
        upc,
        po_type,
        fg_po_association,
        po_date,
        ship_date_actual,
        delivery_date_actual,
        ship_from,
        ship_from_id,
        ship_to,
        ship_to_id,
        quantity,
        quantity_received,
        per_unit_order,
        per_unit_effective,
        payment_terms
    from po_tracker.tracker
    where latest = true
;
create or replace view po_tracker.tracker_clean_complete as 
    select
        master_po_num,
        status,
        brand, 
        sku,
        sku_name, 
        upc,
        po_type,
        fg_po_association,
        po_date,
        ship_date_actual,
        delivery_date_actual,
        ship_from,
        ship_from_id,
        ship_to,
        ship_to_id,
        quantity,
        quantity_received,
        per_unit_order,
        per_unit_effective,
        payment_terms
    from po_tracker.tracker
    where latest = true and
    status = 'COMPLETE'
;
create or replace view po_tracker.tracker_clean_open_po as 
    select
        master_po_num,
        status,
        brand, 
        sku,
        sku_name, 
        upc,
        po_type,
        fg_po_association,
        po_date,
        ship_date_actual,
        delivery_date_actual,
        ship_from,
        ship_from_id,
        ship_to,
        ship_to_id,
        quantity,
        quantity_received,
        per_unit_order,
        per_unit_effective,
        payment_terms
    from po_tracker.tracker
    where latest = true and
    status_type = 'OPEN'
;

-- Creating the procedure
CREATE OR REPLACE PROCEDURE update_po_tracker()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // SQL script as an array of strings
    const sqlScript = [
        `   update po_tracker.tracker set latest = false;`,
        `   insert into po_tracker.tracker (
                master_po_num,
                brand,
                status,
                status_type,
                po_type,
                fg_po_association,
                po_date,
                vendor_sku,
                sku,
                sku_name,
                upc,
                max_lead_time,
                ship_date_lead_time,
                ship_date_req,
                ship_date_actual,
                req_receipt_date,
                delivery_date_actual,
                invoice_due_date,
                ship_from,
                ship_from_id,
                ship_to,
                ship_to_id,
                total_po_quantity_sku,
                quantity,
                quantity_received,
                moq,
                per_unit_order,
                per_unit_effective,
                total,
                payment_terms,
                pmt_upfront_percent,
                pmt_upfront_net,
                pmt_upfront_addl_percent,
                pmt_upfront_addl_net,
                pmt_ship_percent,
                pmt_ship_net,
                pmt_delivery_percent,
                pmt_delivery_net,
                pmt_cc,
                pmt_cc_percent,
                pmt_check,
                new_launch,
                launch_date,
                latest,
                _row_id,
                _line,
                _fivetran_synced
                )
                with tracker_raw as (
                    select *
                    from pc_fivetran_db.po_tracker.po_tracker_siis_po_tracker
                ),
                latest_synced as (
                    select max(date_trunc('minute', _fivetran_synced)) as max_synced from tracker_raw
                ),
                vendor_mapping as (
                    select *
                    from prod_sf_tables.po_tracker.vendor_mapping
                ),
                status as (
                    select *
                    from prod_sf_tables.po_tracker.po_status
                ),
                sku_ref as (
                    select * 
                    from prod_sf_tables.sku_ref.master
                ),
                po_ref_match as (
                    select
                        t.sku,
                        sr.product_type,
                        sr.sku_name,
                        sr.upc,
                        row_number() over (partition by t.sku order by t._line) as row_num
                    from tracker_raw t
                    left join sku_ref sr on t.sku = sr.sku
                ),
                po_ref_filtered as (
                    select *
                    from po_ref_match
                    where row_num = 1
                ),
                final_po_tracker as (
                    select
                        master_po_num,
                        t.brand,
                        r.product_type as po_type,
                        trim(fg_po_association) as fg_po_association,
                        po_date,
                        trim(t.vendor_sku) as vendor_sku,
                        t.sku as sku,
                        r.sku_name as sku_name,
                        r.upc as upc,
                        max_lead_time,
                        to_date(dateadd(day, ship_date_lead_time, '1899-12-30')) as ship_date_lead_time,
                        try_to_date(left(ship_date_req, 10)) as ship_date_req,
                        try_to_date(left(ship_date_actual, 10)) as ship_date_actual,
                        try_to_date(left(req_receipt_date, 10)) as req_receipt_date,
                        try_to_date(left(delivery_date_actual, 10)) as delivery_date_actual,
                        try_to_date(left(invoice_due_date, 10)) as invoice_due_date,
                        ship_from,
                        sf.id as ship_from_id,
                        ship_to,
                        st.id as ship_to_id,
                        total_po_quantity_sku,
                        quantity,
                        quantity_received,
                        t.moq,
                        round(t.price_per_unit, 2) as per_unit_order,
                        case
                            when t.status = 'COMPLETE' then round(t.total / t.quantity_received, 2)
                            else null
                        end as per_unit_effective, 
                        round(t.total, 2) as total,
                        payment_terms,
                        pmt_upfront_percent,
                        pmt_upfront_net,
                        pmt_upfront_addl_percent,
                        pmt_upfront_addl_net,
                        pmt_ship_percent,
                        pmt_ship_net,
                        pmt_delivery_percent,
                        pmt_delivery_net,
                        pmt_cc,
                        pmt_cc_percent,
                        pmt_check,
                        new_launch,
                        try_to_date(left(launch_date, 10)) as launch_date,
                        t.status,
                        case
                            when date_trunc('minute', t._fivetran_synced) = ls.max_synced then true
                            else false
                        end as latest,
                        pos.type as status_type,
                        lower(to_varchar(sha2(t._fivetran_synced|| '-' || t._line, 256))) as _row_id, 
                        _line,
                        t._fivetran_synced
                    from tracker_raw t
                    left join latest_synced ls on 1=1
                    left join po_ref_filtered r on t.sku = r.sku
                    left join vendor_mapping sf on t.ship_from = sf.po_name
                    left join vendor_mapping st on t.ship_to = st.po_name
                    left join status pos on t.status = pos.status
                )
                select 
                    master_po_num,
                    brand,
                    status,
                    status_type,
                    po_type,
                    fg_po_association,
                    po_date,
                    vendor_sku,
                    sku,
                    sku_name,
                    upc,
                    max_lead_time,
                    ship_date_lead_time,
                    ship_date_req,
                    ship_date_actual,
                    req_receipt_date,
                    delivery_date_actual,
                    invoice_due_date,
                    ship_from,
                    ship_from_id,
                    ship_to,
                    ship_to_id,
                    total_po_quantity_sku,
                    quantity,
                    quantity_received,
                    moq,
                    per_unit_order,
                    per_unit_effective,
                    total,
                    payment_terms,
                    pmt_upfront_percent,
                    pmt_upfront_net,
                    pmt_upfront_addl_percent,
                    pmt_upfront_addl_net,
                    pmt_ship_percent,
                    pmt_ship_net,
                    pmt_delivery_percent,
                    pmt_delivery_net,
                    pmt_cc,
                    pmt_cc_percent,
                    pmt_check,
                    new_launch,
                    launch_date,
                    latest,
                    _row_id,
                    _line,
                    _fivetran_synced
                from final_po_tracker
            ;`,
    
    ];

    // Execute each SQL statement
    try {
        sqlScript.forEach((sql) => {
        let stmt = snowflake.createStatement({ sqlText: sql });
        stmt.execute();
        });
        return "Success: PO Tracker updated.";
    } catch (err) {
        return "Error: " + err.message;
    }
$$;
CALL update_po_tracker();
-- Creating the procedure
CREATE OR REPLACE PROCEDURE update_po_tracker_views()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // SQL script as an array of strings
    const sqlScript = [
        `   create or replace view po_tracker.tracker_clean as 
                select
                    master_po_num,
                    status,
                    brand, 
                    sku,
                    sku_name,
                    vendor_sku, 
                    upc,
                    po_type,
                    fg_po_association,
                    po_date,
                    ship_date_actual,
                    delivery_date_actual,
                    ship_from,
                    ship_from_id,
                    ship_to,
                    ship_to_id,
                    quantity,
                    quantity_received,
                    per_unit_order,
                    per_unit_effective,
                    payment_terms
                from po_tracker.tracker
                where latest = true
            ;`,
        `   create or replace view po_tracker.tracker_clean_complete as 
                select
                    master_po_num,
                    status,
                    brand, 
                    sku,
                    sku_name, 
                    upc,
                    po_type,
                    fg_po_association,
                    po_date,
                    ship_date_actual,
                    delivery_date_actual,
                    ship_from,
                    ship_from_id,
                    ship_to,
                    ship_to_id,
                    quantity,
                    quantity_received,
                    per_unit_order,
                    per_unit_effective,
                    payment_terms
                from po_tracker.tracker
                where latest = true and
                status = 'COMPLETE'
            ;`,
        `   create or replace view po_tracker.tracker_clean_open_po as 
                select
                    master_po_num,
                    status,
                    brand, 
                    sku,
                    sku_name, 
                    upc,
                    po_type,
                    fg_po_association,
                    po_date,
                    ship_date_actual,
                    delivery_date_actual,
                    ship_from,
                    ship_from_id,
                    ship_to,
                    ship_to_id,
                    quantity,
                    quantity_received as quantity_received_current,
                    per_unit_order,
                    payment_terms
                from po_tracker.tracker
                where latest = true and
                status_type = 'OPEN'
            ;`
    
    ];
    // Execute each SQL statement
    try {
        sqlScript.forEach((sql) => {
        let stmt = snowflake.createStatement({ sqlText: sql });
        stmt.execute();
        });
        return "Success: PO Tracker views updated.";
    } catch (err) {
        return "Error: " + err.message;
    }
$$;
CALL update_po_tracker_views();

create or replace view po_tracker.sku_match as (
    select distinct 
        p.sku as po_sku,
        p.sku_name as po_name,
        p.upc as po_upc,
        case
            when sr1.sku is not null then true
            else false
        end as sku_matched,
        sr1.sku_name,
        case
            when sr2.sku is not null then true
            else false
        end as upc_matched
    from po_tracker.tracker p
    left join sku_ref.master sr1 on sr1.sku = p.sku
    left join sku_ref.master sr2 on sr2.upc = p.upc 
)