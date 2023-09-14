insert into po_tracker.vendor_mapping (
    po_name,
    qb_name,
    qb_check_name,
    id,
    tc_name,
    similarity,
    row_num
)

with tracker_raw as (
    SELECT *
    FROM pc_fivetran_db.po_tracker.po_tracker_siis_po_tracker
), 
vendors AS (
    SELECT *
    FROM pc_fivetran_db.quickbooks.vendor
),
ship_to_from AS (
    SELECT ship_name FROM (
        SELECT TRIM(ship_to) AS ship_name FROM tracker_raw WHERE ship_to != 'N/A'
        UNION
        SELECT TRIM(ship_from) AS ship_name FROM tracker_raw WHERE ship_from != 'N/A'
    )
),
int_po_qb_map AS (
    SELECT
        s.ship_name as po_name,
        qb.display_name as qb_name,
        qb.print_on_check_name as qb_check_name,
        qb.id as id,
        s.ship_name as tc_name,
        JAROWINKLER_SIMILARITY(qb.display_name, s.ship_name) AS similarity,
        ROW_NUMBER() OVER (PARTITION BY s.ship_name ORDER BY JAROWINKLER_SIMILARITY(qb.display_name, s.ship_name) DESC) AS row_num
    FROM ship_to_from s
    LEFT JOIN vendors qb ON JAROWINKLER_SIMILARITY(qb.display_name, s.ship_name) >= 85 AND qb.display_name NOT LIKE '%(deleted)%'
    AND JAROWINKLER_SIMILARITY(qb.display_name, s.ship_name) = (
        SELECT MAX(JAROWINKLER_SIMILARITY(qb2.display_name, s2.ship_name))
        FROM ship_to_from s2
        JOIN vendors qb2 ON JAROWINKLER_SIMILARITY(qb2.display_name, s2.ship_name) >= 85 AND qb2.display_name NOT LIKE '%(deleted)%'
        WHERE s2.ship_name = s.ship_name
    )
), 

final_mapping as (
    select
        po_name,
        qb_name,
        qb_check_name,
        id,
        tc_name,
        similarity,
        row_num
    from int_po_qb_map
)

SELECT 
    po_name,
    qb_name,
    qb_check_name,
    id,
    tc_name,
    similarity,
    row_num
FROM final_mapping;

CREATE OR REPLACE PROCEDURE po_tracker.update_vendor_mapping()
RETURNS FLOAT
LANGUAGE JAVASCRIPT
AS
$$
    var count_before_command = `select count (*) FROM po_tracker.vendor_mapping;`;
    var delete_command = `delete from po_tracker.vendor_mapping;`;
    var insert_command = `
        insert into po_tracker.vendor_mapping (
    po_name,
    qb_name,
    qb_check_name,
    id,
    tc_name,
    similarity,
    row_num
)

with tracker_raw as (
    SELECT *
    FROM pc_fivetran_db.po_tracker.po_tracker_siis_po_tracker
), 
vendors AS (
    SELECT *
    FROM pc_fivetran_db.quickbooks.vendor
),
ship_to_from AS (
    SELECT ship_name FROM (
        SELECT TRIM(ship_to) AS ship_name FROM tracker_raw WHERE ship_to != 'N/A'
        UNION
        SELECT TRIM(ship_from) AS ship_name FROM tracker_raw WHERE ship_from != 'N/A'
    )
),
int_po_qb_map AS (
    SELECT
        s.ship_name as po_name,
        qb.display_name as qb_name,
        qb.print_on_check_name as qb_check_name,
        qb.id as id,
        s.ship_name as tc_name,
        JAROWINKLER_SIMILARITY(qb.display_name, s.ship_name) AS similarity,
        ROW_NUMBER() OVER (PARTITION BY s.ship_name ORDER BY JAROWINKLER_SIMILARITY(qb.display_name, s.ship_name) DESC) AS row_num
    FROM ship_to_from s
    LEFT JOIN vendors qb ON JAROWINKLER_SIMILARITY(qb.display_name, s.ship_name) >= 85 AND qb.display_name NOT LIKE '%(deleted)%'
    AND JAROWINKLER_SIMILARITY(qb.display_name, s.ship_name) = (
        SELECT MAX(JAROWINKLER_SIMILARITY(qb2.display_name, s2.ship_name))
        FROM ship_to_from s2
        JOIN vendors qb2 ON JAROWINKLER_SIMILARITY(qb2.display_name, s2.ship_name) >= 85 AND qb2.display_name NOT LIKE '%(deleted)%'
        WHERE s2.ship_name = s.ship_name
    )
), 

final_mapping as (
    select
        po_name,
        qb_name,
        qb_check_name,
        id,
        tc_name,
        similarity,
        row_num
    from int_po_qb_map
)

SELECT 
    po_name,
    qb_name,
    qb_check_name,
    id,
    tc_name,
    similarity,
    row_num
FROM final_mapping;
    `;
    var count_after_command = `SELECT COUNT(*) FROM po_tracker.vendor_mapping;`;

    var count_before_stmt = snowflake.createStatement({sqlText: count_before_command});
    var delete_stmt = snowflake.createStatement({sqlText: delete_command});
    var insert_stmt = snowflake.createStatement({sqlText: insert_command});
    var count_after_stmt = snowflake.createStatement({sqlText: count_after_command});

    var count_before_result = count_before_stmt.execute();
    count_before_result.next();
    var count_before = count_before_result.getColumnValue(1);

    delete_stmt.execute();
    insert_stmt.execute();

    var count_after_result = count_after_stmt.execute();
    count_after_result.next();
    var count_after = count_after_result.getColumnValue(1);

    return count_after - count_before;
$$;

call po_tracker.update_vendor_mapping();