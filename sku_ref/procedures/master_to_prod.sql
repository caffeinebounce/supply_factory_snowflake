CREATE OR REPLACE PROCEDURE sku_ref.update_sku_ref_master()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    var create_temp_tables_sql = `
        begin
        create or replace temporary table temp_sku_ref_master as
            with sku_ref_raw as (
                select *
                from pc_fivetran_db.sku_ref_data_s2s.sku_reference_data_v_master_sku_master
                ), 
            vendors as (
                select *
                from pc_fivetran_db.quickbooks.vendor
                ),
            tracker as (
                select *
                from prod_sf_tables.po_tracker.tracker_clean
            ),
            int_sr_qb_map as (
                select
                    sr.vendor as vendor_name,
                    qb.display_name as qb_name,
                    qb.print_on_check_name as qb_check_name,
                    qb.id as id,
                    JAROWINKLER_SIMILARITY(qb.display_name, sr.vendor) AS similarity,
                    ROW_NUMBER() OVER (PARTITION BY sr.vendor ORDER BY JAROWINKLER_SIMILARITY(qb.display_name, sr.vendor) DESC) AS row_num
                FROM sku_ref_raw sr
                LEFT JOIN vendors qb ON JAROWINKLER_SIMILARITY(qb.display_name, sr.vendor) >= 85 AND qb.display_name NOT LIKE '%(deleted)%'
                AND JAROWINKLER_SIMILARITY(qb.display_name, sr.vendor) = (
                    SELECT MAX(JAROWINKLER_SIMILARITY(qb2.display_name, sr2.vendor))
                    FROM sku_ref_raw sr2
                    JOIN vendors qb2 ON JAROWINKLER_SIMILARITY(qb2.display_name, sr2.vendor) >= 85 AND qb2.display_name NOT LIKE '%(deleted)%'
                    WHERE sr2.vendor = sr.vendor
                )
                ),
            filtered_sr_qb_map AS (
                SELECT *
                FROM int_sr_qb_map
                WHERE row_num = 1 and 
                vendor_name is not null
                ), 
            final_sku_ref as (
                select
                    sr.sku as sku,
                    sr.sku_name,
                    sr.upc as upc,
                    sr.brand as brand,
                    sr.status,
                    sr.product_code,
                    sr.product_group,
                    sr.sku_type,
                    sr.class,
                    sr.version,
                    sr.vendor,
                    qb.id as vendor_id,
                    sr.country,
                    sr.dimensions_units,
                    sr.weight_units,
                    sr.unit,
                    sr.unit_width,
                    sr.unit_height,
                    sr.unit_length,
                    sr.carton_height,
                    sr.carton_width,
                    sr.carton_length,
                    sr.carton_weight,
                    try_cast(sr.eaches_per_carton as int) as eaches_per_carton,
                    sr.notes,
                    sr._fivetran_synced
                from sku_ref_raw sr
                left join filtered_sr_qb_map qb on sr.vendor = qb.vendor_name
                )
            select * from final_sku_ref
        ;
        create or replace temporary table temp_retail_master as (
            select
                sku_sold_to_retailer as sku,
                retailer, 
                retailer_sku,
                upc,
                srp,
                retail_discount,
                retail_srp
            from pc_fivetran_db.sku_ref_data_s2s.sku_reference_data_v_master_retail_master_sku_list
            )
        ;
        create or replace temporary table temp_brands as (
            select * 
            from pc_fivetran_db.sku_ref_data_s2s.sku_reference_data_v_master_brands
            )
        ;
        create or replace temporary table temp_groups as (
            select
                group_id,
                group_name,
                product_type 
            from pc_fivetran_db.sku_ref_data_s2s.sku_reference_data_v_master_groups
            )
        ;
        end
        `;

    snowflake.execute({ sqlText: create_temp_tables_sql });

    var tables = [
        { name: "sku_ref.master", temp_name: "temp_sku_ref_master" },
        { name: "sku_ref.retail_master", temp_name: "temp_retail_master" },
        { name: "sku_ref.brands", temp_name: "temp_brands" },
        { name: "sku_ref.groups", temp_name: "temp_groups" }
        ];

    var resultMessage = "";

    tables.forEach(function (table) {
        var count_diff_sql = `
            SELECT COUNT(*) as "diff_count"
            FROM (
                SELECT * FROM ${table.temp_name}
                EXCEPT
                SELECT * FROM ${table.name}
            ) AS diff
        `;

    var count_diff_stmt = snowflake.createStatement({ sqlText: count_diff_sql });
    var count_diff_result = count_diff_stmt.execute();
    if (count_diff_result.next()) {
        var diff_count = count_diff_result.getColumnValue("diff_count");
    } else {
        var diff_count = 0;
    }


    if (diff_count > 0) {
      var update_sql = `CREATE OR REPLACE TABLE ${table.name} AS SELECT * FROM ${table.temp_name};`;
      snowflake.execute({ sqlText: update_sql });
      resultMessage += `Table '${table.name}' has been updated. ${diff_count} rows were added or changed.\n All views and associated tables were updated.`;
    } else {
      resultMessage += `No changes were made to the '${table.name}' table.\n`;
    }
    });

    // Create the finished goods view
var create_finished_goods_view_sql = `
    create or replace view sku_ref.master_finished_goods as
    select
        *
    from
        sku_ref.master
    where
        sku_type = 'Finished Good';
`;

snowflake.execute({ sqlText: create_finished_goods_view_sql });

    return resultMessage;
$$;

CALL sku_ref.update_sku_ref_master();

CREATE OR REPLACE TASK update_sku_ref_master_daily
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 3 * * * America/New_York'
    TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9'
    TIMESTAMP_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9'
    TIMEZONE = 'America/New_York'
AS
    CALL update_sku_ref_master();

ALTER TASK update_sku_ref_master_daily RESUME;


            with sku_ref_raw as (
                select *
                from pc_fivetran_db.sku_ref_data_s2s.sku_reference_data_v_master_sku_master
                ), 
            vendors as (
                select *
                from pc_fivetran_db.quickbooks.vendor
                ),
            tracker as (
                select *
                from prod_sf_tables.po_tracker.tracker_clean
            ),
            int_sr_qb_map as (
                select
                    sr.vendor as vendor_name,
                    qb.display_name as qb_name,
                    qb.print_on_check_name as qb_check_name,
                    qb.id as id,
                    JAROWINKLER_SIMILARITY(qb.display_name, sr.vendor) AS similarity,
                    ROW_NUMBER() OVER (PARTITION BY sr.vendor ORDER BY JAROWINKLER_SIMILARITY(qb.display_name, sr.vendor) DESC) AS row_num
                FROM sku_ref_raw sr
                LEFT JOIN vendors qb ON JAROWINKLER_SIMILARITY(qb.display_name, sr.vendor) >= 85 AND qb.display_name NOT LIKE '%(deleted)%'
                AND JAROWINKLER_SIMILARITY(qb.display_name, sr.vendor) = (
                    SELECT MAX(JAROWINKLER_SIMILARITY(qb2.display_name, sr2.vendor))
                    FROM sku_ref_raw sr2
                    JOIN vendors qb2 ON JAROWINKLER_SIMILARITY(qb2.display_name, sr2.vendor) >= 85 AND qb2.display_name NOT LIKE '%(deleted)%'
                    WHERE sr2.vendor = sr.vendor
                )
                ),
            filtered_sr_qb_map AS (
                SELECT *
                FROM int_sr_qb_map
                WHERE row_num = 1 and 
                vendor_name is not null
                ), 
            final_sku_ref as (
                select
                    sr.sku as sku,
                    sr.upc as upc,
                    sr.brand as brand,
                    sr.product_name as sku_name,
                    sr.status,
                    sr.product_code,
                    sr.product_group,
                    sr.sku_type,
                    sr.class,
                    sr.version,
                    sr.vendor,
                    qb.id as vendor_id,
                    sr.country,
                    sr.dimensions_units,
                    sr.weight_units,
                    sr.unit,
                    sr.unit_width,
                    sr.unit_height,
                    sr.unit_length,
                    sr.carton_height,
                    sr.carton_width,
                    sr.carton_length,
                    sr.carton_weight,
                    sr.eaches_per_carton,
                    sr.notes,
                    sr._fivetran_synced
                from sku_ref_raw sr
                left join filtered_sr_qb_map qb on sr.vendor = qb.vendor_name
                )
            select * from final_sku_ref
        ;

        create or replace temporary table temp_retail_master as (
            select
                sku_sold_to_retailer as sku,
                retailer, 
                retailer_sku,
                upc,
                srp,
                retail_discount,
                retail_srp
            from pc_fivetran_db.sku_ref_data_s2s.sku_reference_data_v_master_retail_master_sku_list
            )
        ;
        create or replace temporary table temp_brands as (
            select * 
            from pc_fivetran_db.sku_ref_data_s2s.sku_reference_data_v_master_brands
            )
        ;
        create or replace temporary table temp_groups as (
            select
                group_id,
                group_name,
                product_type 
            from pc_fivetran_db.sku_ref_data_s2s.sku_reference_data_v_master_groups
            )
        ;