update boms.boms set latest = false;
insert into boms.boms (
                component_sku,
                component_name,
                is_fg,
                usage,
                fg_sku,
                fg_sku_name,
                fg_upc,
                fg_product_code,
                fg_product_group,
                fg_manufacturer,
                fg_status,
                component_index,
                component_type,
                component_version,
                component_unit,
                component_vendor,
                component_vendor_id,
                index,
                latest,
                _fivetran_synced
            )
            with bom_raw as (
                select *
                from pc_fivetran_db.s2s_boms.bom_master_bom
            ),
            sku_ref as (
                select *
                from prod_sf_tables.sku_ref.master
            ),
            final_boms as (
                select
                    b.component_sku,
                    co.sku_name as component_name,
                    case
                        when co.sku_type = 'Finished Good' then true
                        else false
                    end as is_fg,
                    b.usage,
                    b.fg_sku,
                    fg.sku_name as fg_sku_name,
                    fg.upc as fg_upc,
                    fg.product_code as fg_product_code,
                    fg.product_group as fg_product_group,
                    fg.vendor as fg_manufacturer,
                    fg.status as fg_status,
                    row_number() over (partition by fg_sku order by component_sku) as component_index,
                    co.product_group as component_type,
                    co.version as component_version,
                    co.unit as component_unit,
                    co.vendor as component_vendor,
                    co.vendor_id as component_vendor_id,
                    (b._line + 1) as index,
                    true as latest,
                    b._fivetran_synced
                from bom_raw b
                left join sku_ref co on b.component_sku = co.sku
                left join sku_ref fg on b.fg_sku = fg.sku
            )
            select
                component_sku,
                component_name,
                is_fg,
                usage,
                fg_sku,
                fg_sku_name,
                fg_upc,
                fg_product_code,
                fg_product_group,
                fg_manufacturer,
                fg_status,
                component_index,
                component_type,
                component_version,
                component_unit,
                component_vendor,
                component_vendor_id,
                index,
                latest,
                _fivetran_synced
            from final_boms
            ;

CREATE OR REPLACE PROCEDURE boms.update_boms()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // SQL script as an array of strings
    const sqlScript = [
        `   update boms.boms set latest = false;`,
        `   insert into boms.boms (
                component_sku,
                component_name,
                is_fg,
                usage,
                fg_sku,
                fg_sku_name,
                fg_upc,
                fg_product_code,
                fg_product_group,
                fg_manufacturer,
                fg_status,
                component_index,
                component_type,
                component_version,
                component_unit,
                component_vendor,
                component_vendor_id,
                index,
                latest,
                _fivetran_synced
            )
            with bom_raw as (
                select *
                from pc_fivetran_db.s2s_boms.bom_master_bom
            ),
            sku_ref as (
                select *
                from prod_sf_tables.sku_ref.master
            ),
            final_boms as (
                select
                    b.component_sku,
                    co.sku_name as component_name,
                    case
                        when co.sku_type = 'Finished Good' then true
                        else false
                    end as is_fg,
                    b.usage,
                    b.fg_sku,
                    fg.sku_name as fg_sku_name,
                    fg.upc as fg_upc,
                    fg.product_code as fg_product_code,
                    fg.product_group as fg_product_group,
                    fg.vendor as fg_manufacturer,
                    fg.status as fg_status,
                    row_number() over (partition by fg_sku order by component_sku) as component_index,
                    co.product_group as component_type,
                    co.version as component_version,
                    co.unit as component_unit,
                    co.vendor as component_vendor,
                    co.vendor_id as component_vendor_id,
                    (b._line + 1) as index,
                    true as latest,
                    b._fivetran_synced
                from bom_raw b
                left join sku_ref co on b.component_sku = co.sku
                left join sku_ref fg on b.fg_sku = fg.sku
            )
            select
                component_sku,
                component_name,
                is_fg,
                usage,
                fg_sku,
                fg_sku_name,
                fg_upc,
                fg_product_code,
                fg_product_group,
                fg_manufacturer,
                fg_status,
                component_index,
                component_type,
                component_version,
                component_unit,
                component_vendor,
                component_vendor_id,
                index,
                latest,
                _fivetran_synced
            from final_boms
            ;`,
        ];

// Execute each SQL statement
    try {
        sqlScript.forEach((sql) => {
        let stmt = snowflake.createStatement({ sqlText: sql });
        stmt.execute();
        });
        return "Success: Bill of Materials table updated.";
    } catch (err) {
        return "Error: " + err.message;
    }
$$;

call boms.update_boms();

CREATE OR REPLACE TASK update_boms_daily
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 2 3 * * * America/New_York'
    TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9'
    TIMESTAMP_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9'
    TIMEZONE = 'America/New_York'
AS
    call boms.update_all_boms();

ALTER TASK update_boms_daily RESUME;

select b.component_sku, s.vendor_sku from prod_sf_tables.boms.boms b left join prod_sf_tables.sku_ref.master s on b.component_sku = s.sku;

create or replace view boms.boms_by_sku as (
    select
        fg_sku as sku,
        fg_product_name as sku_name,
        fg_upc as upc,
        component_sku as component_sku,
        component_name as component_name,
        component_index as component_index,
        is_fg as is_fg,
        usage as usage,
        component_type as component_type,
        component_version as component_version,
        component_unit as component_unit,
        component_vendor as component_vendor,
        component_vendor_id as component_vendor_id
    from boms.boms
    where latest = true
);

create or replace view boms.boms_fg_missing_usage as (
    select
        fg_sku as sku,
        fg_product_name as sku_name,
        fg_upc as upc,
        count(*) as num_components,
        sum(case when usage is null then 1 else 0 end) as num_missing_usage
    from boms.boms
    where latest = true
    group by
        fg_sku,
        fg_product_name,
        fg_upc
    having
        SUM(case when usage is null then 1 else 0 end) > 0
);

CREATE OR REPLACE VIEW boms_fg_usage_summary AS (
    SELECT
        fg_sku AS sku,
        fg_product_name AS sku_name,
        fg_upc AS upc,
        COUNT(*) AS num_components,
        SUM(CASE WHEN usage IS NULL THEN 1 ELSE 0 END) AS num_missing_usage
    FROM boms.boms
    where latest = true
    GROUP BY
        fg_sku,
        fg_product_name,
        fg_upc
);

create or replace view boms_component_type_matrix as (
    with component_types as (
        select distinct
            fg_sku as sku,
            case when 
        from boms.boms
    )
    select 
        sku
    from boms.boms
);

CREATE OR REPLACE PROCEDURE boms.update_boms_views()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // SQL script as an array of strings
    const sqlScript = [
        `create or replace view boms.boms_by_sku as (
            select
                fg_sku as sku,
                fg_product_name as sku_name,
                fg_upc as upc,
                component_sku as component_sku,
                component_name as component_name,
                component_index as component_index,
                is_fg as is_fg,
                usage as usage,
                component_type as component_type,
                component_version as component_version,
                component_unit as component_unit,
                component_vendor as component_vendor,
                component_vendor_id as component_vendor_id
            from boms.boms
            where latest = true
        );`,
        `create or replace view boms.boms_fg_missing_usage as (
            select
                fg_sku as sku,
                fg_product_name as sku_name,
                fg_upc as upc,
                count(*) as num_components,
                sum(case when usage is null then 1 else 0 end) as num_missing_usage
            from boms.boms
            where latest = true
            group by
                fg_sku,
                fg_product_name,
                fg_upc
            having
                sum(case when usage is null then 1 else 0 end) > 0
        );`,
        `create or replace view boms.boms_fg_usage_summary as (
            select
                fg_sku as sku,
                fg_product_name as sku_name,
                fg_upc as upc,
                count(*) as num_components,
                sum(case when usage is null then 1 else 0 end) as num_missing_usage
            from boms.boms
            where latest = true
            group by
                fg_sku,
                fg_product_name,
                fg_upc
        );`,
    ];

    // Execute each SQL statement
    try {
        sqlScript.forEach((sql) => {
        let stmt = snowflake.createStatement({ sqlText: sql });
        stmt.execute();
        });
        return "Success: BOMs views updated.";
    } catch (err) {
        return "Error: " + err.message;
    }
$$;

call boms.update_boms_views();

CREATE OR REPLACE PROCEDURE boms.update_all_boms()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    try {
        // Call boms.update_boms() procedure
        {
            let sql = `CALL boms.update_boms();`;
            let stmt = snowflake.createStatement({ sqlText: sql });
            stmt.execute();
        }
        
        // Call boms.update_boms_views() procedure
        {
            let sql = `CALL boms.update_boms_views();`;
            let stmt = snowflake.createStatement({ sqlText: sql });
            stmt.execute();
        }
        
        return "Success: All BOMs tables and views updated.";
    } catch (err) {
        return "Error: " + err.message;
    }
$$;

CALL boms.update_all_boms();

