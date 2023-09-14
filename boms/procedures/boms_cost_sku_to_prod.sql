update boms.sku set latest = false;
create or replace view ()
insert into boms.sku (
    sku,
    name,
    upc,
    product_code,
    product_group,
    status,
    total_cost
    latest,
    last_updated
);
with boms as (
    select *
    from boms.boms
),
sku_ref as (
    select *
    from prod_sf_tables.sku_ref.master
),
avg_prices as (
    select *
    from prod_sf_tables.po_tracker.average_price_summary_all
),
avg_price_boms as (
    select distinct
        b.component_sku,
        b.component_name,
        b.fg_sku,
        b.fg_product_name,
        a.average_price
    from boms b
    left join avg_prices a on b.component_sku = a.sku
)
select * from avg_price_boms;
ag_avg_price_boms as (
    select
        fg_sku,
        sum(average_price) as total_cost
    from avg_price_boms
    group by fg_sku
)
select * from ag_avg_price_boms;
avg_sku_join as (
    select
        sr.sku,
        ag.total_cost
    from  sku_ref sr
    left join ag_avg_price_boms ag on ag.fg_sku = sr.sku
)
select * from avg_sku_join; 
finished_goods as (
    select
        sku,
        product_name, 
        upc,
        product_code,
        product_group,
        status,
        ag.total_cost
    from sku_ref sr
    left join ag_avg_price_boms ag on ag.fg_sku = sr.sku
    where product_type = 'Finished Good' and 
    status = 'Active'
    group by sku
)
select * from finished_goods;
final_boms_sku as (,
    select
        sku,
        product_name as name,
        upc,
        product_code,
        product_group,
        status,
        sum(a.average_price) as total_cost
        latest,
        last_updated
    from finished_goods f
    left join avg_price_boms a on 
)
select
    sku,
    name,
    upc,
    product_code,
    product_group,
    status,
    total_cost,
    latest,
    last_updated
from final_boms_sku;

CREATE OR REPLACE PROCEDURE update_boms()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    const sqlScript = [
        `   update boms.boms set latest = false;`,
        `   insert into boms.boms (
                component_sku,
                component_name,
                is_fg,
                usage,
                fg_sku,
                fg_product_name,
                fg_upc,
                fg_product_code,
                fg_product_group,
                fg_manufacturer,
                fg_status,
                component_type,
                component_version,
                unit,
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
                    co.product_name as component_name,
                    case
                        when co.product_type = 'Finished Good' then true
                        else false
                    end as is_fg,
                    b.usage,
                    b.fg_sku,
                    fg.product_name as fg_product_name,
                    fg.upc as fg_upc,
                    fg.product_code as fg_product_code,
                    fg.product_group as fg_product_group,
                    fg.vendor as fg_manufacturer,
                    fg.status as fg_status,
                    co.product_group as component_type,
                    co.version as component_version,
                    co.unit as unit,
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
                fg_product_name,
                fg_upc,
                fg_product_code,
                fg_product_group,
                fg_manufacturer,
                fg_status,
                component_type,
                component_version,
                unit,
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

call update_boms_sku();

CREATE OR REPLACE TASK update_boms_daily
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 2 3 * * * America/New_York'
    TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9'
    TIMESTAMP_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9'
    TIMEZONE = 'America/New_York'
AS
    CALL update_boms();

ALTER TASK update_boms_daily RESUME;
