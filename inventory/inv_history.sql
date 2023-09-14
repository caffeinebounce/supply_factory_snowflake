insert into inventory.inventory_history (
    sku,
    sku_name,
    upc,
    product_type,
    status,
    warehouse,
    date,
    total_on_hand,
    total_on_hold,
    total_available,
    month_end,
    _row_id
)
with hanzo as (
    select * 
    from hanzo.inventory
),
extensiv as (
    select * 
    from extensiv.inventory_history
),
date_spine as (
    select *
    from reporting.date_spine_to_today
),
warehouse_map as (
    select *
    from inventory.warehouse_map
),
hanzo_sku as (
    select
        case
            when w.sku_ref is not null then w.sku_ref
            else h.sku
        end as sku,
        h.sku_name,
        quantity_on_hand as on_hand,
        quantity_allocated as on_hold,
        quantity_available as available,
        report_date as inv_date,
        case
            when last_day(report_date) = report_date then true
            else false
        end as month_end
    from hanzo h 
    left join warehouse_map w on w.warehouse_sku = h.sku
),
ag_hanzo as (
    select
        sku,
        sku_name,
        sum(on_hand) as total_on_hand,
        sum(on_hold) as total_on_hold,
        sum(available) as total_available,
        inv_date, 
        month_end
    from hanzo_sku
    group by sku, sku_name, inv_date, month_end
),
hanzo_history as (
    select
        ag.sku,
        case
            when sr.sku_name is null then ag.sku_name
            else sr.sku_name
        end as sku_name,
        sr.upc,
        case 
            when sr.product_type is null then 'Component'
            else sr.product_type
        end as product_type,
        case 
            when sr.status is null then 'Active'
            else sr.status
        end as status,
        'Hanzo' as warehouse,
        ag.total_on_hand,
        ag.total_on_hold,
        ag.total_available,
        inv_date,
        month_end
    from ag_hanzo ag
    left join sku_ref.master sr on sr.sku = ag.sku
),
extensiv_sku as (
    select
        case
            when w.sku_ref is not null then w.sku_ref
            else e.sku
        end as sku,
        sku_name,
        on_hand,
        on_hold,
        available,
        to_date(inv_run) as inv_date,
        case
            when last_day(to_date(inv_run)) = to_date(inv_run) then true
            else false
        end as month_end
    from extensiv e
    left join warehouse_map w on w.warehouse_sku = e.sku
),
ag_extensiv as (
    select
        sku,
        sku_name,
        sum(on_hand) as total_on_hand,
        sum(on_hold) as total_on_hold,
        sum(available) as total_available,
        inv_date, 
        month_end
    from extensiv_sku
    group by sku, sku_name, inv_date, month_end
),
extensiv_history as (
    select
        ag.sku as sku,
        case
            when sr.sku_name is null then ag.sku
            else sr.sku_name
        end as sku_name,
        sr.upc as upc,
        case 
            when ag.sku like 'MISCSUPPLY%' then 'Component'
            when ag.sku like 'WorkOrder%' then 'Service SKU'
            when ag.sku like '%Pallet%' then 'Pallet'
            when ag.sku like '%Return%' then 'Return'
            else sr.product_type
        end as product_type,
        case 
            when sr.status is null then 'Active'
            else sr.status
        end as status,
        'Hopkins - Reno' as warehouse,
        ag.total_on_hand,
        ag.total_on_hold,
        ag.total_available,
        inv_date,
        ag.month_end
    from ag_extensiv ag
    left join sku_ref.master sr on sr.sku = ag.sku
    where total_on_hand is not null
),
history_join as (
    select 
        sku,
        sku_name,
        upc,
        product_type,
        status,
        warehouse,
        total_on_hand,
        total_on_hold,
        total_available,
        inv_date,
        month_end 
    from hanzo_history
    union all
    select 
        sku,
        sku_name,
        upc,
        product_type,
        status,
        warehouse,
        total_on_hand,
        total_on_hold,
        total_available,
        inv_date,
        month_end 
    from extensiv_history
),
distinct_sku_warehouse as (
    select distinct
        sku,
        sku_name,
        warehouse
    from history_join
),
date_crossjoin as (
    select 
        s.sku,
        s.sku_name,
        s.warehouse,
        d.date
    from distinct_sku_warehouse s
    cross join date_spine d
),
date_join as (
    select 
        dc.date,
        dc.sku,
        dc.sku_name,
        h.upc,
        h.product_type,
        h.status,
        dc.warehouse,
        coalesce(
            h.total_on_hand,
            last_value(h.total_on_hand ignore nulls) over (partition by dc.sku, dc.warehouse order by dc.date rows between unbounded preceding and 1 preceding)
        ) as total_on_hand,
        coalesce(
            h.total_on_hold,
            last_value(h.total_on_hold ignore nulls) over (partition by dc.sku, dc.warehouse order by dc.date rows between unbounded preceding and 1 preceding)
        ) as total_on_hold,
        coalesce(
            h.total_available,
            last_value(h.total_available ignore nulls) over (partition by dc.sku, dc.warehouse order by dc.date rows between unbounded preceding and 1 preceding)
        ) as total_available,
        h.month_end,
        lower(to_varchar(sha2(dc.warehouse|| '-' || dc.date || '-' || dc.sku, 256))) as _row_id
    from date_crossjoin dc
    left join history_join h on dc.date = h.inv_date and dc.sku = h.sku and dc.warehouse = h.warehouse
)
select 
    sku,
    sku_name,
    upc,
    product_type,
    status,
    warehouse,
    date,
    total_on_hand,
    total_on_hold,
    total_available,
    month_end,
    _row_id
from date_join
where not exists (
    select 1
    from inventory.inventory_history i
    where i._row_id = date_join._row_id
);


insert into inventory.warehouse_map (
    warehouse_sku,
    sku_ref
)
with map as (
    select *
    from pc_fivetran_db.s2s_inv_warehouse_map.warehouse_mapping_mapping
),
final_map as (
    select
        sku as warehouse_sku,
        sku_ref as sku_ref,
        match
    from map
)
select
    warehouse_sku,
    sku_ref
from final_map
where match = false and 
sku_ref is not null
and not exists (
    select 1
    from inventory.warehouse_map i
    where i.sku_ref = final_map.sku_ref
);

CREATE OR REPLACE PROCEDURE inventory.update_inventory_history()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    const sqlScript = [
        `   insert into inventory.warehouse_map (
                warehouse_sku,
                sku_ref
            )
            with map as (
                select *
                from pc_fivetran_db.s2s_inv_warehouse_map.warehouse_mapping_mapping
            ),
            final_map as (
                select
                    sku as warehouse_sku,
                    sku_ref as sku_ref,
                    match
                from map
            )
            select
                warehouse_sku,
                sku_ref
            from final_map
            where match = false and 
            sku_ref is not null
            and not exists (
                select 1
                from inventory.warehouse_map i
                where i.sku_ref = final_map.sku_ref
            );`,
        `   insert into inventory.inventory_history (
                sku,
                sku_name,
                upc,
                product_type,
                status,
                warehouse,
                date,
                total_on_hand,
                total_on_hold,
                total_available,
                month_end,
                _row_id
            )
            with hanzo as (
                select * 
                from hanzo.inventory
            ),
            extensiv as (
                select * 
                from extensiv.inventory_history
            ),
            date_spine as (
                select *
                from reporting.date_spine_to_today
            ),
            warehouse_map as (
                select *
                from inventory.warehouse_map
            ),
            hanzo_sku as (
                select
                    case
                        when w.sku_ref is not null then w.sku_ref
                        else h.sku
                    end as sku,
                    h.sku_name,
                    quantity_on_hand as on_hand,
                    quantity_allocated as on_hold,
                    quantity_available as available,
                    report_date as inv_date,
                    case
                        when last_day(report_date) = report_date then true
                        else false
                    end as month_end
                from hanzo h 
                left join warehouse_map w on w.warehouse_sku = h.sku
            ),
            ag_hanzo as (
                select
                    sku,
                    sku_name,
                    sum(on_hand) as total_on_hand,
                    sum(on_hold) as total_on_hold,
                    sum(available) as total_available,
                    inv_date, 
                    month_end
                from hanzo_sku
                group by sku, sku_name, inv_date, month_end
            ),
            hanzo_history as (
                select
                    ag.sku,
                    case
                        when sr.sku_name is null then ag.sku_name
                        else sr.sku_name
                    end as sku_name,
                    sr.upc,
                    case 
                        when sr.product_type is null then 'Component'
                        else sr.product_type
                    end as product_type,
                    case 
                        when sr.status is null then 'Active'
                        else sr.status
                    end as status,
                    'Hanzo' as warehouse,
                    ag.total_on_hand,
                    ag.total_on_hold,
                    ag.total_available,
                    inv_date,
                    month_end
                from ag_hanzo ag
                left join sku_ref.master sr on sr.sku = ag.sku
            ),
            extensiv_sku as (
                select
                    case
                        when w.sku_ref is not null then w.sku_ref
                        else e.sku
                    end as sku,
                    sku_name,
                    on_hand,
                    on_hold,
                    available,
                    to_date(inv_run) as inv_date,
                    case
                        when last_day(to_date(inv_run)) = to_date(inv_run) then true
                        else false
                    end as month_end
                from extensiv e
                left join warehouse_map w on w.warehouse_sku = e.sku
            ),
            ag_extensiv as (
                select
                    sku,
                    sku_name,
                    sum(on_hand) as total_on_hand,
                    sum(on_hold) as total_on_hold,
                    sum(available) as total_available,
                    inv_date, 
                    month_end
                from extensiv_sku
                group by sku, sku_name, inv_date, month_end
            ),
            extensiv_history as (
                select
                    ag.sku as sku,
                    case
                        when sr.sku_name is null then ag.sku
                        else sr.sku_name
                    end as sku_name,
                    sr.upc as upc,
                    case 
                        when ag.sku like 'MISCSUPPLY%' then 'Component'
                        when ag.sku like 'WorkOrder%' then 'Service SKU'
                        when ag.sku like '%Pallet%' then 'Pallet'
                        when ag.sku like '%Return%' then 'Return'
                        else sr.product_type
                    end as product_type,
                    case 
                        when sr.status is null then 'Active'
                        else sr.status
                    end as status,
                    'Hopkins - Reno' as warehouse,
                    ag.total_on_hand,
                    ag.total_on_hold,
                    ag.total_available,
                    inv_date,
                    ag.month_end
                from ag_extensiv ag
                left join sku_ref.master sr on sr.sku = ag.sku
                where total_on_hand is not null
            ),
            history_join as (
                select 
                    sku,
                    sku_name,
                    upc,
                    product_type,
                    status,
                    warehouse,
                    total_on_hand,
                    total_on_hold,
                    total_available,
                    inv_date,
                    month_end 
                from hanzo_history
                union all
                select 
                    sku,
                    sku_name,
                    upc,
                    product_type,
                    status,
                    warehouse,
                    total_on_hand,
                    total_on_hold,
                    total_available,
                    inv_date,
                    month_end 
                from extensiv_history
            ),
            distinct_sku_warehouse as (
                select distinct
                    sku,
                    sku_name,
                    warehouse
                from history_join
            ),
            date_crossjoin as (
                select 
                    s.sku,
                    s.sku_name,
                    s.warehouse,
                    d.date
                from distinct_sku_warehouse s
                cross join date_spine d
            ),
            date_join as (
                select 
                    dc.date,
                    dc.sku,
                    dc.sku_name,
                    h.upc,
                    h.product_type,
                    h.status,
                    dc.warehouse,
                    coalesce(
                        h.total_on_hand,
                        last_value(h.total_on_hand ignore nulls) over (partition by dc.sku, dc.warehouse order by dc.date rows between unbounded preceding and 1 preceding)
                    ) as total_on_hand,
                    coalesce(
                        h.total_on_hold,
                        last_value(h.total_on_hold ignore nulls) over (partition by dc.sku, dc.warehouse order by dc.date rows between unbounded preceding and 1 preceding)
                    ) as total_on_hold,
                    coalesce(
                        h.total_available,
                        last_value(h.total_available ignore nulls) over (partition by dc.sku, dc.warehouse order by dc.date rows between unbounded preceding and 1 preceding)
                    ) as total_available,
                    h.month_end,
                    lower(to_varchar(sha2(dc.warehouse|| '-' || dc.date || '-' || dc.sku, 256))) as _row_id
                from date_crossjoin dc
                left join history_join h on dc.date = h.inv_date and dc.sku = h.sku and dc.warehouse = h.warehouse
            )
            select 
                sku,
                sku_name,
                upc,
                product_type,
                status,
                warehouse,
                date,
                total_on_hand,
                total_on_hold,
                total_available,
                month_end,
                _row_id
            from date_join
            where not exists (
                select 1
                from inventory.inventory_history i
                where i._row_id = date_join._row_id
            );`,
        ];

    // Execute each SQL statement
    try {
        sqlScript.forEach((sql) => {
        let stmt = snowflake.createStatement({ sqlText: sql });
        stmt.execute();
        });
        return "Success: Inventory History tables updated.";
    } catch (err) {
        return "Error: " + err.message;
    }
$$;

CREATE OR REPLACE TASK update_inventory_history_daily
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 2 * * * America/New_York'
    TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9'
    TIMESTAMP_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9'
    TIMEZONE = 'America/New_York'
AS
    CALL update_inventory_history();


ALTER TASK update_inventory_history_daily RESUME;

CALL update_inventory_history();