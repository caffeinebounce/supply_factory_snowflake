with tracker as (
    select *
    from po_tracker.tracker
    where status = 'COMPLETE' and
    latest = true
),
sku_ref as (
    select * 
    from prod_sf_tables.sku_ref.master
), 
inventory_history as (
    select *
    from inventory.inventory_history
),
brands as (
    select *
    from prod_sf_tables.sku_ref.brands
),
date_spine as (
    select *
    from reporting.date_spine_to_today
),
int_purchases as (
    select 
        t.master_po_num,
        t.sku as sku,
        sr.sku_name as sku_name,
        round(sum (t.total), 2) as total_purchase_cost,
        sum(t.quantity) as total_ordered,
        sum(t.quantity_received) as total_received,
        t.per_unit_order as per_unit,
        t.status as status,
        t.delivery_date_actual as delivery_date,
        t.ship_date_actual as ship_date,
        t.po_date as po_date,
        try_to_date(left(t._fivetran_synced, 10)) as last_updated
    from tracker t
    left join sku_ref sr on t.sku = sr.sku
    group by t.master_po_num, t.status, t.sku, sr.sku_name, t.per_unit_order, t.delivery_date_actual, t.ship_date_actual, t.po_date, t._fivetran_synced
),
purchases as (
    select
        master_po_num,
        sku,
        sku_name,
        total_purchase_cost,
        total_ordered,
        total_received,
        per_unit,
        case
            when total_received is not null then (total_received - total_ordered)
            else null
        end as received_delta,
        po_date,
        ship_date,
        delivery_date, 
        status,
        last_updated
    from int_purchases
),
purchases_average_cost as (
    select 
        p.master_po_num,
        p.sku,
        p.sku_name,
        total_purchase_cost,
        total_ordered,
        total_received,
        coalesce(po_date, ship_date, delivery_date) as po_date,
        round(sum(p.total_purchase_cost) over (partition by p.sku order by p.po_date) / sum(p.total_ordered) over (partition by p.sku order by p.po_date), 2) as average_inventory_cost_at_po_date,
        p.delivery_date, 
        round(sum(p.total_purchase_cost) over (partition by p.sku order by p.po_date) / sum(coalesce(p.total_received, p.total_ordered)) over (partition by p.sku order by p.po_date), 2) as adjusted_average_cost_based_on_received,
        round((p.total_purchase_cost / coalesce(p.total_received, p.total_ordered)), 2) as average_effective_price 
    from purchases p
),
daily_purchase_history as (
    select
        ih.date,
        ih.sku,
        sum(pac.total_purchase_cost) as daily_total_purchase_cost_by_sku,
        sum(pac.total_ordered) as daily_total_purchased_units_by_sku,
        sum(pac.total_received) as daily_total_received_units_by_sku
    from inventory_history ih
    left join purchases_average_cost pac on ih.sku = pac.sku and ih.date = pac.po_date
    group by ih.date, ih.sku
),
cumulative_purchase_history as (
    select
        dph.date,
        dph.sku,
        sum(dph.daily_total_purchase_cost_by_sku) over (partition by dph.sku order by dph.date) as cumulative_total_purchase_cost_by_sku,
        sum(dph.daily_total_purchased_units_by_sku) over (partition by dph.sku order by dph.date) as cumulative_total_purchased_units_by_sku,
        sum(dph.daily_total_received_units_by_sku) over (partition by dph.sku order by dph.date) as cumulative_total_received_units_by_sku
    from daily_purchase_history dph
),
inventory_purchase_history as (
    select
        cph.date,
        cph.sku,
        cph.cumulative_total_purchase_cost_by_sku,
        cph.cumulative_total_purchased_units_by_sku,
        cph.cumulative_total_purchase_cost_by_sku / cph.cumulative_total_purchased_units_by_sku as weighted_avg_order_price_based_on_pos,
        (cph.cumulative_total_purchase_cost_by_sku - sum(pac.total_purchase_cost - (coalesce(pac.total_received, pac.total_ordered) * pac.adjusted_average_cost_based_on_received)) over (partition by cph.sku order by cph.date)) / cph.cumulative_total_purchased_units_by_sku as weighted_avg_order_price_based_on_received
    from cumulative_purchase_history cph
    left join purchases_average_cost pac on cph.sku = pac.sku and cph.date = pac.po_date
),
purchase_timeline as (
    select 
        sku,
        effective_from,
        effective_to,
        sum(total_purchase_cost) over (partition by sku order by effective_from) as total_cost,
        sum(total_ordered) over (partition by sku order by effective_from) as total_ordered,
        sum(total_received) over (partition by sku order by effective_from) as total_received,
        avg(per_unit) over (partition by sku order by effective_from) as average_per_unit,
        max(last_updated) over (partition by sku order by effective_from) as last_updated
    from purchases_with_effective_dates
),
average_price_timeline as (
    select    
        sku,
        effective_from,
        effective_to,
        total_cost,
        total_ordered,
        total_received,
        round((total_cost / total_ordered), 2) as average_po_price_order,
        round((total_cost / total_received), 2) as average_po_price_effective,
        average_per_unit,
        last_updated
    from purchase_timeline
),
final_average_price_table as (
    select 
        sr.sku,
        sr.sku_name,
        sr.product_type,
        sr.upc,
        b.brand_name as brand,
        sr.status,
        sr.vendor,
        sr.vendor_id,
        apt.average_price_order,
        apt.average_price_effective,
        apt.average_per_unit,
        case
            when apt.average_price_effective is not null then round((apt.average_price_effective - apt.average_price_order), 2)
            else null
        end as order_effective_delta,
        coalesce(average_price_effective, average_price_order, average_per_unit) as average_price,
        apt.last_updated as last_updated
    from sku_ref sr
    left join average_price_timeline apt on sr.sku = apt.sku
    left join brands b on sr.brand = b.brand_id
    where apt.last_updated is not null
)
select
    sku,
    sku_name,
    product_type,
    upc,
    brand,
    status,
    vendor,
    vendor_id,
    average_price_order,
    average_price_effective,
    average_per_unit,
    order_effective_delta,
    average_price,
    null as price_effective_from,
    null as price_effective_to,
    last_updated,
    true as latest
from final_average_price_table
;
create or replace view po_tracker.average_price_summary_all as 
    select
        sku,
        sku_name, 
        product_type,
        upc,
        status,
        average_price_effective as average_price
    from po_tracker.average_price
    where latest = true
;
create or replace view po_tracker.average_price_summary_all_active as 
    select
        sku,
        sku_name, 
        product_type,
        upc,
        status,
        average_price_effective as average_price
    from po_tracker.average_price
    where latest = true and 
    status = 'Active'
;
create or replace view po_tracker.average_price_summary_fg as 
    select
        sku,
        sku_name, 
        product_type,
        upc,
        status,
        average_price_effective as average_price
    from po_tracker.average_price
    where latest = true and 
    product_type = 'Finished Good'
;
create or replace view po_tracker.average_price_summary_fg_active as 
    select
        sku,
        sku_name, 
        product_type,
        upc,
        status,
        average_price_effective as average_price
    from po_tracker.average_price
    where latest = true and 
    product_type = 'Finished Good' and 
    status = 'Active'
;

-- Creating the procedure
CREATE OR REPLACE PROCEDURE update_po_tracker_average_price()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // SQL script as an array of strings
    const sqlScript = [
        `   update po_tracker.average_price set latest = false;`,
        `   insert into po_tracker.average_price (
                sku,
                sku_name,
                product_type,
                upc,
                brand,
                status,
                vendor,
                vendor_id,
                average_price_order,
                average_price_effective,
                average_per_unit,
                order_effective_delta,
                average_price,
                last_updated,
                latest
            )
            with tracker as (
                select *
                from po_tracker.tracker
                where status = 'COMPLETE' and
                latest = true
            ),
            sku_ref as (
                select * 
                from prod_sf_tables.sku_ref.master
            ), 
            brands as (
                select *
                from prod_sf_tables.sku_ref.brands
            ),
            int_purchases as (
                select 
                    t.master_po_num,
                    t.sku as sku,
                    sr.sku_name as sku_name,
                    round(sum (t.total), 2) as total_purchase_cost,
                    sum(t.quantity) as total_ordered,
                    sum(t.quantity_received) as total_received,
                    t.per_unit_order as per_unit,
                    t.status as status,
                    t.delivery_date_actual as delivery_date,
                    t.ship_date_actual as ship_date,
                    t.po_date as po_date,
                    try_to_date(left(t._fivetran_synced, 10)) as last_updated
                from tracker t
                left join sku_ref sr on t.sku = sr.sku
                group by t.master_po_num, t.status, t.sku, sr.sku_name, t.per_unit_order, t.delivery_date_actual, t.ship_date_actual, t.po_date, t._fivetran_synced
            ),
            purchases as (
                select
                    master_po_num,
                    sku,
                    sku_name,
                    total_purchase_cost,
                    total_ordered,
                    total_received,
                    per_unit,
                    case
                        when total_received is not null and status = 'COMPLETE' then (total_received - total_ordered)
                        else null
                    end as received_delta,
                    coalesce(delivery_date, ship_date, po_date) as purchase_date,
                    status,
                    last_updated
                from int_purchases
            ),
            aggregated_values as (
                select 
                    sku,
                    sum(total_purchase_cost) as total_purchase_cost_sum,
                    sum(total_ordered) as total_ordered_sum,
                    sum(total_received) as total_received_sum,
                    round(avg(per_unit), 2) as average_per_unit,
                    max(last_updated) as last_updated
                from purchases
                group by sku
            ),
            average_price_calcs as (
                select
                    ag.sku,
                    round(ag.total_purchase_cost_sum / ag.total_ordered_sum, 2) as average_price_order,
                    round(ag.total_purchase_cost_sum / ag.total_received_sum, 2) as average_price_effective,
                    coalesce(ag.average_per_unit, 0) as average_per_unit,
                    ag.last_updated as last_updated
                from aggregated_values ag
            ),
            final_average_price_table as (
                select 
                    sr.sku,
                    sr.sku_name,
                    sr.product_type,
                    sr.upc,
                    b.brand_name as brand,
                    sr.status,
                    sr.vendor,
                    sr.vendor_id,
                    apt.average_price_order,
                    apt.average_price_effective,
                    apt.average_per_unit,
                    case
                        when apt.average_price_effective is not null then round((apt.average_price_effective - apt.average_price_order), 2)
                        else null
                    end as order_effective_delta,
                    coalesce(average_price_effective, average_price_order, average_per_unit) as average_price,
                    apt.last_updated as last_updated
                from sku_ref sr
                left join average_price_calcs apt on sr.sku = apt.sku
                left join brands b on sr.brand = b.brand_id
                where apt.last_updated is not null
            )
            select
                sku,
                sku_name,
                product_type,
                upc,
                brand,
                status,
                vendor,
                vendor_id,
                average_price_order,
                average_price_effective,
                average_per_unit,
                order_effective_delta,
                average_price,
                last_updated,
                true as latest
            from final_average_price_table
            ;`,
        `   create or replace view po_tracker.average_price_summary_all as 
                select
                    sku,
                    sku_name, 
                    product_type,
                    upc,
                    status,
                    average_price
                from po_tracker.average_price
                where latest = true
            ;`,
        `   create or replace view po_tracker.average_price_summary_all_active as 
                select
                    sku,
                    sku_name, 
                    product_type,
                    upc,
                    status,
                    average_price
                from po_tracker.average_price
                where latest = true and 
                status = 'Active'
            ;`,
        `   create or replace view po_tracker.average_price_summary_fg as 
                select
                    sku,
                    sku_name, 
                    product_type,
                    upc,
                    status,
                    average_price
                from po_tracker.average_price
                where latest = true and 
                product_type = 'Finished Good'
            ;`,
        `   create or replace view po_tracker.average_price_summary_fg_active as 
                select
                    sku,
                    sku_name, 
                    product_type,
                    upc,
                    status,
                    average_price
                from po_tracker.average_price
                where latest = true and 
                product_type = 'Finished Good' and 
                status = 'Active'
            ;`
    ];

// Execute each SQL statement
    try {
        sqlScript.forEach((sql) => {
        let stmt = snowflake.createStatement({ sqlText: sql });
        stmt.execute();
        });
        return "Success: PO Tracker Average Price tables updated.";
    } catch (err) {
        return "Error: " + err.message;
    }
$$;

CALL update_po_tracker_average_price();

create or replace view po_tracker.average_price_summary_all as 
                select
                    sku,
                    sku_name, 
                    product_type,
                    upc,
                    status,
                    average_price
                from po_tracker.average_price
                where latest = true

update po_tracker.average_price
set latest = false;
insert into po_tracker.average_price (
    sku,
    sku_name,
    product_type,
    upc,
    brand,
    status,
    vendor,
    vendor_id,
    average_price_order,
    average_price_effective,
    average_per_unit,
    order_effective_delta,
    average_price,
    price_effective_from,
    price_effective_to,
    last_updated,
    latest
);