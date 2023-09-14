create schema prod_sf_tables.inventory;
create or replace view inventory.sku_match as
with hanzo as (
    select * from hanzo.inventory
),
extensiv as (
    select * from extensiv.inventory_history
),
hanzo_sku as (
    select
        sku,
        sku_name,
        upc,
        sku_description,
        row_number() over (partition by sku order by index) as row_num
    from hanzo
),
hanzo_filter as (
    select * 
    from hanzo_sku
    where row_num = 1
),
hanzo_match as (
    select
        'Hanzo' as warehouse,
        h.sku,
        h.sku_name,
        h.upc,
        h.sku_description,
        case
            when sr.sku is not null then true
            else false
        end as match,
        case
            when sr.sku is not null then sr.sku
            else null
        end as sku_ref
    from hanzo_filter h
    left join sku_ref.master sr on sr.sku = h.sku
),
extensiv_sku as (
    select
        sku,
        sku_name,
        upc,
        null as sku_description,
        row_number() over (partition by sku order by on_hand) as row_num
    from extensiv
),
extensiv_filter as (
    select * 
    from extensiv_sku
    where row_num = 1
),
extensiv_match as (
    select
        'Hopkins' as warehouse,
        e.sku,
        e.sku_name,
        e.upc,
        e.sku_description,
        case
            when sr.sku is not null then true
            else false
        end as match,
        case
            when sr.sku is not null then sr.sku
            else null
        end as sku_ref
    from extensiv_filter e
    left join sku_ref.master sr on sr.sku = e.sku
), 
table_join as (
    select * from hanzo_match
    union all
    select * from extensiv_match
) 
select * from table_join;

