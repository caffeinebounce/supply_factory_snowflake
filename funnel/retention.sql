create or replace view
    basic_retention as
with
    dates as (
        select
            date,
            month_year,
            month_number,
            year,
            month_first_day,
            month_last_day
        from
            prod_sf_tables.reporting.date_spine
        where
            date <= current_date
            and date > '2019-12-31'
    ),
    first_orders as (
        select
            customer_id,
            min(date_trunc('month', created_timestamp))::date as first_order_month
        from
            prod_sf_tables.shopify.orders
        where
            is_confirmed = true
            and is_test_order = false
            and is_deleted = false
            and source_name != 'amazon-us'
        group by
            customer_id
    ),
    monthly_orders as (
        select
            d.month_year,
            d.month_number,
            d.year,
            fo.first_order_month,
            count(*) as new_customers
        from
            first_orders fo
            left join dates d on d.date = fo.first_order_month
        group by
            d.month_year,
            d.month_number,
            d.year,
            fo.first_order_month
    ),
    cumulative_orders as (
        select
            month_year,
            month_number,
            year,
            first_order_month,
            new_customers,
            sum(new_customers) over (
                order by
                    year,
                    month_number
            ) as all_customers,
            row_number() over (
                order by
                    year,
                    month_number
            ) as months_since_inception
        from
            monthly_orders
    ),
    cumulative_order_count as (
        select
            date_trunc('month', o.created_timestamp) as order_month,
            count(o.order_id) as monthly_order_count,
            sum(count(o.order_id)) over (
                order by
                    date_trunc('month', o.created_timestamp)
            ) as cumulative_order_count
        from
            prod_sf_tables.shopify.orders o
        where
            o.is_confirmed = true
            and o.is_test_order = false
            and o.is_deleted = false
            and o.source_name != 'amazon-us'
        group by
            order_month
    ),
    repeat_customers as (
        select
            date_trunc('month', o.created_timestamp) as order_month,
            count(distinct o.customer_id) as repeat_customers
        from
            prod_sf_tables.shopify.orders o
            inner join first_orders fo on o.customer_id = fo.customer_id
        where
            date_trunc('month', o.created_timestamp) > fo.first_order_month
            and o.is_confirmed = true
            and o.is_test_order = false
            and o.is_deleted = false
            and o.source_name != 'amazon-us'
        group by
            order_month
    ),
    weighted_lifespan as (
        select
            a.month_year,
            a.months_since_inception,
            sum(
                b.new_customers * (
                    a.months_since_inception - b.months_since_inception + 1
                )
            ) as weighted_months
        from
            cumulative_orders a
            join cumulative_orders b on b.months_since_inception <= a.months_since_inception
        group by
            a.month_year,
            a.months_since_inception
    ),
    average_lifespan as (
        select
            w.month_year,
            c.month_number,
            c.year,
            c.first_order_month,
            w.months_since_inception,
            c.new_customers,
            c.all_customers,
            w.weighted_months,
            w.weighted_months / c.all_customers as avg_lifespan
        from
            weighted_lifespan w
            left join cumulative_orders c on w.month_year = c.month_year
        order by
            c.year,
            c.month_number
    ),
    final_metrics as (
        select
            a.month_year,
            a.year,
            a.month_number,
            a.first_order_month,
            count(o.order_id) as order_count,
            a.new_customers,
            count(distinct o.customer_id) as unique_customers,
            sum(count(distinct o.customer_id)) over (
                order by
                    a.year,
                    a.month_number rows between unbounded preceding
                    and current row
            ) as cumulative_unique_customers,
            sum(sum(o.total_price)) over (
                order by
                    a.year,
                    a.month_number rows between unbounded preceding
                    and current row
            ) as cumulative_total_revenue,
            coalesce(rc.repeat_customers, 0) as repeat_customers,
            case
                when unique_customers > 0 then (
                    coalesce(rc.repeat_customers, 0)::float / unique_customers::float
                ) * 100
                else 0
            end as repeat_customer_percentage,
            co.cumulative_order_count / sum(count(distinct o.customer_id)) over (
                order by
                    a.year,
                    a.month_number rows between unbounded preceding
                    and current row
            ) as average_lifetime_orders,
            sum(o.total_price) as total_revenue,
            avg(o.total_price) as monthly_aov,
            a.avg_lifespan,
            case
                when sum(count(distinct o.customer_id)) over (
                    order by
                        a.year,
                        a.month_number rows between unbounded preceding
                        and current row
                ) > 0 then sum(sum(o.total_price)) over (
                    order by
                        a.year,
                        a.month_number rows between unbounded preceding
                        and current row
                ) / sum(count(distinct o.customer_id)) over (
                    order by
                        a.year,
                        a.month_number rows between unbounded preceding
                        and current row
                ) / a.avg_lifespan
                else 0
            end as arpu_per_month,
            case
                when sum(count(distinct o.customer_id)) over (
                    order by
                        a.year,
                        a.month_number rows between unbounded preceding
                        and current row
                ) > 0 then (
                    sum(sum(o.total_price)) over (
                        order by
                            a.year,
                            a.month_number rows between unbounded preceding
                            and current row
                    ) / sum(count(distinct o.customer_id)) over (
                        order by
                            a.year,
                            a.month_number rows between unbounded preceding
                            and current row
                    )
                )
                else 0
            end as clv
        from
            average_lifespan a
            left join prod_sf_tables.shopify.orders o on date_trunc('month', o.created_timestamp) = a.first_order_month
            left join repeat_customers rc on rc.order_month = date_trunc('month', o.created_timestamp)
            left join cumulative_order_count co on co.order_month = date_trunc('month', o.created_timestamp)
        where
            o.is_confirmed = true
            and o.is_test_order = false
            and o.is_deleted = false
            and o.source_name != 'amazon-us'
        group by
            a.month_year,
            a.year,
            a.month_number,
            a.first_order_month,
            a.new_customers,
            rc.repeat_customers,
            co.cumulative_order_count,
            a.avg_lifespan
    )
select
    month_year,
    year,
    month_number,
    first_order_month,
    order_count,
    new_customers,
    unique_customers,
    cumulative_unique_customers,
    repeat_customers,
    repeat_customer_percentage,
    average_lifetime_orders,
    cumulative_total_revenue,
    total_revenue,
    monthly_aov,
    arpu_per_month,
    avg_lifespan,
    clv
from
    final_metrics
order by
    year,
    month_number;
