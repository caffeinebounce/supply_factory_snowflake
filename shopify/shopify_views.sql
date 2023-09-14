// this is a table summarizing the shopify daily shop table

create or replace view daily_shop_key as (
    select
        date_day,
        shop_id,
        currency,
        count_orders as order_count,
        quantity_sold as items_sold,
        quantity_refunded as items_refunded,
        avg_line_item_count as avg_items_per_order,
        count_customers as total_customers,
        order_adjusted_total as gross_sales,
        total_discounts,
        refund_subtotal,
        refund_total_tax,
        shipping_cost,
        shipping_discount_amount,
        order_adjustment_amount,
        order_adjustment_tax_amount,
        count_discount_codes_applied as discount_code_applied,
        count_orders_with_discounts,
        count_orders_with_refunds,  
        count_variants_sold as variants_sold,
        count_products_sold as products_sold,
        quantity_gift_cards_sold,
        quantity_requiring_shipping,
        count_abandoned_checkouts,
        count_customers_abandoned_checkout,
        count_fulfillment_attempted_delivery,
        count_fulfillment_delivered,
        count_fulfillment_failure,
        count_fulfillment_confirmed
    from prod_sf_tables.shopify.daily_shop
    where is_deleted = false
    );



