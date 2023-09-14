insert into truecommerce.qb_mapping (
    qb_id,
    qb_name,
    qb_fq_name, 
    tc_name
)

with qb_customer as (
    select distinct
        id, 
        fully_qualified_name,
        display_name
    from pc_fivetran_db.quickbooks.customer
), 

tc_trading_partners as (
    select distinct
        textbox_23 as trading_partner
    from pc_fivetran_db.s2s_truecommerce.txr
), 

int_tc_qb_map as (
    select
        qb.id as qb_id,
        qb.display_name as qb_name,
        qb.fully_qualified_name as qb_fq_name,
        tc.trading_partner as tc_name,
        JAROWINKLER_SIMILARITY(qb.display_name, tc.trading_partner) AS similarity,
        ROW_NUMBER() OVER (PARTITION BY tc.trading_partner ORDER BY JAROWINKLER_SIMILARITY(qb.display_name, tc.trading_partner) DESC) AS row_num
    FROM qb_customer qb
    CROSS JOIN tc_trading_partners tc
    WHERE JAROWINKLER_SIMILARITY(qb.display_name, tc.trading_partner) >= 85
),

tc_qb_map as (
    select
        qb_id,
        qb_name,
        qb_fq_name, 
        tc_name
    from int_tc_qb_map
    where row_num = 1
)

select
    qb_id,
    qb_name,
    qb_fq_name, 
    tc_name
from tc_qb_map
where not exists (
    select 1
    from truecommerce.qb_mapping tq
    where tq.qb_id = tc_qb_map.qb_id);

CREATE OR REPLACE PROCEDURE truecommerce.update_qb_mapping()
RETURNS FLOAT
LANGUAGE JAVASCRIPT
AS
$$
    var count_before_command = `SELECT COUNT(*) FROM truecommerce.qb_mapping;`;
    var insert_command = `
        insert into truecommerce.qb_mapping (
            qb_id,
            qb_name,
            qb_fq_name, 
            tc_name
        )

        with qb_customer as (
            select distinct
                id, 
                fully_qualified_name,
                display_name
            from pc_fivetran_db.quickbooks.customer
        ), 

        tc_trading_partners as (
            select distinct
                textbox_23 as trading_partner
            from pc_fivetran_db.s2s_truecommerce.txr
        ), 

        int_tc_qb_map AS (
            SELECT
                qb.id as qb_id,
                qb.display_name as qb_name,
                qb.fully_qualified_name as qb_fq_name,
                tc.trading_partner as tc_name,
                JAROWINKLER_SIMILARITY(qb.display_name, tc.trading_partner) AS similarity,
                ROW_NUMBER() OVER (PARTITION BY tc.trading_partner ORDER BY JAROWINKLER_SIMILARITY(qb.display_name, tc.trading_partner) DESC) AS row_num
            FROM qb_customer qb
            CROSS JOIN tc_trading_partners tc
            WHERE JAROWINKLER_SIMILARITY(qb.display_name, tc.trading_partner) >= 85
        ),

        tc_qb_map as (
            select
                qb_id,
                qb_name,
                qb_fq_name, 
                tc_name
            from int_tc_qb_map
            where row_num = 1
        )

        select
            qb_id,
            qb_name,
            qb_fq_name, 
            tc_name
        from tc_qb_map
        WHERE NOT EXISTS (
            SELECT 1
            FROM truecommerce.qb_mapping tq
            WHERE tq.qb_id = tc_qb_map.qb_id
        );
    `;
    var count_after_command = `SELECT COUNT(*) FROM truecommerce.qb_mapping;`;

    var count_before_stmt = snowflake.createStatement({sqlText: count_before_command});
    var insert_stmt = snowflake.createStatement({sqlText: insert_command});
    var count_after_stmt = snowflake.createStatement({sqlText: count_after_command});

    var count_before_result = count_before_stmt.execute();
    count_before_result.next();
    var count_before = count_before_result.getColumnValue(1);

    insert_stmt.execute();

    var count_after_result = count_after_stmt.execute();
    count_after_result.next();
    var count_after = count_after_result.getColumnValue(1);

    return count_after - count_before;
$$;

call truecommerce.update_qb_mapping();