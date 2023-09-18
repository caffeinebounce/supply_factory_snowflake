create or replace procedure public.create_latest_view(base_table string)
returns string
language javascript
as
$$
    var sql_command = `
        create or replace view ${BASE_TABLE}_latest as
        select * from ${BASE_TABLE} where latest = true
    `;

    try {
        snowflake.execute({sqlText: sql_command});
        return 'View created successfully for ' + BASE_TABLE;
    } catch (err) {
        return 'Failed to create view for ' + BASE_TABLE + ': ' + err.message;
    }
$$;