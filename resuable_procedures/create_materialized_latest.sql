create or replace procedure public.create_materialized_view(base_table string)
returns string
language sql
as
$$
    declare
        sql_command string;
    begin
        -- Construct the SQL command dynamically
        set sql_command = concat(
            'create or replace materialized view ',
            base_table,
            '_mview as select * from ',
            base_table,
            ' where latest = true;'
        );

        -- Execute the SQL command
        execute immediate sql_command;
        return 'Materialized view created successfully for ' || base_table_name;
    exception
        when others then
            return 'Failed to create materialized view for ' || base_table_name || ': ' || error_message();
    end;
$$;
