create schema audiences;

CREATE OR REPLACE TABLE shopify (
    id VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    city CHAR(100),
    zip VARCHAR(20),
    country CHAR(20),
    country_code VARCHAR(2),
    phone VARCHAR(50),
    PRIMARY KEY (id)
);

CREATE OR REPLACE TABLE shopify_all_customers (
    email VARCHAR(255) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    zip VARCHAR(20),
    country CHAR(2),
    phone VARCHAR(50),
    PRIMARY KEY (email)
);

CREATE OR REPLACE PROCEDURE s2s_shopify_all_customers()
  RETURNS STRING 
  LANGUAGE JAVASCRIPT
  EXECUTE AS 'YOUR_SNOWFLAKE_ROLE'
AS
$$
  var sql_command = 
    `INSERT INTO shopify_2023_purchases (email, First_Name, Last_Name, city, zip, country, country_code, phone)
     SELECT Email, FirstName, LastName, Country, Zip, Phone
     FROM prod_sf_tables.shopify.customers
     WHERE Email IS NOT NULL`;

  var statement = snowflake.createStatement({sqlText: sql_command});
  statement.execute();

  return 'Data migration completed successfully.';
$$;

CREATE OR REPLACE PROCEDURE s2s_shopify_audience()
  RETURNS STRING 
  LANGUAGE JAVASCRIPT
AS
$$
  var delete_data = 
    `DELETE FROM shopify_all_customers`;

  var statement = snowflake.createStatement({sqlText: delete_data});
  statement.execute();

  var insert_data = 
    `INSERT INTO shopify_all_customers (email, first_name, last_name, zip, country, phone)
     SELECT email, first_name, last_name, zip, country_code as country, phone
     FROM prod_sf_tables.audiences.shopify`;

  statement = snowflake.createStatement({sqlText: insert_data});
  statement.execute();

  return 'Shopify all customer audience data successfully exported.';
$$;


call s2s_shopify_audience();

SELECT 
    c.id, 
    c.Email, 
    c.First_Name, 
    c.Last_Name, 
    a.Country, 
    a.Zip, 
    a.Phone
FROM pc_fivetran_db.shopify.customer c
INNER JOIN pc_fivetran_db.shopify.customer_address a ON c.id = a.customer_id
WHERE c.Email IS NOT NULL

CREATE OR REPLACE PROCEDURE s2s_shopify_all_customers()
  RETURNS STRING 
  LANGUAGE JAVASCRIPT
AS
$$
  var delete_data = 
    `DELETE FROM shopify`;

  var statement = snowflake.createStatement({sqlText: delete_data});
  statement.execute();

  var insert_data = 
    `INSERT INTO shopify (id, email, first_name, last_name, city, zip, country, country_code, phone)
     SELECT c.id, c.email, c.first_name, c.last_name, a.city, a.zip, a.country, a.country_code, a.phone
     FROM pc_fivetran_db.shopify.customer c
     INNER JOIN pc_fivetran_db.shopify.customer_address a ON c.id = a.customer_id
     WHERE c.Email IS NOT NULL AND a.is_default = TRUE`;

  statement = snowflake.createStatement({sqlText: insert_data});
  statement.execute();

  return 'Shopify customer data successfully exported.';
$$;