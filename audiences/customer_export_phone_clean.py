import phonenumbers
import snowflake.connector

# Establish connection
con = snowflake.connector.connect(
    user='thebigdoog',
    password='dgv7PED5pxh@ejv4ukm',
    account='ym77059.east-us-2.azure',
    warehouse='compute_wh',
    database='prod_sf_tables',
    schema='audiences'
)

# Create a cursor object
cur = con.cursor()

# Execute a query to fetch the data
cur.execute("SELECT email, Phone FROM prod_sf_tables.audiences.shopify")

# Fetch the result of the query
rows = cur.fetchall()

# Process each row
for row in rows:
    email, phone_number = row
    
    # Skip if phone number is None or blank
    if not phone_number:
        continue
    
    try:
        # Parse the phone number and country
        parsed_phone_number = phonenumbers.parse(phone_number, 'US')  # Change 'US' to the correct country if needed

        # Format the phone number in E.164
        formatted_phone_number = phonenumbers.format_number(parsed_phone_number, phonenumbers.PhoneNumberFormat.E164)

        # Update the phone number in the database
        update_query = "UPDATE prod_sf_tables.audiences.shopify SET Phone = %s WHERE email = %s"
        cur.execute(update_query, (formatted_phone_number, email))

    except phonenumbers.phonenumberutil.NumberParseException:
        print(f"Could not parse phone number for email {email}")

# Set blank phone numbers to NULL
cur.execute("UPDATE prod_sf_tables.audiences.shopify SET Phone = NULL WHERE Phone = ''")

# Commit the transaction
con.commit()

# Close the cursor and connection
cur.close()
con.close()