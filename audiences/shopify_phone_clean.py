import phonenumbers
import snowflake.connector
import requests
import subprocess
import json
import time

# Function to retrieve secrets from 1Password
def get_secret(item_name):
    secret_reference = f"op://{item_name}"  # Format the secret reference

    try:
        result = subprocess.run(
            ["op", "read", secret_reference],
            text=True,
            capture_output=True,
            check=True,
        )
        secret = result.stdout.strip()  # Strip leading/trailing white spaces
        return secret
    except subprocess.CalledProcessError as e:
        print(f"The command failed with error: {e.stderr}")

# Establish connection
con = snowflake.connector.connect(
    user=get_secret("Supply Factory/Snowflake/username"),
    password=get_secret("Supply Factory/Snowflake/password"),
    account=get_secret("Supply Factory/Snowflake/account id"),
    warehouse='compute_wh',
    database='prod_sf_tables',
    schema='audiences'
)

# Create a cursor object
cur = con.cursor()

# Execute a query to fetch the data
cur.execute("SELECT id, phone, country_code FROM prod_sf_tables.audiences.shopify")

# Fetch the result of the query
rows = cur.fetchall()

# Initialize counter for successful parses and dictionary for updated phone numbers
successful_parses = 0
updated_phone_numbers = {}

# Process each row
for row in rows:
    id, phone_number, country_code = row
    
    # Skip if phone number is None or blank
    if not phone_number:
        continue

    # Define the update query
    update_query = "UPDATE prod_sf_tables.audiences.shopify SET phone = %s WHERE id = %s"

    try:
        # If country code is None or blank, default to 'US'
        if not country_code or len(country_code) != 2:
            country_code = 'US'

        # Parse the phone number and country
        parsed_phone_number = phonenumbers.parse(phone_number, country_code)  # Use the country_code from the database

        # Format the phone number in E.164
        formatted_phone_number = phonenumbers.format_number(parsed_phone_number, phonenumbers.PhoneNumberFormat.E164)

        # Update the phone number in the database
        cur.execute(update_query, (formatted_phone_number, id))

        # Store the updated phone number
        updated_phone_numbers[id] = formatted_phone_number

        # Increment successful parses counter
        successful_parses += 1

        # Print progress every 100 successful parses
        if successful_parses % 100 == 0:
            print(f"{successful_parses} numbers successfully parsed.")

    except phonenumbers.phonenumberutil.NumberParseException:
        print(f"Could not parse phone number for id {id}")
        # Set the phone number to NULL in the database if it can't be parsed
        cur.execute(update_query, (None, id))

# Commit the transaction
con.commit()

# Shopify API update
base_url = "https://sunday-ii-sunday.myshopify.com/admin/api/2023-07/customers"
headers = {'Content-Type': 'application/json', 'X-Shopify-Access-Token': get_secret("Supply Factory/Shopify/access token")}

for id in updated_phone_numbers:
    phone_number = updated_phone_numbers[id]
    if phone_number:  # if phone number is not None or empty
        data = {"customer": {"id": id, "phone": phone_number}}
        try:
            response = requests.put(f"{base_url}/{id}.json", headers=headers, data=json.dumps(data))
            if response.status_code != 200:
                print(f"Failed to update phone number for customer {id}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to send request for customer {id}: {e}")
        # Respect the Shopify API rate limit
        time.sleep(0.05)  # Adjust this as needed

# Close the cursor and connection
cur.close()
con.close()