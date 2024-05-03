#!/usr/bin/env python3
import time
import logging
import google.cloud.logging
from utils import get_db_connection, make_api_call  # Import utility functions
from config_loader import load_config  # Import configuration loader

# Sets up Google Cloud logging with the default client.
client = google.cloud.logging.Client()
client.setup_logging()

# Load configuration settings
config = load_config()


def get_existing_countries(cursor):
    """
    Fetches existing countries from the database.

    Args:
        cursor (Cursor): A database cursor to execute database operations.

    Returns:
        dict: A dictionary of countries with country IDs as keys and country names as values.
    """
    cursor.execute("SELECT id, name FROM countries")
    return {row[0]: {"name": row[1]} for row in cursor.fetchall()}


def insert_country(cursor, conn, country_id, country_name):
    """
    Inserts a new country into the database.

    Args:
        cursor (Cursor): A database cursor to execute database operations.
        conn (Connection): A database connection object to commit changes.
        country_id (int): The ID of the country to insert.
        country_name (str): The name of the country to insert.
    """
    insert_country_sql = "INSERT INTO countries (id, name) VALUES (%s, %s)"
    cursor.execute(insert_country_sql, (country_id, country_name))
    conn.commit()


def update_country(cursor, conn, country_id, country_name):
    """
    Updates an existing country's name in the database.

    Args:
        cursor (Cursor): A database cursor to execute database operations.
        conn (Connection): A database connection object to commit changes.
        country_id (int): The ID of the country to update.
        country_name (str): The new name for the country.
    """
    update_country_sql = "UPDATE countries SET name = %s WHERE id = %s"
    cursor.execute(update_country_sql, (country_name, country_id))
    conn.commit()


def fetch_countries():
    """
    Fetches country data from the API.

    Returns:
        list of dict: Parsed JSON response data from the API, or None if the API call fails.
    """
    endpoint = config['api']['endpoints']['countries']  # Access endpoint from config
    logging.debug(f"Fetching countries from API endpoint: {endpoint}")
    response_data = make_api_call(endpoint)

    if response_data:
        return response_data
    else:
        return None


def parse_countries_data(json_data):
    """
    Parses JSON data containing countries information.

    Args:
        json_data (dict): The JSON response data from the API call.

    Returns:
        list: A list of dictionaries, each containing a country's ID and name.
    """
    countries = []
    if json_data and "categories" in json_data:
        for category in json_data["categories"]:
            countries.append({"id": category["id"], "name": category["name"]})
    logging.debug(f"Parsed {len(countries)} countries from API data.")
    return countries


def countries_main(request):
    """
    Main function to handle country data fetching and updating.

    Args:
        request: The request payload for HTTP Cloud Function.

    Returns:
        tuple: Response message and HTTP status code.
    """
    start_time = time.time()

    logging.info("Countries function execution started.")

    engine = get_db_connection()  # This is an SQLAlchemy engine now
    conn = engine.raw_connection()  # Gets a raw connection from the engine
    cursor = conn.cursor()

    try:
        # Initialize counters for inserted and updated countries
        inserted_count = 0
        updated_count = 0

        # Fetch existing country data from the database (optimized as dictionary)
        existing_countries = get_existing_countries(cursor)

        # Fetch country data from API Call
        api_countries = fetch_countries()

        # Parse JSON country data
        country_data = parse_countries_data(api_countries)

        for country in country_data:
            country_id = country['id']

            # Check if country is new or existing
            if country_id not in existing_countries:
                # Insert new country data into the database
                insert_country(cursor, conn, country_id, country['name'])
                inserted_count += 1
            else:
                # Check if specific data has changed before updating
                if existing_countries[country_id]['name'] != country['name']:
                    update_country(cursor, conn, country_id, country['name'])
                    updated_count += 1

        logging.info(f"Inserted {inserted_count} new countries and updated {updated_count} existing countries.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return f'An error occurred: {str(e)}', 500

    finally:
        cursor.close()  # Ensure the cursor is closed after operations
        conn.close()  # Ensure the connection is closed after operations

    logging.info(f"Total execution time: {time.time() - start_time:.4f} seconds")
    return 'Function executed successfully', 200
