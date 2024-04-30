#!/usr/bin/env python3
import time
from utils import get_db_connection, make_api_call  # Import utility functions
from log import setup_logger  # Import logging functionality
from config_loader import load_config  # Import configuration loader

# Set up logger for the countries entity
logger = setup_logger('countries', 'INFO')
config = load_config()


def get_existing_countries(cursor):
    """
    Fetches existing country data from the database and returns them as a dictionary.

    Args:
        cursor (cursor): A database cursor object.

    Returns:
        dict: A dictionary where keys are country IDs and values are dictionaries containing country data (name).
    """
    cursor.execute("SELECT id, name FROM countries")
    return {row[0]: {"name": row[1]} for row in cursor.fetchall()}


def insert_country(cursor, conn, country_id, country_name):
    """
    Inserts a new country record into the 'countries' table.

    Args:
        cursor (cursor): A database cursor object.
        conn (connection): A database connection object.
        country_id (int): The ID of the country to insert.
        country_name (str): The name of the country to insert.
    """
    insert_country_sql = "INSERT INTO countries (id, name) VALUES (%s, %s)"
    cursor.execute(insert_country_sql, (country_id, country_name))
    conn.commit()


def update_country(cursor, conn, country_id, country_name):
    """
    Updates the name of an existing country record in the 'countries' table.

    Args:
        cursor (cursor): A database cursor object.
        conn (connection): A database connection object.
        country_id (int): The ID of the country to update.
        country_name (str): The new name for the country.
    """
    update_country_sql = "UPDATE countries SET name = %s WHERE id = %s"
    cursor.execute(update_country_sql, (country_name, country_id))
    conn.commit()


def fetch_countries():
    """
    Fetches countries data from the API and returns it.

    Returns:
        dict: The parsed API response data for countries, or None if API call fails.
    """

    endpoint = config['api']['endpoints']['countries']  # Access endpoint from config
    response_data = make_api_call(endpoint)

    if response_data:
        return response_data
    else:
        return None


def parse_countries_data(json_data):
    """
    Parses the JSON response data containing country information.

    Args:
        json_data (dict): The JSON response data from the API call.

    Returns:
        list: A list of dictionaries containing parsed country data (id and name).
    """

    countries = []
    if json_data and "categories" in json_data:
        for category in json_data["categories"]:
            countries.append({"id": category["id"], "name": category["name"]})
    return countries


def main_countries():
    start_time = time.time()
    """Main function to handle country data fetching and updates."""

    conn = get_db_connection()
    cursor = conn.cursor()

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

    # Log the number of inserted and updated countries
    logger.info(f"Inserted {inserted_count} and updated {updated_count} countries in the database.")

    # Close the database connection
    cursor.close()
    conn.close()

    logger.info(f"Total execution time: {time.time() - start_time:.4f} seconds")


if __name__ == "__main__":
    main_countries()
