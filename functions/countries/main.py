#!/usr/bin/env python3
import time
from utils import get_db_connection, make_api_call  # Import utility functions
from log import setup_logger  # Import logging functionality
from config_loader import load_config  # Import configuration loader

# Set up logger for the countries entity
logger = setup_logger('countries', 'INFO')
config = load_config()


def get_existing_countries(cursor):
    cursor.execute("SELECT id, name FROM countries")
    return {row[0]: {"name": row[1]} for row in cursor.fetchall()}


def insert_country(cursor, conn, country_id, country_name):

    insert_country_sql = "INSERT INTO countries (id, name) VALUES (%s, %s)"
    cursor.execute(insert_country_sql, (country_id, country_name))
    conn.commit()


def update_country(cursor, conn, country_id, country_name):

    update_country_sql = "UPDATE countries SET name = %s WHERE id = %s"
    cursor.execute(update_country_sql, (country_name, country_id))
    conn.commit()


def fetch_countries():

    endpoint = config['api']['endpoints']['countries']  # Access endpoint from config
    response_data = make_api_call(endpoint)

    if response_data:
        return response_data
    else:
        return None


def parse_countries_data(json_data):

    countries = []
    if json_data and "categories" in json_data:
        for category in json_data["categories"]:
            countries.append({"id": category["id"], "name": category["name"]})
    return countries


def countries_main(request):

    start_time = time.time()
    engine = get_db_connection()  # This is an SQLAlchemy engine now

    try:
        conn = engine.raw_connection()  # Gets a raw connection from the engine
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

    except Exception as e:
        logger.error(f"An error occurred: {e}")

    finally:
        cursor.close()  # Ensure the cursor is closed after operations
        conn.close()  # Ensure the connection is closed after operations

    logger.info(f"Total execution time: {time.time() - start_time:.4f} seconds")


