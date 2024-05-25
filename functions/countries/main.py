import logging
from utils import get_session, close_session, make_api_call
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from config_loader import load_config
import google.cloud.logging

# Sets up Google Cloud logging with the default client.
client = google.cloud.logging.Client()
client.setup_logging()

# Load configuration settings
config = load_config()


def get_existing_countries():
    """
    Retrieves existing countries from the database.

    Returns:
        list: A list of tuples containing country IDs, names, and alpha2 codes.
    """
    db_session = get_session()
    session = db_session()
    try:
        result = session.execute(text("SELECT id, name, alpha2 FROM countries")).fetchall()
        return result
    except SQLAlchemyError as e:
        logging.error(f"Failed to fetch existing countries: {e}")
        return []
    finally:
        close_session(db_session)


def insert_country(session, country_id, country_name, alpha2):
    """
    Inserts a new country into the database.

    Args:
        session: The database session.
        country_id (str): The ID of the country.
        country_name (str): The name of the country.
        alpha2 (str): The alpha2 code of the country.
    """
    try:
        session.execute(
            text("INSERT INTO countries (id, name, alpha2) VALUES (:id, :name, :alpha2)"),
            {'id': country_id, 'name': country_name, 'alpha2': alpha2}
        )
    except SQLAlchemyError as e:
        logging.error(f"Failed to insert country {country_name} ({country_id}): {e}")


def update_country(session, country_id, country_name, alpha2):
    """
    Updates an existing country's name and alpha2 code in the database.

    Args:
        session: The database session.
        country_id (str): The ID of the country.
        country_name (str): The new name of the country.
        alpha2 (str): The new alpha2 code of the country.
    """
    try:
        session.execute(
            text("UPDATE countries SET name = :name, alpha2 = :alpha2 WHERE id = :id"),
            {'name': country_name, 'alpha2': alpha2, 'id': country_id}
        )
    except SQLAlchemyError as e:
        logging.error(f"Failed to update country {country_id}: {e}")


def fetch_countries():
    """
    Fetches countries data from the API.

    Returns:
        dict or None: The JSON response from the API call or None if an error occurs.
    """
    endpoint = config['api']['endpoints']['countries']
    return make_api_call(endpoint)


def parse_countries_data(json_data):
    """
    Parses the API response to extract country data.

    Args:
        json_data (dict): The JSON data from the API response.

    Returns:
        list: A list of dictionaries containing country IDs, names, and alpha2 codes.
    """
    countries = []
    if json_data and "categories" in json_data:
        for category in json_data["categories"]:
            countries.append({
                "id": category["id"],
                "name": category["name"],
                "alpha2": category.get("alpha2", 'XX')
            })
    return countries


def countries_main(request):
    """
    Main function to handle country updates.

    Args:
        request (flask.Request): The request object, used for triggering the function.

    Returns:
        str, int: A response message and status code indicating the result of the operation.
    """
    inserted_count = 0
    updated_count = 0
    db_session = get_session()
    session = None

    try:
        # Get existing countries from the database
        session = db_session()
        existing_countries = get_existing_countries()
        existing_country_ids = {country[0] for country in existing_countries}
        existing_country_names = {country[1] for country in existing_countries}
        existing_country_alpha2 = {country[2] for country in existing_countries}

        # Fetch countries data from the API
        api_response = fetch_countries()
        if not api_response:
            logging.error("Failed to fetch countries from the API.")
            return "Failed to fetch countries from the API.", 500

        # Parse the API response
        new_countries = parse_countries_data(api_response)

        # Insert or update countries as needed
        for country in new_countries:
            if country['id'] not in existing_country_ids:
                insert_country(session, country['id'], country['name'], country['alpha2'])
                inserted_count += 1
            elif (country['name'] not in existing_country_names or
                  country['alpha2'] not in existing_country_alpha2):
                update_country(session, country['id'], country['name'], country['alpha2'])
                updated_count += 1
        session.commit()

        # Log the results
        logging.info(f"Inserted {inserted_count} new countries and updated {updated_count} existing countries.")

    except Exception as e:
        if session:
            session.rollback()
        logging.error(f"An error occurred during the country update process: {e}", exc_info=True)
        return f'An error occurred: {str(e)}', 500

    finally:
        if db_session:
            close_session(db_session)

    return 'Function executed successfully', 200
