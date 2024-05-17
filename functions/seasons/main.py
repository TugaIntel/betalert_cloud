#!/usr/bin/env python3
import time
import logging
import pymysql
import google.cloud.logging
from utils import get_db_connection, make_api_call
from config_loader import load_config

# Set up Google Cloud logging with the default client.
client = google.cloud.logging.Client()
client.setup_logging()

# Load configuration settings
config = load_config()


def get_tournaments(cursor):
    """
    Fetches all tournament IDs from the database.

    Args:
        cursor (cursor): A database cursor object.

    Returns:
        list: A list of integer tournament IDs.
    """
    cursor.execute("SELECT id FROM tournaments")
    return [row[0] for row in cursor.fetchall()]


def get_existing_seasons(cursor):
    """
    Fetches existing season data from the database.

    Args:
        cursor (cursor): A database cursor object.

    Returns:
        dict: A dictionary mapping season IDs to a dictionary of season attributes.
    """
    cursor.execute("""
        SELECT id, name, year, tournament_id
        FROM seasons
    """)
    return {
        row[0]: {
            "name": row[1],
            "year": row[2],
            "tournament_id": row[3]
        } for row in cursor.fetchall()
    }


def insert_season(cursor, season_data):
    """
    Inserts a new season record into the database.

    Args:
        cursor (cursor): A database cursor object.
        season_data (tuple): The season data to insert.
    """
    insert_sql = """
        INSERT INTO seasons (id, name, year, tournament_id) 
        VALUES (%s, %s, %s, %s)
    """
    try:
        cursor.execute(insert_sql, season_data)
    except pymysql.err.IntegrityError as e:
        if e.args[0] == 1062:  # Duplicate entry error
            logging.warning(f"Skipped duplicate season with ID {season_data[0]}")
        else:
            raise
    except Exception as e:
        logging.error(f"An error occurred while inserting season: {e}")
        raise


def update_season(cursor, season_data):
    """
    Updates an existing season record in the database.

    Args:
        cursor (cursor): A database cursor object.
        season_data (tuple): The season data to update.
    """
    update_sql = """
        UPDATE seasons 
        SET name = %s, year = %s, tournament_id = %s
        WHERE id = %s
    """
    cursor.execute(update_sql, season_data)


def fetch_seasons_list(tournament_id):
    """
    Fetches season information for a specific tournament from an external API.

    Args:
        tournament_id (int): The ID of the tournament to fetch seasons for.

    Returns:
        dict: A dictionary containing season information.
    """
    endpoint = config['api']["endpoints"]["seasons"].format(tournament_id)
    return make_api_call(endpoint) or {'seasons': []}


def parse_season_details(season_data, tournament_id):
    """
    Parses the JSON response data to extract details of the latest season.

    Args:
        season_data (dict): The JSON response data from the API call.
        tournament_id (int): The ID of the tournament for the season.

    Returns:
        dict: A dictionary containing details of the latest season if found, otherwise None.
    """
    seasons = season_data.get('seasons', [])
    if seasons:
        latest_season = seasons[0]
        return {
            'id': latest_season['id'],
            'name': latest_season['name'],
            'year': latest_season.get('year', ''),
            'tournament_id': tournament_id
        }
    return None


def seasons_main(request):
    """Main function to handle season data fetching and updates."""
    start_time = time.time()
    logging.info("Seasons function execution started.")

    engine = get_db_connection()

    try:
        with engine.connect() as conn:
            with conn.begin():
                cursor = conn.connection.cursor()
                # Initialize counters for inserted and updated seasons
                inserted_count = 0
                updated_count = 0

                # Fetch the list of tournament IDs
                tournament_ids = get_tournaments(cursor)

                for tournament_id in tournament_ids:
                    logging.debug(f"Processing tournament ID: {tournament_id}")
                    # Fetch season list from API for each tournament
                    seasons_data = fetch_seasons_list(tournament_id)

                    # Parse the latest season details from the API response
                    latest_season = parse_season_details(seasons_data, tournament_id)

                    if latest_season:
                        # Fetch existing seasons from the database
                        existing_seasons = get_existing_seasons(cursor)

                        # Prepare data for both insert and update operations
                        season_data = (
                            latest_season['id'],
                            latest_season['name'],
                            latest_season['year'],
                            latest_season['tournament_id']
                        )

                        if latest_season['id'] in existing_seasons:
                            # Existing season; update if data has changed
                            existing_data = existing_seasons[latest_season['id']]
                            if (existing_data['name'] != latest_season['name'] or
                                    existing_data['year'] != latest_season['year']):
                                update_season(cursor, (latest_season['name'], latest_season['year'],
                                                       latest_season['tournament_id'], latest_season['id']))
                                updated_count += 1
                        else:
                            # New season; insert record
                            insert_season(cursor, season_data)
                            inserted_count += 1

                logging.info(f"{inserted_count} new seasons inserted, {updated_count} seasons updated")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return f'An error occurred: {str(e)}', 500

    logging.info(f"Total execution time: {time.time() - start_time:.4f} seconds")
    return 'Function executed successfully', 200
