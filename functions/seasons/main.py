#!/usr/bin/env python3
import time
import logging
import pymysql
import google.cloud.logging
from utils import get_db_connection, make_api_call  # Import utility functions
from config_loader import load_config  # Import configuration loader


# Sets up Google Cloud logging with the default client.
client = google.cloud.logging.Client()
client.setup_logging()

# Load configuration settings
config = load_config()


def get_tournaments(cursor):
    """
    Fetches all tournament IDs from the database.

    Returns:
        list: A list of integer tournament IDs.
    """

    cursor.execute("SELECT id FROM tournaments")
    tournament_ids = [row[0] for row in cursor.fetchall()]

    return tournament_ids


def get_existing_seasons(cursor):
    """
    Fetches existing season data from the database.

    This function retrieves all seasons currently recorded in the database,
    including their associated tournament ID.

    Args:
        cursor: The database cursor object.

    Returns:
        A dictionary mapping season IDs to a dictionary of season attributes.
    """
    cursor.execute("""
        SELECT id, name, year, tournament_id
        FROM seasons
    """)
    seasons = {
        row[0]: {
            "name": row[1],
            "year": row[2],
            "tournament_id": row[3]
        } for row in cursor.fetchall()
    }
    return seasons


def insert_season(cursor, conn, season_data):
    """
    Inserts a new season record into the database.

    Args:
        cursor: The database cursor object.
        conn: The database connection object.
        season_data (tuple): The season data to insert, expected to contain:
                             (id, name, year, tournament_id)
    """
    insert_sql = """
        INSERT INTO seasons (id, name, year, tournament_id) 
        VALUES (%s, %s, %s, %s)
    """
    try:
        cursor.execute(insert_sql, season_data)
        conn.commit()
    except pymysql.err.IntegrityError as e:
        if e.args[0] == 1062:  # Check if error code is for a duplicate entry
            logging.warning(f"Skipped duplicate season with ID {season_data[-1]}")
        else:
            raise  # Re-raise the exception if it's not a duplicate entry error
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise  # Continue to propagate other types of exceptions


def update_season(cursor, conn, season_data):
    """
    Updates an existing season record in the database.

    Args:
        cursor (cursor): The database cursor object.
        conn (connection): The database connection object.
        season_data (tuple): The season data to update, expected to contain:
                             (name, year, tournament_id, id) where id is the last element for identification.
    """
    update_sql = """
        UPDATE seasons 
        SET name = %s, year = %s, tournament_id = %s
        WHERE id = %s
    """
    cursor.execute(update_sql, season_data)
    conn.commit()


def fetch_seasons_list(tournament_id):
    """
    Fetches season information for a specific tournament from an external API.

    Args:
        tournament_id (int): The ID of the tournament to fetch seasons for.

    Returns:
        A list of dictionaries containing season information.
    """
    endpoint = config['api']["endpoints"]["seasons"].format(tournament_id)
    season_data = make_api_call(endpoint)
    return season_data if season_data else []


def parse_season_details(season_data, tournament_id):
    """
    Parses the JSON response data to extract details of the latest season.
    Assumes the API returns seasons in order and the first one is the latest.
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

    engine = get_db_connection()  # This is an SQLAlchemy engine now
    conn = engine.raw_connection()  # Gets a raw connection from the engine
    cursor = conn.cursor()

    try:
        # Initialize counters for inserted and updated tournaments
        inserted_count = 0
        updated_count = 0

        # Fetch the list of tournament IDs
        tournament_ids = get_tournaments(cursor)

        for tournament_id in tournament_ids:
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
                        update_season(cursor, conn, (latest_season['name'], latest_season['year'],
                                                     latest_season['tournament_id'], latest_season['id']))
                        updated_count += 1
                else:
                    # New season; insert record
                    insert_season(cursor, conn, season_data)
                    inserted_count += 1

        # Log the counters
        logging.info(
            f"{inserted_count} new seasons inserted, {updated_count} seasons updated")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return f'An error occurred: {str(e)}', 500

    finally:
        cursor.close()  # Ensure the cursor is closed after operations
        conn.close()  # Ensure the connection is closed after operations

    logging.info(f"Total execution time: {time.time() - start_time:.4f} seconds")
    return 'Function executed successfully', 200
