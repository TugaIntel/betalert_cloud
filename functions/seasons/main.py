#!/usr/bin/env python3
import logging
from utils import get_session, close_session, make_api_call
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from config_loader import load_config
import google.cloud.logging

# Set up Google Cloud logging with the default client.
client = google.cloud.logging.Client()
client.setup_logging()

# Load configuration settings
config = load_config()


def get_tournaments(session):
    """
    Fetches all tournament IDs from the database.

    Args:
        session (Session): A database session object.

    Returns:
        list: A list of integer tournament IDs.
    """
    result = session.execute(text("SELECT id FROM tournaments"))
    return [row[0] for row in result.fetchall()]


def get_existing_seasons(session):
    """
    Fetches existing season data from the database.

    Args:
        session (Session): A database session object.

    Returns:
        dict: A dictionary mapping season IDs to a dictionary of season attributes.
    """
    result = session.execute(text("""
        SELECT id, name, year, tournament_id
        FROM seasons
    """))
    return {
        row[0]: {
            "name": row[1],
            "year": row[2],
            "tournament_id": row[3]
        } for row in result.fetchall()
    }


def insert_season(session, season_data):
    """
    Inserts a new season record into the database.

    Args:
        session (Session): A database session object.
        season_data (dict): The season data to insert.
    """
    insert_sql = text("""
        INSERT INTO seasons (id, name, year, tournament_id) 
        VALUES (:id, :name, :year, :tournament_id)
    """)
    try:
        session.execute(insert_sql, season_data)
        session.commit()
    except IntegrityError as e:
        if "1062" in str(e.orig):
            logging.warning(f"Skipped duplicate season with ID {season_data['id']}")
        else:
            logging.warning(f"IntegrityError while inserting season with ID {season_data['id']}: {e}")
            raise
    except SQLAlchemyError as e:
        logging.error(f"An error occurred while inserting season with ID {season_data['id']}: {e}")
        raise


def update_season(session, season_data):
    """
    Updates an existing season record in the database.

    Args:
        session (Session): A database session object.
        season_data (dict): The season data to update.
    """
    update_sql = text("""
        UPDATE seasons 
        SET name = :name, year = :year, tournament_id = :tournament_id
        WHERE id = :id
    """)
    try:
        session.execute(update_sql, season_data)
        session.commit()
    except SQLAlchemyError as e:
        logging.error(f"An error occurred while updating season with ID {season_data['id']}: {e}")
        raise


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
    """
    Main function to handle season data fetching and updates.

    Args:
        request (flask.Request): The request object.

    Returns:
        tuple: A response tuple containing a message and a status code.
    """
    logging.info("Seasons function execution started.")

    inserted_count = 0
    updated_count = 0
    db_session = get_session()
    session = None

    try:
        session = db_session()
        tournament_ids = get_tournaments(session)
        existing_seasons = get_existing_seasons(session)

        seasons_to_insert = []
        seasons_to_update = []

        for tournament_id in tournament_ids:
            logging.debug(f"Processing tournament ID: {tournament_id}")
            seasons_data = fetch_seasons_list(tournament_id)
            latest_season = parse_season_details(seasons_data, tournament_id)

            if latest_season:
                season_data = {
                    'id': latest_season['id'],
                    'name': latest_season['name'],
                    'year': latest_season['year'],
                    'tournament_id': latest_season['tournament_id']
                }

                if latest_season['id'] in existing_seasons:
                    existing_data = existing_seasons[latest_season['id']]
                    if (existing_data['name'] != latest_season['name'] or
                            existing_data['year'] != latest_season['year']):
                        seasons_to_update.append(season_data)
                        if len(seasons_to_update) >= 100:
                            for season in seasons_to_update:
                                update_season(session, season)
                            updated_count += len(seasons_to_update)
                            seasons_to_update.clear()
                else:
                    seasons_to_insert.append(season_data)
                    if len(seasons_to_insert) >= 100:
                        for season in seasons_to_insert:
                            insert_season(session, season)
                        inserted_count += len(seasons_to_insert)
                        seasons_to_insert.clear()

        # Insert any remaining seasons in the batch
        if seasons_to_insert:
            for season in seasons_to_insert:
                insert_season(session, season)
            inserted_count += len(seasons_to_insert)

        # Update any remaining seasons in the batch
        if seasons_to_update:
            for season in seasons_to_update:
                update_season(session, season)
            updated_count += len(seasons_to_update)

        logging.info(f"{inserted_count} new seasons inserted, {updated_count} seasons updated")

    except Exception as e:
        if session:
            session.rollback()
        logging.error(f"An error occurred during the country update process: {e}", exc_info=True)
        return f'An error occurred: {str(e)}', 500

    finally:
        if db_session:
            close_session(db_session)

    return 'Function executed successfully', 200