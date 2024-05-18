#!/usr/bin/env python3
import time
import logging
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from utils import get_session, close_session, make_api_call  # Import utility functions
from config_loader import load_config  # Import configuration loader
import google.cloud.logging

# Set up Google Cloud logging with the default client.
client = google.cloud.logging.Client()
client.setup_logging()

# Load configuration settings
config = load_config()


def get_teams(session):
    """
    Fetches all team IDs from the teams that have games to play in the next 24 hours.

    Args:
        session (Session): A database session object.

    Returns:
        list of int: A list containing team IDs.
    """
    query = text("""
        SELECT DISTINCT home_team_id FROM matches 
        WHERE match_time BETWEEN NOW() AND NOW() + INTERVAL 245 minute
        UNION 
        SELECT DISTINCT away_team_id FROM matches 
        WHERE match_time BETWEEN NOW() AND NOW() + INTERVAL 245 minute
    """)
    result = session.execute(query)
    return [row[0] for row in result.fetchall()]


def get_players(session):
    """
    Fetches all existing players from the database.

    Args:
        session (Session): A database session object.

    Returns:
        dict: Existing players keyed by a tuple of (player ID, team ID).
    """
    result = session.execute(text("""
        SELECT id, name, short_name, position, market_value, team_id FROM players
    """))
    return {(row[0], row[5]): {  # Key by a tuple of (player_id, team_id)
        'name': row[1],
        'short_name': row[2],
        'position': row[3],
        'market_value': row[4],
        'team_id': row[5]
    } for row in result.fetchall()}


def fetch_team_players(team_id):
    """
    Fetches player details for a given team from the API.

    Args:
        team_id (int): The ID of the team.

    Returns:
        list of dicts: Player details from the API.
    """
    endpoint = config['api']['endpoints']['players'].format(team_id)
    response = make_api_call(endpoint)
    return response['players'] if response else []


def parse_player_data(player_container, team_id):
    """
    Parses player data from the API response.

    Args:
        player_container (dict): Container with 'player' key holding player data from the API.
        team_id (int): The ID of the team to which the player belongs.

    Returns:
        dict: Parsed player data suitable for database insertion or update.
    """
    player_data = player_container.get('player')
    if not player_data:
        return None

    player_id = player_data.get('id')
    if player_id is None or team_id is None:
        return None

    return {
        'id': player_id,
        'name': player_data.get('name'),
        'short_name': player_data.get('shortName'),
        'position': player_data.get('position'),
        'market_value': player_data.get('proposedMarketValue'),
        'team_id': team_id,
    }


def insert_player(session, player_data):
    """
    Inserts a new player into the database.

    Args:
        session (Session): A database session object.
        player_data (dict): The parsed player data.
    """
    insert_sql = text("""
        INSERT INTO players (name, short_name, position, market_value, team_id, id)
        VALUES (:name, :short_name, :position, :market_value, :team_id, :id)
    """)
    try:
        logging.debug(f"Attempting to insert player with data: {player_data}")
        session.execute(insert_sql, player_data)
        session.commit()
    except IntegrityError as e:
        if "1062" in str(e.orig):
            logging.warning(f"Skipped duplicate player with ID {player_data['id']}")
        else:
            logging.error(f"IntegrityError while inserting player with ID {player_data['id']}: {e}")
            raise
    except SQLAlchemyError as e:
        logging.error(f"An error occurred while inserting player with ID {player_data['id']}: {e}")
        raise


def update_player(session, player_data):
    """
    Updates an existing player in the database.

    Args:
        session (Session): A database session object.
        player_data (dict): The parsed player data.
    """
    update_sql = text("""
        UPDATE players
        SET name = :name, short_name = :short_name, position = :position, market_value = :market_value
        WHERE id = :id AND team_id = :team_id
    """)
    try:
        logging.debug(f"Updating player with data: {player_data}")
        session.execute(update_sql, player_data)
        session.commit()
    except SQLAlchemyError as e:
        logging.error(f"An error occurred while updating player with ID {player_data['id']}: {e}")
        raise


def players_main(request):
    """
    Main function to handle player data fetching and updates.

    Args:
        request (flask.Request): The request object.

    Returns:
        tuple: A response tuple containing a message and a status code.
    """
    start_time = time.time()
    logging.info("Players function execution started.")

    inserted_count = 0
    updated_count = 0
    db_session = get_session()
    session = None

    try:
        session = db_session()
        team_ids = get_teams(session)
        existing_players = get_players(session)

        for team_id in team_ids:
            players_container = fetch_team_players(team_id)
            for player_container in players_container:
                parsed_data = parse_player_data(player_container, team_id)

                if not parsed_data:
                    continue

                composite_key = (parsed_data['id'], parsed_data['team_id'])

                if composite_key in existing_players:
                    existing_data = existing_players[composite_key]
                    needs_update = any(
                        str(existing_data.get(key, '')) != str(value)
                        for key, value in parsed_data.items() if key != 'id'
                    )
                    if needs_update:
                        try:
                            update_player(session, parsed_data)
                            updated_count += 1
                        except IntegrityError:
                            logging.error(f"Error updating player {parsed_data['id']}")
                else:
                    try:
                        insert_player(session, parsed_data)
                        inserted_count += 1
                    except IntegrityError:
                        logging.error(f"Error inserting player {parsed_data['id']}")

        logging.info(f"Inserted {inserted_count} new players, updated {updated_count} players.")

    except Exception as e:
        if session:
            session.rollback()
        logging.error(f"An error occurred: {e}", exc_info=True)
        return f'An error occurred: {str(e)}', 500

    finally:
        if db_session:
            close_session(db_session)

    logging.info(f"Total execution time: {time.time() - start_time:.4f} seconds")
    return 'Function executed successfully', 200
