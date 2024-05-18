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


def get_distinct_teams(session):
    """
    Fetches distinct team IDs from the matches table.

    Args:
        session (Session): A database session object.

    Returns:
        list of int: A list containing unique team IDs.
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


def get_teams_details(session):
    """
    Fetches all team details from the teams table.

    Args:
        session (Session): A database session object.

    Returns:
        dict: A dictionary mapping team IDs to their details.
    """
    result = session.execute(text("""
        SELECT id, name, short_name, user_count, stadium_capacity, primary_tournament_id
        FROM teams
    """))
    return {
        row[0]: {
            'name': row[1],
            'short_name': row[2],
            'user_count': row[3],
            'stadium_capacity': row[4],
            'primary_tournament_id': row[5],
        } for row in result.fetchall()
    }


def fetch_team_details(team_id):
    """
    Fetches team details from the API for a given team ID.

    Args:
        team_id (int): The unique ID of the team.

    Returns:
        dict: Team details from the API.
    """
    endpoint = config['api']['endpoints']['team'].format(team_id)
    return make_api_call(endpoint)


def parse_team_details(team_data):
    """
    Parses team data from the API response.

    Args:
        team_data (dict): Raw team data from the API.

    Returns:
        dict: Parsed team data suitable for database insertion or update.
    """
    if not team_data:
        return {}
    team = team_data.get('team', {})
    return {
        'id': team.get('id'),
        'name': team.get('name'),
        'short_name': team.get('shortName'),
        'user_count': team.get('userCount', 0),
        'stadium_capacity': team.get('venue', {}).get('stadium', {}).get('capacity', 0),
        'primary_tournament_id': team.get('primaryUniqueTournament', {}).get('id')
    }


def insert_team(session, team_data):
    """
    Inserts a new team record into the database.

    Args:
        session (Session): A database session object.
        team_data (dict): The parsed team data.
    """
    insert_sql = text("""
        INSERT INTO teams (id, name, short_name, user_count, stadium_capacity, primary_tournament_id)
        VALUES (:id, :name, :short_name, :user_count, :stadium_capacity, :primary_tournament_id)
    """)
    try:
        logging.debug(f"Attempting to insert team with data: {team_data}")
        session.execute(insert_sql, team_data)
        session.commit()
    except IntegrityError as e:
        if "1062" in str(e.orig):
            logging.warning(f"Skipped duplicate team with ID {team_data['id']}")
        else:
            logging.error(f"IntegrityError while inserting team with ID {team_data['id']}: {e}")
            raise
    except SQLAlchemyError as e:
        logging.error(f"An error occurred while inserting team with ID {team_data['id']}: {e}")
        raise


def update_team(session, team_data):
    """
    Updates an existing team record in the database.

    Args:
        session (Session): A database session object.
        team_data (dict): The parsed team data.
    """
    update_sql = text("""
        UPDATE teams
        SET name = :name, short_name = :short_name, user_count = :user_count, 
            stadium_capacity = :stadium_capacity, primary_tournament_id = :primary_tournament_id
        WHERE id = :id
    """)
    try:
        logging.debug(f"Updating team with data: {team_data}")
        session.execute(update_sql, team_data)
        session.commit()
    except SQLAlchemyError as e:
        logging.error(f"An error occurred while updating team with ID {team_data['id']}: {e}")
        raise


def update_squad_value(session):
    """
    Updates the squad value for all teams in the database.

    Args:
        session (Session): A database session object.
    """
    update_query = text("""
        UPDATE teams
        SET squad_value = (
            SELECT round(SUM(market_value)/1000000,3)
            FROM players
            WHERE players.team_id = teams.id
        )
    """)
    try:
        session.execute(update_query)
        session.commit()
    except SQLAlchemyError as e:
        logging.error(f"An error occurred while updating squad values: {e}")
        session.rollback()


def update_team_reputation(session):
    """
    Updates the reputation for all teams in the database.

    Args:
        session (Session): A database session object.
    """
    update_query = text("""
        UPDATE teams
        SET reputation = (
            user_count * 0.5 +
            stadium_capacity * 0.3 +
            (SELECT reputation FROM tournaments WHERE id = teams.primary_tournament_id) * 0.2
        )
    """)
    try:
        session.execute(update_query)
        session.commit()
    except SQLAlchemyError as e:
        logging.error(f"An error occurred while updating team reputations: {e}")
        session.rollback()


def teams_main(request):
    """
    Main function to handle team data fetching and updates.

    Args:
        request (flask.Request): The request object.

    Returns:
        tuple: A response tuple containing a message and a status code.
    """
    start_time = time.time()
    logging.info("Teams function execution started.")

    inserted_count = 0
    updated_count = 0
    integrity_error_count = 0
    db_session = get_session()
    session = None

    try:
        session = db_session()
        team_ids = get_distinct_teams(session)
        existing_teams = get_teams_details(session)

        for team_id in team_ids:
            team_data = fetch_team_details(team_id)
            parsed_data = parse_team_details(team_data)

            if team_id in existing_teams:
                existing_data = existing_teams[team_id]
                needs_update = any(
                    str(existing_data.get(key, None)) != str(value)
                    for key, value in parsed_data.items() if key != 'id'
                )
                if needs_update:
                    try:
                        update_team(session, parsed_data)
                        updated_count += 1
                    except IntegrityError:
                        integrity_error_count += 1
            else:
                try:
                    insert_team(session, parsed_data)
                    inserted_count += 1
                except IntegrityError:
                    integrity_error_count += 1

        update_squad_value(session)
        update_team_reputation(session)

        logging.info(f"Inserted {inserted_count} new teams, updated {updated_count} teams.")
        logging.info(f"Encountered {integrity_error_count} integrity errors.")

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
