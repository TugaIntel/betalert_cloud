#!/usr/bin/env python3
import time
import logging
import urllib.request
import json
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from utils import get_session, close_session  # Import utility functions
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
        SELECT id, name, short_name, user_count, stadium_capacity, primary_tournament_id, is_national
        FROM teams
    """))
    return {
        row[0]: {
            'name': row[1],
            'short_name': row[2],
            'user_count': row[3],
            'stadium_capacity': row[4],
            'primary_tournament_id': row[5],
            'is_national': row[6],
        } for row in result.fetchall()
    }


def get_countries(session):
    """
    Fetches all country IDs and their alpha2 codes from the countries table.

    Args:
        session (Session): A database session object.

    Returns:
        dict: A dictionary mapping country IDs to their alpha2 codes.
    """
    result = session.execute(text("SELECT id, alpha2 FROM countries")).fetchall()
    return {row[0]: row[1] for row in result}


def fetch_team_details(team_id):
    """
    Fetches team details from the API for a given team id.

    Args:
        team_id (int): The ID of the team.

    Returns:
        list: team details from the API.
    """
    url = config['api']['base_url'] + config['api']['endpoints']['team'].format(team_id)
    try:
        with urllib.request.urlopen(url) as response:
            data = response.read()
            json_data = json.loads(data)
            return json_data
    except urllib.error.URLError as e:
        logging.error(f"Failed to fetch team from API: {e}")
        return []


def parse_team_details(team_data, countries):
    if not team_data:
        return {}

    team = team_data.get('team', {})
    category = team.get('category', {})
    primary_unique_tournament_category = team.get('primaryUniqueTournament', {}).get('category', {})

    # Determine is_national
    is_national = team.get('national')
    team_debug = team.get('name')
    logging.debug(f"Team: {team_debug} National: {is_national}")
    if is_national is None:
        # Determine country_id
        country_id = primary_unique_tournament_category.get('id')
        if country_id is None:
            country_id = category.get('id')

        # Determine is_national based on country_id
        is_national = determine_is_national(country_id, countries)
    else:
        # Determine country_id based on national flag
        country_id = primary_unique_tournament_category.get('id') or category.get('id')

    parsed_data = {
        'id': team.get('id'),
        'name': team.get('name'),
        'short_name': team.get('shortName'),
        'user_count': team.get('userCount', 0),
        'stadium_capacity': team.get('venue', {}).get('stadium', {}).get('capacity', 0),
        'primary_tournament_id': team.get('primaryUniqueTournament', {}).get('id'),
        'country_id': country_id,
        'is_national': is_national
    }

    return parsed_data


def determine_is_national(country_id, countries):
    """
    Determines if a team is national based on the country_id and the fetched countries data.

    Args:
        country_id (int): The ID of the country.
        countries (dict): A dictionary mapping country IDs to their alpha2 codes.

    Returns:
        int: 1 if the team is national, 0 otherwise.
    """
    if country_id is None:
        return 0

    alpha2 = countries.get(country_id)
    return 1 if alpha2 == 'XX' else 0


def insert_teams_batch(session, teams_data):
    insert_sql = text("""
        INSERT INTO teams (id, name, short_name, user_count, stadium_capacity, primary_tournament_id, is_national)
        VALUES (:id, :name, :short_name, :user_count, :stadium_capacity, :primary_tournament_id, :is_national)
    """)
    try:
        session.execute(insert_sql, teams_data)
        session.commit()
    except IntegrityError as e:
        logging.error(f"IntegrityError while inserting teams batch: {e}")
        session.rollback()
    except SQLAlchemyError as e:
        logging.error(f"An error occurred while inserting teams batch: {e}")
        session.rollback()


def update_teams_batch(session, teams_data):
    update_sql = text("""
        UPDATE teams
        SET name = :name, short_name = :short_name, user_count = :user_count, 
            stadium_capacity = :stadium_capacity, primary_tournament_id = :primary_tournament_id, 
            is_national = :is_national
        WHERE id = :id
    """)
    try:
        session.execute(update_sql, teams_data)
        session.commit()
    except SQLAlchemyError as e:
        logging.error(f"An error occurred while updating teams batch: {e}")
        session.rollback()


def update_squad_value(session):
    """
    Updates the squad value for all teams in the database.

    Args:
        session (Session): A database session object.
    """
    update_query = text("""
    UPDATE teams
    SET squad_value = (SELECT ROUND(SUM(market_value) / COUNT(*), 2)
                        FROM players
                        WHERE players.team_id = teams.id
                        AND players.market_value > 0 )
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
    SET reputation = ( user_count * 0.5 + stadium_capacity * 0.3 +
    COALESCE((SELECT reputation FROM tournaments WHERE id = teams.primary_tournament_id), 0) * 0.2)
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
        countries = get_countries(session)

        teams_to_insert = []
        teams_to_update = []

        for team_id in team_ids:
            team_data = fetch_team_details(team_id)
            parsed_data = parse_team_details(team_data, countries)

            if team_id in existing_teams:
                existing_data = existing_teams[team_id]
                needs_update = any(
                    str(existing_data.get(key, None)) != str(value)
                    for key, value in parsed_data.items()
                )
                if needs_update:
                    teams_to_update.append(parsed_data)
                    if len(teams_to_update) >= 100:
                        update_teams_batch(session, teams_to_update)
                        updated_count += len(teams_to_update)
                        teams_to_update.clear()
            else:
                teams_to_insert.append(parsed_data)
                if len(teams_to_insert) >= 100:
                    insert_teams_batch(session, teams_to_insert)
                    inserted_count += len(teams_to_insert)
                    teams_to_insert.clear()

        # Insert any remaining teams in the batch
        if teams_to_insert:
            insert_teams_batch(session, teams_to_insert)
            inserted_count += len(teams_to_insert)

        # Update any remaining teams in the batch
        if teams_to_update:
            update_teams_batch(session, teams_to_update)
            updated_count += len(teams_to_update)

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
