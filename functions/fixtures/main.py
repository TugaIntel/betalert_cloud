#!/usr/bin/env python3
import time
import json
import logging
import pytz
import urllib.request
from datetime import datetime, timedelta
from urllib import error
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


def fetch_seasons_db(session):
    """
    Fetches all distinct season IDs, tournament IDs, and additional details from the database.

    Args:
        session (Session): A database session object.

    Returns:
        dict: A dictionary mapping season IDs to their tournament IDs and additional details.
    """
    result = session.execute(text("""
        SELECT DISTINCT s.id, s.tournament_id, t.reputation_tier, t.tier, t.country_id, t.name
        FROM tournaments t
        JOIN seasons s ON s.tournament_id = t.id
    """))
    return {row[0]: {
        "tournament_id": row[1],
        "reputation_tier": row[2],
        "tier": row[3],
        "country_id": row[4],
        "tournament_name": row[5]
    } for row in result.fetchall()}


def fetch_fixtures_db(session):
    """
    Fetches existing fixtures that are not finished from the database.

    Args:
        session (Session): A database session object.

    Returns:
        dict: A dictionary mapping match IDs to their match time and status.
    """
    result = session.execute(text("""
        SELECT id, match_time, match_status 
        FROM matches 
        WHERE match_status != 'finished'
    """))
    existing = {row[0]: {"match_time": row[1], "match_status": row[2]} for row in result.fetchall()}
    logging.debug(f"Fetched existing fixtures: {existing}")
    return existing


# Initialize counters
success_count = 0
not_found_count = 0


def fetch_fixtures_api(tournament_id, season_id):
    """
    Fetches fixtures from the API for a given tournament and season.

    Args:
        tournament_id (int): The ID of the tournament.
        season_id (int): The ID of the season.

    Returns:
        list: A list of fixtures fetched from the API.
    """
    global success_count, not_found_count

    url = config['api']['base_url'] + config['api']['endpoints']['next'].format(tournament_id, season_id)
    try:
        with urllib.request.urlopen(url) as response:
            if response.status == 200:
                success_count += 1
            data = response.read()
            json_data = json.loads(data)
            return json_data.get('events', [])
    except urllib.error.HTTPError as e:
        if e.code == 404:
            not_found_count += 1
        if not_found_count > success_count:
            logging.debug(f"Number of 404 responses ({not_found_count}) "
                          f"exceeded number of 200 responses ({success_count}).")
        return []
    except urllib.error.URLError as e:
        logging.debug(f"Failed to fetch fixtures from API: {e}")
        return []


def insert_match(session, match_data):
    """
    Inserts a new match record into the database.

    Args:
        session (Session): A database session object.
        match_data (dict): The match data to insert.
    """
    insert_match_sql = text("""
        INSERT INTO matches (id, home_team_id, away_team_id, tournament_id, round_number, 
        match_time, home_score, away_score, match_status, season_id, match_type) 
        VALUES (:id, :home_team_id, :away_team_id, :tournament_id, :round_number, 
                :match_time, :home_score, :away_score, :match_status, :season_id, :match_type)
    """)
    try:
        logging.debug(f"Attempting to insert match with data: {match_data}")
        session.execute(insert_match_sql, match_data)
        session.commit()
    except IntegrityError as e:
        if "1062" in str(e.orig):
            logging.warning(f"Skipped duplicate match with ID {match_data['id']}")
        else:
            logging.warning(f"IntegrityError while inserting match with ID {match_data['id']}: {e}")
            raise
    except SQLAlchemyError as e:
        logging.error(f"An error occurred while inserting match with ID {match_data['id']}: {e}")
        raise


def update_match(session, match_data):
    """
    Updates an existing match record in the database.

    Args:
        session (Session): A database session object.
        match_data (dict): The match data to update.
    """
    update_match_sql = text("""
        UPDATE matches
        SET match_time = :match_time, home_score = :home_score, away_score = :away_score, match_status = :match_status,
        match_type = :match_type
        WHERE id = :id
    """)
    try:
        logging.debug(f"Updating match with data: {match_data}")
        session.execute(update_match_sql, match_data)
        session.commit()
    except SQLAlchemyError as e:
        logging.error(f"An error occurred while updating match with ID {match_data['id']}: {e}")
        raise


def delete_matches(session):
    """
    Deletes obsolete matches from the database.

    Args:
        session (Session): A database session object.
    """
    delete_sql = text("""
        DELETE FROM matches
        WHERE match_status IN ('canceled', 'postponed') 
        OR (match_time < (NOW() - INTERVAL 3 DAY) AND match_status != 'finished')
    """)
    try:
        result = session.execute(delete_sql)
        deleted_count = result.rowcount
        session.commit()
        logging.info(f"Deleted {deleted_count} matches with 'canceled' or 'postponed' status.")
    except SQLAlchemyError as e:
        logging.error(f"Failed to delete matches, error: {e}")
        session.rollback()


def determine_match_type(tournament_details):
    tier = tournament_details['tier']
    reputation_tier = tournament_details['reputation_tier']
    country_id = tournament_details['country_id']
    tournament_name = tournament_details['tournament_name']

    if tier <= 2 and reputation_tier in ('top', 'good'):
        return reputation_tier.lower()
    elif tier <= 3 and reputation_tier == 'medium' and country_id not in (21, 1151, 1158, 542, 1132, 1088, 500, 390):
        return reputation_tier.lower()
    elif 'Friendly' in tournament_name and reputation_tier not in ('bottom', 'low'):
        return 'friendly'
    elif 1460 <= country_id <= 1470 and reputation_tier != 'bottom':
        return 'international'
    elif tier in (21, 22) and country_id in (280, 165, 281, 26, 34, 305) and reputation_tier != 'bottom':
        return reputation_tier.lower()
    elif tier in (21, 22) and not (1460 <= country_id <= 1470) and reputation_tier != 'bottom':
        return 'cup'
    else:
        return 'other'


def fixtures_main(request):
    """
    Main function to handle fixtures data fetching and updates.

    Args:
        request (flask.Request): The request object.

    Returns:
        tuple: A response tuple containing a message and a status code.
    """
    start_time = time.time()
    logging.info("Fixtures function execution started.")

    inserted_count = 0
    updated_count = 0
    db_session = get_session()
    session = None

    try:
        session = db_session()
        season_tournaments = fetch_seasons_db(session)

        fixtures_to_insert = []
        fixtures_to_update = []

        for season_id, details in season_tournaments.items():
            tournament_id = details['tournament_id']
            existing_fixtures = fetch_fixtures_db(session)
            fixtures_from_api = fetch_fixtures_api(tournament_id, season_id)

            cest = pytz.timezone('Europe/Berlin')
            today = datetime.now(pytz.utc).astimezone(cest)
            delta = today + timedelta(days=15)

            for fixture_data in fixtures_from_api:
                timestamp_data = fixture_data['startTimestamp']
                utc_time_data = datetime.fromtimestamp(timestamp_data, tz=pytz.utc)
                cest_time_data = utc_time_data.astimezone(cest)

                if cest_time_data < delta:
                    formatted_time_data = cest_time_data.strftime('%Y-%m-%d %H:%M:%S')
                    match_type = determine_match_type(details)
                    match_data = {
                        'id': fixture_data['id'],
                        'home_team_id': fixture_data['homeTeam']['id'],
                        'away_team_id': fixture_data['awayTeam']['id'],
                        'tournament_id': fixture_data['tournament']['uniqueTournament']['id'],
                        'round_number': fixture_data.get('roundInfo', {}).get('round', 0),
                        'match_time': formatted_time_data,
                        'home_score': fixture_data.get('homeScore', {}).get('aggregated', None),
                        'away_score': fixture_data.get('awayScore', {}).get('aggregated', None),
                        'match_status': fixture_data['status']['type'],
                        'season_id': fixture_data['season']['id'],
                        'match_type': match_type
                    }

                    logging.debug(f"Prepared match data: {match_data}")
                    if match_data['id'] not in existing_fixtures:
                        fixtures_to_insert.append(match_data)
                        if len(fixtures_to_insert) >= 100:
                            for fixture in fixtures_to_insert:
                                insert_match(session, fixture)
                            inserted_count += len(fixtures_to_insert)
                            fixtures_to_insert.clear()
                    else:
                        db_match_data = existing_fixtures[fixture_data['id']]
                        if (db_match_data['match_time'] != formatted_time_data or
                                db_match_data['match_status'] != fixture_data['status']['type']):
                            update_match_data = {
                                'match_time': formatted_time_data,
                                'home_score': match_data['home_score'],
                                'away_score': match_data['away_score'],
                                'match_status': match_data['match_status'],
                                'id': match_data['id'],
                                'match_type': match_type
                            }
                            fixtures_to_update.append(update_match_data)
                            if len(fixtures_to_update) >= 100:
                                for fixture in fixtures_to_update:
                                    update_match(session, fixture)
                                updated_count += len(fixtures_to_update)
                                fixtures_to_update.clear()

        # Insert any remaining fixtures in the batch
        if fixtures_to_insert:
            for fixture in fixtures_to_insert:
                insert_match(session, fixture)
            inserted_count += len(fixtures_to_insert)

        # Update any remaining fixtures in the batch
        if fixtures_to_update:
            for fixture in fixtures_to_update:
                update_match(session, fixture)
            updated_count += len(fixtures_to_update)

        delete_matches(session)
        logging.info(f"Inserted {inserted_count} new fixtures, updated {updated_count} fixtures.")
        logging.info(f"Total execution time: {time.time() - start_time:.4f} seconds")

    except Exception as e:
        if session:
            session.rollback()
        logging.error(f"An error occurred during the fixtures update process: {e}", exc_info=True)
        return f'An error occurred: {str(e)}', 500

    finally:
        if db_session:
            close_session(db_session)

    return 'Function executed successfully', 200
