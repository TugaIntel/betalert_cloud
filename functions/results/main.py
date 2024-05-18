#!/usr/bin/env python3
import time
import json
import logging
import urllib.request
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from utils import get_session, close_session
from config_loader import load_config
import google.cloud.logging
from urllib import error

# Set up Google Cloud logging with the default client.
client = google.cloud.logging.Client()
client.setup_logging()

# Load configuration settings
config = load_config()


def get_matches(session):
    """
    Fetches matches that are in progress or not started and scheduled to start within the next 5 minutes.

    Args:
        session (Session): A database session object.

    Returns:
        dict: A dictionary of match details keyed by match ID.
    """
    query = text("""
        SELECT id, home_score, away_score, match_status
        FROM matches  
        WHERE match_status IN ('inprogress', 'notstarted') 
        AND match_time <= NOW() + INTERVAL '5 minute'
    """)
    result = session.execute(query)
    return {row[0]: {"home_score": row[1], "away_score": row[2], "match_status": row[3]} for row in result.fetchall()}


def fetch_match_results(match_id):
    """
    Fetches match results from the API for a given match ID.

    Args:
        match_id (int): The ID of the match.

    Returns:
        dict: Match results from the API.
    """
    endpoint = config['api']['base_url'] + config['api']['endpoints']['matches'].format(match_id)
    try:
        with urllib.request.urlopen(endpoint) as response:
            data = json.loads(response.read())
            return data.get('event', {})
    except urllib.error.URLError as e:
        logging.error(f"Failed to fetch results for match ID {match_id}: {e}")
        return {}


def update_match_data(session, match_id, home_score, away_score, match_status):
    """
    Updates the match data in the database.

    Args:
        session (Session): A database session object.
        match_id (int): The ID of the match.
        home_score (int): The home team's score.
        away_score (int): The away team's score.
        match_status (str): The status of the match.
    """
    update_sql = text("""
        UPDATE matches 
        SET home_score = :home_score, away_score = :away_score, match_status = :match_status
        WHERE id = :match_id
    """)
    try:
        session.execute(update_sql, {
            'home_score': home_score,
            'away_score': away_score,
            'match_status': match_status,
            'match_id': match_id
        })
        session.commit()
    except SQLAlchemyError as e:
        logging.error(f"An error occurred while updating match with ID {match_id}: {e}")
        session.rollback()


def results_main(request):
    """
    Main function to handle match results data fetching and updates.

    Args:
        request (flask.Request): The request object.

    Returns:
        tuple: A response tuple containing a message and a status code.
    """
    start_time = time.time()
    logging.info("Results function execution started.")

    updated_count = 0
    db_session = get_session()
    session = None

    try:
        session = db_session()
        matches_to_update = get_matches(session)

        for match_id, match_info in matches_to_update.items():
            results_data = fetch_match_results(match_id)
            if results_data:
                new_home_score = results_data['homeScore'].get('current', None)
                new_away_score = results_data['awayScore'].get('current', None)
                new_status = results_data['status']['type']

                if (new_home_score != match_info['home_score'] or new_away_score != match_info['away_score']
                        or new_status != match_info['match_status']):
                    update_match_data(session, match_id, new_home_score, new_away_score, new_status)
                    updated_count += 1

        logging.info(f"Results update process completed. {updated_count} matches updated.")
        logging.info(f"Total execution time: {time.time() - start_time:.4f} seconds")

    except Exception as e:
        if session:
            session.rollback()
        logging.error(f"An error occurred: {e}", exc_info=True)
        return f'An error occurred: {str(e)}', 500

    finally:
        if db_session:
            close_session(db_session)

    return 'Function executed successfully', 200
