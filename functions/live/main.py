#!/usr/bin/env python3
import time
import logging
import pytz
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from utils import get_session, close_session, send_alert, make_api_call
from config_loader import load_config
import google.cloud.logging

# Set up Google Cloud logging with the default client.
client = google.cloud.logging.Client()
client.setup_logging()

# Set logging level to debug
logging.getLogger().setLevel(logging.DEBUG)

# Load configuration settings
config = load_config()


def fetch_current_live_matches():
    """
    Fetches the current live matches from the API.

    Returns:
        list: A list of live match IDs.
    """
    response = make_api_call(config['api']['endpoints']['live_matches'])
    if not response:
        logging.error("Failed to fetch live matches.")
        return []
    try:
        live_matches_data = response.get('events', [])
        logging.info(f"Found {len(live_matches_data)} live matches.")
        return [match['id'] for match in live_matches_data]
    except KeyError as e:
        logging.error(f"Incorrect data format for live matches: {e}")
        return []


def fetch_live_match_incidents(match_id):
    """
    Fetches incidents for a specific live match from the API.

    Args:
        match_id (int): The ID of the match.

    Returns:
        list: A list of incidents for the match.
    """
    endpoint = config['api']['endpoints']['incidents'].format(match_id)
    response = make_api_call(endpoint)
    if not response:
        return []
    try:
        incidents = response.get('incidents', [])
        logging.debug(f"Incidents data retrieved for match ID {match_id}: {incidents}")
        return incidents
    except KeyError as e:
        logging.error(f"Incorrect data format for incidents of match ID {match_id}: {e}")
        return []


def fetch_teams_info(session, match_id):
    """
    Fetches team information for a specific match from the database.

    Args:
        session (Session): A database session object.
        match_id (int): The ID of the match.

    Returns:
        dict: A dictionary containing team information.
    """
    query = text("""
        SELECT minutes, country, tournament, home, away, home_score, away_score, home_pos, away_pos,
               score_ratio, conceded_ratio, h_squad_k, a_squad_k, squad_ratio, h_lineup_k, a_squad_k
        FROM v_matches_live WHERE match_id = :match_id
    """)
    result = session.execute(query, {'match_id': match_id}).fetchone()
    if result:
        teams_info = {
            'home': {
                'team': result[3],
                'score': result[5],
                'squad_value': result[11],
                'lineup_value': result[14],
                'standing_position': result[7],
            },
            'away': {
                'team': result[4],
                'score': result[6],
                'squad_value': result[12],
                'lineup_value': result[15],
                'standing_position': result[8],
            },
            'match_minutes': result[0],
            'tournament': result[2],
            'country': result[1],
            'squad_ratio': result[13],
            'score_ratio': result[9],
            'concede_ratio': result[10],
        }
        logging.debug(f"Teams info for match ID {match_id}: {teams_info}")
        return teams_info
    logging.error(f"No team info found for match ID {match_id}")
    return {}


def check_incident_in_db(session, incident_id):
    """
    Checks if an incident is already processed in the database.

    Args:
        session (Session): A database session object.
        incident_id (int): The ID of the incident.

    Returns:
        bool: True if the incident is already processed, False otherwise.
    """
    query = text("SELECT 1 FROM incidents WHERE id = :incident_id AND is_processed = 1")
    exists = session.execute(query, {'incident_id': incident_id}).fetchone() is not None
    logging.debug(f"Incident ID {incident_id} exists in DB: {exists}")
    return exists


def insert_incident(session, incident_id, now_formatted):
    """
    Inserts or updates an incident in the database.

    Args:
        session (Session): A database session object.
        incident_id (int): The ID of the incident.
        now_formatted (str): The current timestamp.
    """
    query = text("""
        INSERT INTO incidents (id, is_processed, processed_at)
        VALUES (:id, 1, :processed_at)
        ON DUPLICATE KEY UPDATE processed_at = :processed_at, is_processed = 1
    """)
    try:
        session.execute(query, {'id': incident_id, 'processed_at': now_formatted})
        session.commit()
        logging.debug(f"Inserted incident ID {incident_id} into DB.")
    except SQLAlchemyError as e:
        logging.error(f"Failed to insert/update incident ID {incident_id}. Error: {e}")
        session.rollback()


def rule_red_card(incident):
    """
    Checks if an incident is a red card.

    Args:
        incident (dict): The incident data.

    Returns:
        bool: True if the incident is a red card, False otherwise.
    """
    return incident['incidentType'] == 'card' and incident.get('incidentClass') in ['red', 'yellowRed'] and incident[
        'time'] < 80


def process_alerts(session, match_id, incidents):
    """
    Processes alerts for incidents in a match.

    Args:
        session (Session): A database session object.
        match_id (int): The ID of the match.
        incidents (list): A list of incidents for the match.
    """
    cest = pytz.timezone('Europe/Berlin')
    now_utc = datetime.now(pytz.utc)
    now_local = now_utc.astimezone(cest)
    now_formatted = now_local.strftime('%Y-%m-%d %H:%M:%S')

    teams_info = fetch_teams_info(session, match_id)
    if not teams_info:
        return

    for incident in incidents:
        logging.debug(f"Processing incident: {incident}")

        if rule_red_card(incident):
            incident_id = incident['id']
            if not check_incident_in_db(session, incident_id):
                message = construct_alert_message("Red Card", teams_info, incident)
                send_alert(message)
                insert_incident(session, incident_id, now_formatted)
                logging.info(f"Red card alert sent for match ID: {match_id}, incident ID: {incident_id}.")
            else:
                logging.debug(f"Incident ID {incident_id} already processed.")
        else:
            logging.debug(f"Incident does not match any rule: {incident}")


def construct_alert_message(incident_type, teams_info, incident):
    """
    Constructs an alert message for an incident.

    Args:
        incident_type (str): The type of incident.
        teams_info (dict): Information about the teams involved in the match.
        incident (dict): The incident data.

    Returns:
        str: The alert message.
    """
    team_received = "Home team" if incident.get('isHome', False) else "Away team"
    home_value = f"{teams_info['home']['lineup_value']}K" if teams_info['home']['lineup_value'] \
        else f"{teams_info['home']['squad_value']}K"
    away_value = f"{teams_info['away']['lineup_value']}K" if teams_info['away']['lineup_value'] \
        else f"{teams_info['away']['squad_value']}K"

    message = (
        f"Alert: {incident_type}\n"
        f"{teams_info['tournament']} ({teams_info['country']})\n"
        f"{teams_info['home']['team']} vs. {teams_info['away']['team']}\n"
        f"Current Score: {teams_info['home']['score']} - {teams_info['away']['score']}\n"
        f"Incident Time: {incident['time']} minutes\n"
        f"{team_received} received a red card.\n"
        f"Team info: Pos {teams_info['home']['standing_position']} vs Pos {teams_info['away']['standing_position']}\n"
        f"Goal Ratio: {teams_info['score_ratio']}/{teams_info['concede_ratio']}\n"
        f"Values: {home_value} vs {away_value} (Squad Ratio: {teams_info['squad_ratio']})\n"
    )
    return message


def live_main(request):
    """
    Main function to send live match alerts.

    Args:
        request (flask.Request): The request object.

    Returns:
        tuple: A response tuple containing a message and a status code.
    """
    start_time = time.time()
    logging.info("Live function execution started.")

    db_session = get_session()
    session = None

    try:
        session = db_session()
        live_match_ids = fetch_current_live_matches()

        for match_id in live_match_ids:
            incidents = fetch_live_match_incidents(match_id)
            if not incidents:
                continue
            process_alerts(session, match_id, incidents)

    except Exception as e:
        if session:
            session.rollback()
        logging.error(f"An error occurred: {e}")
        send_alert(f"Live match processing failed: {e}")
        return f'An error occurred: {str(e)}', 500

    finally:
        if db_session:
            close_session(db_session)

    logging.info(f"Total execution time: {time.time() - start_time:.4f} seconds")
    return 'Function executed successfully', 200
