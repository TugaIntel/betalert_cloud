#!/usr/bin/env python3
import time
import logging
import pytz
from datetime import datetime, timedelta
from sqlalchemy import text
from utils import get_session, close_session, send_alert
from config_loader import load_config
import google.cloud.logging

# Set up Google Cloud logging with the default client.
client = google.cloud.logging.Client()
client.setup_logging()

# Load configuration settings
config = load_config()


def fetch_pre_match_info(session):
    """
    Fetches upcoming matches within a time window, considering time zone.

    Args:
        session (Session): A database session object.

    Returns:
        list: A list containing match details.
    """
    cest = pytz.timezone('Europe/Berlin')
    offset_start = timedelta(minutes=25)
    offset_end = timedelta(minutes=100)

    now_utc = datetime.now(pytz.utc)
    now_local = now_utc.astimezone(cest)
    start_time = (now_local + offset_start).strftime('%Y-%m-%d %H:%M:%S')
    end_time = (now_local + offset_end).strftime('%Y-%m-%d %H:%M:%S')

    query = text(f"""
        SELECT label, DATE_FORMAT(match_time, '%H:%i') AS match_time, country, tournament, 
               home, away, h_squad_k, a_squad_k, squad_ratio, score_ratio, conceded_ratio, 
               h_lineup_k, a_lineup_k, home_pos, away_pos, round_number
        FROM v_pre_match_analysis
        WHERE match_time BETWEEN '{start_time}' AND '{end_time}'
          AND label IS NOT NULL
          AND reputation_tier in ('top', 'good', 'medium')
        ORDER BY match_time, tournament_reputation DESC
    """)
    result = session.execute(query)
    return result.fetchall()


def construct_alert_message(matches):
    """
    Constructs a list of messages with each message adhering to the character limit imposed by Telegram.

    Args:
        matches (list): A list of match details.

    Returns:
        list: A list of alert messages.
    """
    max_message_length = 4000
    messages = []
    current_message = "Upcoming Matches:\n\n"

    for row in matches:
        (label, match_time, country, tournament, home, away, h_squad_k, a_squad_k, squad_ratio, score_ratio,
         conceded_ratio, h_lineup_k, a_lineup_k, home_pos, away_pos, round_number) = row

        home_value = f"{h_lineup_k}K" if h_lineup_k and h_lineup_k != 0 else f"{h_squad_k}K"
        away_value = f"{a_lineup_k}K" if a_lineup_k and a_lineup_k != 0 else f"{a_squad_k}K"
        home_score_char = score_ratio[0] if score_ratio else ''
        away_score_char = score_ratio[1] if score_ratio else ''
        home_concede_char = conceded_ratio[0] if conceded_ratio else ''
        away_concede_char = conceded_ratio[1] if conceded_ratio else ''

        addition = (f"{label} in {country} {tournament} - {match_time}\n"
                    f"Round {round_number}: {home}({home_pos}) vs {away}({away_pos})\n"
                    f"Goal Ratio: {home_score_char}/{home_concede_char} vs {away_score_char}/{away_concede_char}\n"
                    f"Values: {home_value} vs {away_value} (Ratio: {squad_ratio})\n\n")

        if len(current_message) + len(addition) > max_message_length:
            messages.append(current_message)
            current_message = "Upcoming Matches:\n\n"

        current_message += addition

    if current_message:
        messages.append(current_message)

    return messages


def prematch_main(request):
    """
    Main function to send pre-match alerts.

    Args:
        request (flask.Request): The request object.

    Returns:
        tuple: A response tuple containing a message and a status code.
    """
    start_time = time.time()
    logging.info("PreMatch function execution started.")

    db_session = get_session()
    session = None

    try:
        session = db_session()
        logging.info("Fetching pre-match information...")

        pre_match_info = fetch_pre_match_info(session)
        if pre_match_info:
            messages = construct_alert_message(pre_match_info)
            for message in messages:
                send_alert(message)
                logging.info("Pre-match alert sent.")
        else:
            logging.info("No matches to alert.")

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
