#!/usr/bin/env python3
import time
import json
import logging
import google.cloud.logging
import urllib.request
from utils import get_db_connection
from config_loader import load_config
from urllib import error

# Sets up Google Cloud logging with the default client.
client = google.cloud.logging.Client()
client.setup_logging()

# Load configuration settings
config = load_config()


def get_matches(cursor):
    cursor.execute("""
        SELECT id, home_score, away_score, match_status
        FROM matches  
        WHERE match_status IN ('inprogress', 'notstarted') 
        AND match_time <= NOW() + INTERVAL 5 MINUTE
    """)
    return {row[0]: {"home_score": row[1], "away_score": row[2], "match_status": row[3]} for row in cursor.fetchall()}


def fetch_match_results(match_id):
    endpoint = config['api']['base_url'] + config['api']['endpoints']['matches'].format(match_id)
    try:
        with urllib.request.urlopen(endpoint) as response:
            data = json.loads(response.read())
            return data.get('event', {})
    except urllib.error.URLError as e:
        logging.error(f"Failed to fetch results for match ID {match_id}: {e}")
        return {}


def update_match_data(cursor, conn, match_id, home_score, away_score, match_status):
    update_sql = """
        UPDATE matches 
        SET home_score = %s, away_score = %s, match_status = %s
        WHERE id = %s
    """
    cursor.execute(update_sql, (home_score, away_score, match_status, match_id))
    conn.commit()


def results_main(request):
    start_time = time.time()

    logging.info("Players function execution started.")

    engine = get_db_connection()  # This is an SQLAlchemy engine now
    conn = engine.raw_connection()  # Gets a raw connection from the engine
    cursor = conn.cursor()

    try:
        updated_count = 0

        matches_to_update = get_matches(cursor)
        for match_id, match_info in matches_to_update.items():
            results_data = fetch_match_results(match_id)
            if results_data:
                new_home_score = results_data['homeScore'].get('current', None)
                new_away_score = results_data['awayScore'].get('current', None)
                new_status = results_data['status']['type']

                if (new_home_score != match_info['home_score'] or new_away_score != match_info['away_score']
                        or new_status != match_info['match_status']):
                    update_match_data(cursor, conn, match_id, new_home_score, new_away_score, new_status)
                    updated_count += 1

        logging.info(f"Results update process completed. {updated_count} matches updated.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return f'An error occurred: {str(e)}', 500

    finally:
        cursor.close()  # Ensure the cursor is closed after operations
        conn.close()  # Ensure the connection is closed after operations

    logging.info(f"Total execution time: {time.time() - start_time:.4f} seconds")
    return 'Function executed successfully', 200
