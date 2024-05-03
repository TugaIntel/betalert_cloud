#!/usr/bin/env python3
import time
import json
import logging
import urllib.request
import google.cloud.logging
from datetime import datetime, timezone, timedelta
from utils import get_db_connection  # Import utility functions
from config_loader import load_config  # Import configuration loader
from urllib import error

# Sets up Google Cloud logging with the default client.
client = google.cloud.logging.Client()
client.setup_logging()

# Load configuration settings
config = load_config()


def fetch_seasons_db(cursor):
    cursor.execute("SELECT id, tournament_id FROM seasons")
    return {row[0]: {"tournament_id": row[1]} for row in cursor.fetchall()}


def fetch_fixtures_db(cursor):
    cursor.execute("SELECT id, match_time, match_status FROM matches WHERE match_status!='finished'")
    existing = {row[0]: {"match_time": row[1], "match_status": row[2]} for row in cursor.fetchall()}
    logging.debug(f"Fetched existing fixtures: {existing}")
    return existing


def fetch_fixtures_api(tournament_id, season_id):
    # Construct the endpoint URL using the formatted date
    url = config['api']['base_url'] + config['api']['endpoints']['next'].format(tournament_id, season_id)

    # Perform the API call using urllib.request
    try:
        with urllib.request.urlopen(url) as response:
            data = response.read()
            json_data = json.loads(data)
            return json_data.get('events', [])
    except urllib.error.URLError:
        # Handle cases where the API call fails (e.g., log the error)
        return []


def insert_match(cursor, conn, match_data):
    insert_match_sql = """
    INSERT INTO matches (id, home_team_id, away_team_id, tournament_id, round_number, 
    match_time, home_score, away_score, match_status, season_id) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
        logging.debug(f"Attempting to insert match with data: {match_data}")
        cursor.execute(insert_match_sql, match_data)
        conn.commit()
    except Exception as e:
        # This will capture any SQL errors, such as trying to insert a duplicate key without an update fallback
        logging.error(f"Failed to insert match: {match_data[0]}, error: {e}")
        conn.rollback()


def update_match(cursor, conn, match_data):
    update_match_sql = """
    UPDATE matches
    SET match_time = %s, home_score = %s, away_score = %s, match_status = %s
    WHERE id = %s
    """
    try:
        logging.debug(f"Updating match with data: {match_data}")
        cursor.execute(update_match_sql, match_data)
        conn.commit()
    except Exception as e:
        logging.error(f"Failed to update match: {match_data[4]}, error: {e}")
        conn.rollback()


def delete_matches(cursor, conn):
    """
    Deletes obsolete matches from the database.

    Args:
        cursor: Database cursor object.
        conn: Database connection object.
    """
    delete_sql = """
        DELETE FROM matches
        WHERE match_status IN ('canceled', 'postponed') 
        or (match_time < now() - interval 1 day and match_status != 'finished') 
    """
    try:
        cursor.execute(delete_sql)
        deleted_count = cursor.rowcount
        conn.commit()
        logging.info(f"Deleted {deleted_count} matches with 'canceled' or 'postponed' status.")
    except Exception as e:
        logging.error(f"Failed to delete matches, error: {e}")
        conn.rollback()


def fixtures_main(request):
    start_time = time.time()

    logging.info("Players function execution started.")

    engine = get_db_connection()  # This is an SQLAlchemy engine now
    conn = engine.raw_connection()  # Gets a raw connection from the engine
    cursor = conn.cursor()

    try:
        inserted_count = 0
        updated_count = 0

        # Fetch all active season details which include season_id and tournament_id
        season_tournaments = fetch_seasons_db(cursor)

        # Loop through each season to fetch and process match data
        for season_id, details in season_tournaments.items():
            tournament_id = details['tournament_id']

            # Fetch existing matches from the database for the current tournament and season
            existing_fixtures = fetch_fixtures_db(cursor)

            # Fetch new fixtures data from the API for the current date + 2 days
            fixtures_from_api = fetch_fixtures_api(tournament_id, season_id)

            cest = timezone(timedelta(hours=2))

            for fixtures_data in fixtures_from_api:

                timestamp_data = fixtures_data['startTimestamp']
                utc_time_data = datetime.fromtimestamp(timestamp_data, tz=timezone.utc)
                cest_time_data = utc_time_data.astimezone(cest)
                formatted_time_data = cest_time_data.strftime('%Y-%m-%d %H:%M:%S')
                match_data = (
                    fixtures_data['id'],
                    fixtures_data['homeTeam']['id'],
                    fixtures_data['awayTeam']['id'],
                    fixtures_data['tournament']['uniqueTournament']['id'],  # Updated to reflect new JSON structure
                    fixtures_data.get('roundInfo', {}).get('round', 0),
                    formatted_time_data,
                    fixtures_data.get('homeScore', {}).get('aggregated', None),  # Adjusted for new score field
                    fixtures_data.get('awayScore', {}).get('aggregated', None),  # Adjusted for new score field
                    fixtures_data['status']['type'],
                    fixtures_data['season']['id']
                )

                logging.debug(f"Prepared match data: {match_data}")
                if match_data[0] not in existing_fixtures:
                    insert_match(cursor, conn, match_data)
                    inserted_count += 1
                else:
                    db_match_data = existing_fixtures[fixtures_data['id']]
                    if (db_match_data['match_time'] != formatted_time_data or
                            db_match_data['match_status'] != fixtures_data['status']['type']):
                        update_match_data = (formatted_time_data, match_data[6], match_data[7],
                                             match_data[8], match_data[0])
                        update_match(cursor, conn, update_match_data)
                        updated_count += 1

        delete_matches(cursor, conn)

        logging.info(f"Inserted {inserted_count} new fixtures, updated {updated_count} fixtures.")
        logging.info(f"Total execution time: {time.time() - start_time:.4f} seconds")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return f'An error occurred: {str(e)}', 500

    finally:
        cursor.close()  # Ensure the cursor is closed after operations
        conn.close()  # Ensure the connection is closed after operations

    logging.info(f"Total execution time: {time.time() - start_time:.4f} seconds")
    return 'Function executed successfully', 200
