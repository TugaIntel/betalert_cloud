#!/usr/bin/env python3
import time
import logging
import google.cloud.logging
from utils import get_db_connection, make_api_call  # Import utility functions
from config_loader import load_config  # Import configuration loader


# Sets up Google Cloud logging with the default client.
client = google.cloud.logging.Client()
client.setup_logging()

# Load configuration settings
config = load_config()


def get_matches(cursor):
    """
    Fetches matches scheduled to start within the next 6 hours that have not started yet,
    and belong to tournaments with a reputation of 'good' or 'top'.

    Args:
        cursor: A database cursor to execute the query.

    Returns:
        list of tuples: Each tuple contains match_id, home_team_id, and away_team_id for the matches.
    """
    query = """
    SELECT m.id, m.home_team_id, m.away_team_id
    FROM matches m
    JOIN tournaments t ON m.tournament_id = t.id
    WHERE 1=1
    AND m.match_status = 'notstarted'
    AND t.reputation_tier IN ('medium','good', 'top')
    AND m.match_time < now() + interval 2 hour
    """
    cursor.execute(query)
    return cursor.fetchall()


def get_player_values(cursor, player_ids):
    """
    Fetches market values for a list of player IDs.
    Args:
        cursor: Database cursor object.
        player_ids (list): List of player IDs.
    Returns:
        int: Sum of market values for the given player IDs.
    """
    format_strings = ','.join(['%s'] * len(player_ids))
    cursor.execute(f"""
        SELECT SUM(market_value) FROM players WHERE id IN ({format_strings})
    """, tuple(player_ids))
    result = cursor.fetchone()
    return result[0] if result else 0


def fetch_lineups(match_id):
    """
    Fetches lineups from the API for a specified match ID.

    Args:
        match_id (int): The unique identifier of the match.

    Returns:
        dict: Lineup details from the API if successful, None otherwise.
    """
    endpoint = config['api']['endpoints']['lineups'].format(match_id)
    response = make_api_call(endpoint)
    logging.debug(f"Lineups API response for match ID {match_id}: {response}")
    return response


def fetch_form(match_id):
    """
    Fetches pre-match form and average rating from the API for a specified match ID.

    Args:
        match_id (int): The unique identifier of the match.

    Returns:
        dict: Pre-match form details from the API if successful, None otherwise.
    """
    endpoint = config['api']['endpoints']['pregame_form'].format(match_id)
    response = make_api_call(endpoint)
    logging.debug(f"Form API response for match ID {match_id}: {response}")
    return response


def parse_lineups(lineups_data):
    """
    Extracts player IDs from the API response.
    Args:
        lineups_data (dict): Raw lineups data from the API.
    Returns:
        tuple: Tuple of lists containing player IDs for home and away teams.
    """
    if not lineups_data or 'home' not in lineups_data or 'away' not in lineups_data:
        return None, None

    home_player_ids = [player.get('player', {}).get('id') for player in lineups_data['home'].get('players', [])]
    away_player_ids = [player.get('player', {}).get('id') for player in lineups_data['away'].get('players', [])]

    return home_player_ids, away_player_ids


def parse_form_data(form_data):
    """
    Parses the form data received from the API to extract the form string and average rating for home and away teams.
    Ensures that rating values are either valid decimals or None.

    Args:
        form_data (dict): The raw form data from the API.

    Returns:
        tuple: Contains the form string and average rating for both home and away teams.
        Returns None if data is missing or invalid.
    """
    if not form_data:
        return None

    home_form = ''.join(form_data.get('homeTeam', {}).get('form', []))
    away_form = ''.join(form_data.get('awayTeam', {}).get('form', []))

    # Safely get the average rating or default to None if not present or empty
    home_rating = form_data.get('homeTeam', {}).get('avgRating')
    away_rating = form_data.get('awayTeam', {}).get('avgRating')

    # Ensure ratings are either decimal values or None
    home_rating = float(home_rating) if home_rating and home_rating.strip() else None
    away_rating = float(away_rating) if away_rating and away_rating.strip() else None

    logging.debug(
        f"Parsed form data - Home Form: {home_form}, Home Rating: {home_rating}, "
        f"Away Form: {away_form}, Away Rating: {away_rating}")
    return home_form, home_rating, home_form, away_rating


def update_match(cursor, conn, match_id, home_lineup, home_form, home_rating, away_lineup, away_form, away_rating):
    """
    Updates the database record for a match with the lineup values, form, and average ratings for both teams.

    Args:
        cursor: A database cursor to execute the update query.
        conn: Database connection object for committing the transaction.
        match_id (int): The unique identifier of the match.
        home_lineup (int): The total market value of the home team's lineup.
        home_form (str): The form string for the home team.
        home_rating (str): The average rating for the home team.
        away_lineup (int): The total market value of the away team's lineup.
        away_form (str): The form string for the away team.
        away_rating (str): The average rating for the away team.
    """
    update_sql = """
    UPDATE matches
    SET home_lineup = %s, home_form = %s, home_rating = %s, away_lineup = %s, away_form = %s, away_rating = %s
    WHERE id = %s
    """
    logging.debug(
        f"Updating match {match_id} with Home Lineup: {home_lineup}, Home Form: {home_form}, Home Rating: {home_rating}"
        f", Away Lineup: {away_lineup}, Away Form: {away_form}, Away Rating: {away_rating}")
    cursor.execute(update_sql, (home_lineup, home_form, home_rating, away_lineup, away_form, away_rating, match_id))
    conn.commit()


def lineups_main(request):
    start_time = time.time()

    logging.info("Fixtures function execution started.")

    engine = get_db_connection()  # This is an SQLAlchemy engine now
    conn = engine.raw_connection()  # Gets a raw connection from the engine
    cursor = conn.cursor()

    try:
        updated_count = 0
        matches = get_matches(cursor)

        for match_id, home_team_id, away_team_id in matches:
            lineups_data = fetch_lineups(match_id)
            form_data = fetch_form(match_id)

            if not lineups_data or not form_data:
                logging.debug(f"No data available for match ID {match_id}")
                continue

            home_player_ids, away_player_ids = parse_lineups(lineups_data)
            if home_player_ids and away_player_ids:
                home_lineup_value = get_player_values(cursor, home_player_ids)
                away_lineup_value = get_player_values(cursor, away_player_ids)
            else:
                logging.debug(f"No lineup data available for match ID {match_id}")
                continue

            form_values = parse_form_data(form_data)
            if not form_values:
                logging.debug(f"No form data available for match ID {match_id}")
                continue

            home_form, home_rating, away_form, away_rating = form_values

            # Update the match with lineup and form values
            update_match(cursor, conn, match_id, home_lineup_value, home_form, home_rating, away_lineup_value,
                         away_form, away_rating)
            updated_count += 1

        logging.info(f"Updated {updated_count} matches.")
        logging.info(f"Total execution time: {time.time() - start_time:.4f} seconds")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return f'An error occurred: {str(e)}', 500

    finally:
        cursor.close()  # Ensure the cursor is closed after operations
        conn.close()  # Ensure the connection is closed after operations

    logging.info(f"Total execution time: {time.time() - start_time:.4f} seconds")
    return 'Function executed successfully', 200
