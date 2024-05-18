#!/usr/bin/env python3
import time
import logging
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from utils import get_session, close_session, make_api_call
from config_loader import load_config
import google.cloud.logging

# Set up Google Cloud logging with the default client.
client = google.cloud.logging.Client()
client.setup_logging()

# Load configuration settings
config = load_config()


def get_recent_matches(session):
    """
    Fetches distinct tournaments and seasons that have matches finished in the last 12 hours.

    Args:
        session (Session): A database session object.

    Returns:
        list of tuples: A list containing tuples of (tournament_id, season_id).
    """
    query = text("""
        SELECT DISTINCT tournament_id, season_id
        FROM matches
        WHERE match_status = 'finished' AND match_time >= NOW() - INTERVAL 5 hour
    """)
    result = session.execute(query)
    return result.fetchall()


def fetch_standings(tournament_id, season_id):
    """
    Fetches standings information for a specific tournament and season from an external API.

    Args:
        tournament_id (int): The ID of the tournament to fetch standings for.
        season_id (int): The ID of the season to fetch standings for.

    Returns:
        list: A list of dictionaries containing standings information.
    """
    endpoint = config['api']['endpoints']['standings'].format(tournament_id, season_id)
    standing_data = make_api_call(endpoint)
    return standing_data if standing_data else []


def parse_standings_data(standings_data, tournament_id, season_id):
    """
    Parses the JSON response data to extract standings information.

    Args:
        standings_data (list): The JSON response data from the API call.
        tournament_id (int): The ID of the tournament for which standings are being parsed.
        season_id (int): The ID of the season for which standings are being parsed.

    Returns:
        list of dict: A list containing dictionaries of parsed standings data.
    """
    parsed_standings = []
    groups = standings_data if isinstance(standings_data, list) else standings_data.get("standings", [])

    for group in groups:
        group_name = group.get("name", "Overall")
        for row in group.get("rows", []):
            team = row.get("team", {})
            parsed_standings.append({
                "tournament_id": tournament_id,
                "season_id": season_id,
                "group_name": group_name,
                "team_id": team.get("id"),
                "position": row.get("position"),
                "played": row.get("matches"),
                "wins": row.get("wins"),
                "losses": row.get("losses"),
                "draws": row.get("draws"),
                "scored": row.get("scoresFor"),
                "conceded": row.get("scoresAgainst"),
                "points": row.get("points")
            })
    return parsed_standings if parsed_standings else []


def insert_standings(session, standing):
    """
    Inserts a new standings record into the database.

    Args:
        session (Session): A database session object.
        standing (dict): The parsed standings data.
    """
    insert_sql = text("""
        INSERT INTO standings (tournament_id, season_id, team_id, group_name, position, played, wins, 
                               losses, draws, scored, conceded, points)
        VALUES (:tournament_id, :season_id, :team_id, :group_name, :position, :played, :wins, 
                :losses, :draws, :scored, :conceded, :points)
    """)
    try:
        session.execute(insert_sql, standing)
        session.commit()
    except SQLAlchemyError as e:
        logging.error(f"Error inserting standings for tournament_id: {standing['tournament_id']}, "
                      f"season_id: {standing['season_id']}, team_id: {standing['team_id']}. Error: {e}")
        session.rollback()


def delete_standings(session, tournament_id, season_id):
    """
    Deletes existing standings for the given tournament and season.

    Args:
        session (Session): A database session object.
        tournament_id (int): The ID of the tournament.
        season_id (int): The ID of the season.
    """
    delete_sql = text("""
        DELETE FROM standings 
        WHERE tournament_id = :tournament_id AND season_id = :season_id
    """)
    try:
        session.execute(delete_sql, {'tournament_id': tournament_id, 'season_id': season_id})
        session.commit()
    except SQLAlchemyError as e:
        logging.error(f"Error deleting standings for tournament_id: {tournament_id}, "
                      f"season_id: {season_id}. Error: {e}")
        session.rollback()


def standings_main(request):
    """
    Main function to handle the standings update process.
    Fetches and updates standings for tournaments and seasons based on recent match results.

    Args:
        request (flask.Request): The request object.

    Returns:
        tuple: A response tuple containing a message and a status code.
    """
    start_time = time.time()
    logging.info("Standings function execution started.")

    db_session = get_session()
    session = None

    try:
        session = db_session()
        recent_matches = get_recent_matches(session)

        for tournament_id, season_id in recent_matches:
            standings_data = fetch_standings(tournament_id, season_id)
            parsed_standings = parse_standings_data(standings_data, tournament_id, season_id)

            delete_standings(session, tournament_id, season_id)

            for standing in parsed_standings:
                insert_standings(session, standing)

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
