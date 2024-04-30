#!/usr/bin/env python3
from datetime import datetime
from utils import get_db_connection, make_api_call
from log import setup_logger
from config_loader import load_config

# Setup logger
logger = setup_logger('standings', 'INFO')

# Load configurations
config = load_config()


def get_recent_matches(cursor):
    """
    Fetches distinct tournaments and seasons that have matches finished in the last 12 hours.

    Returns:
        list of tuples: A list containing tuples of (tournament_id, season_id).
    """
    query = """
        SELECT DISTINCT tournament_id, season_id
        FROM matches
        WHERE match_status = 'finished' AND match_time >= NOW() - INTERVAL 12 HOUR
    """
    cursor.execute(query)
    return cursor.fetchall()


def fetch_standings(tournament_id, season_id):
    """
    Fetches standings information for a specific tournament and season from an external API.

    Args:
        tournament_id (int): The ID of the tournament to fetch fixtures for.
        season_id (int): The ID of the season to fetch fixtures for.

    Returns:
        A list of dictionaries containing fixture information.
    """
    endpoint = config['api']["endpoints"]["standings"].format(tournament_id, season_id)
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
    # If standings_data is a list directly, process it as such
    if isinstance(standings_data, list):
        groups = standings_data
    # If standings_data is a dict, assume 'standings' key contains the relevant list
    elif "standings" in standings_data:
        groups = standings_data.get("standings", [])
    else:
        groups = []

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


def insert_standings(cursor, conn, tournament_id, season_id, group_name, team_id, position, played, wins,
                     losses, draws, scored, conceded, points):
    try:
        insert_sql = """
            INSERT INTO standings (tournament_id, season_id, team_id, group_name, position, played, wins, 
            losses, draws, scored, conceded, points)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_sql, (
            tournament_id, season_id, team_id, group_name, position, played, wins, losses, draws, scored,
            conceded, points))
        conn.commit()
    except Exception as e:
        logger.error(f"Error inserting standings for tournament_id: {tournament_id}, season_id: {season_id}, "
                     f"team_id: {team_id}. Error: {e}")


def delete_standings(cursor, conn, tournament_id, season_id):
    try:
        delete_sql = """
            DELETE FROM standings 
            WHERE tournament_id = %s AND season_id = %s
        """
        cursor.execute(delete_sql, (tournament_id, season_id))
        conn.commit()
    except Exception as e:
        logger.error(f"Error deleting standings for tournament_id: {tournament_id},season_id: {season_id}. Error: {e}")


def main_standings():
    """
    Main function to handle the standings update process.
    Fetches and updates standings for tournaments and seasons based on recent match results.
    """
    start_time = datetime.now()
    logger.info("Starting the standings update process.")

    conn = get_db_connection()
    cursor = conn.cursor()

    # Fetch recent matches to determine which standings to update
    recent_matches = get_recent_matches(cursor)

    for tournament_id, season_id in recent_matches:
        # Fetch and parse standings data from the API
        standings_data = fetch_standings(tournament_id, season_id)
        parsed_standings = parse_standings_data(standings_data, tournament_id, season_id)

        # Delete existing standings for the current tournament and season
        delete_standings(cursor, conn, tournament_id, season_id)

        # Insert new standings data
        for standing in parsed_standings:
            insert_standings(cursor, conn, **standing)

    # Summary log
    logger.info(f"Standings update process completed. Execution time: {datetime.now() - start_time}.")

    # Clean up
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main_standings()
