#!/usr/bin/env python3
import time
import mysql
from utils import get_db_connection, make_api_call
from log import setup_logger
from config_loader import load_config
from mysql.connector import IntegrityError

# Setup logger
logger = setup_logger('teams', 'INFO')

# Load configurations
config = load_config()


def get_distinct_teams(cursor):
    """
    Fetches distinct team IDs from the standings table.
    Returns:
        list of int: A list containing unique team IDs.
    """
    query = """SELECT DISTINCT home_team_id FROM matches 
        WHERE match_time >= DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)
        UNION 
        SELECT DISTINCT away_team_id FROM matches 
        WHERE match_time >= DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)
        """
    cursor.execute(query)
    return [row[0] for row in cursor.fetchall()]


def get_teams_details(cursor):
    # SQL query to get all team details from the teams table
    cursor.execute("""
        SELECT id, name, short_name, user_count, stadium_capacity,primary_tournament_id
        FROM teams
    """)
    return {row[0]: {
        'name': row[1],
        'short_name': row[2],
        'user_count': row[3],
        'stadium_capacity': row[4],
        'primary_tournament_id': row[5],
    } for row in cursor.fetchall()}


def fetch_team_details(team_id):
    """
    Fetches team details from the API for a given team ID.
    Args:
        team_id (int): The unique ID of the team.
    Returns:
        dict: Team details from the API.
    """
    endpoint = config['api']['endpoints']['team'].format(team_id)
    return make_api_call(endpoint)


def parse_team_details(team_data):
    """
    Parses team data from the API response.
    Args:
        team_data (dict): Raw team data from the API.
    Returns:
        dict: Parsed team data suitable for database insertion or update.
    """
    if not team_data:
        return {}
    team = team_data.get('team', {})
    return {
        'id': team.get('id'),
        'name': team.get('name'),
        'short_name': team.get('shortName'),
        'user_count': team.get('userCount', 0),
        'stadium_capacity': team.get('venue', {}).get('stadium', {}).get('capacity', 0),
        'primary_tournament_id': team.get('primaryUniqueTournament', {}).get('id')
    }


def insert_team(cursor, conn, team_data):
    """
    Inserts a new team record into the database.

    Args:
        cursor: Database cursor object.
        conn: Database connection object.
        team_data (dict): The parsed team data.
    """
    try:
        insert_sql = """
            INSERT INTO teams (id, name, short_name, user_count, stadium_capacity, primary_tournament_id)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_sql, (
            team_data['id'],
            team_data['name'],
            team_data['short_name'],
            team_data['user_count'],
            team_data['stadium_capacity'],
            team_data['primary_tournament_id'],
        ))
        conn.commit()
    except mysql.connector.errors.IntegrityError as e:
        raise e


def update_team(cursor, conn, team_data):
    """
    Updates an existing team record in the database.

    Args:
        cursor: Database cursor object.
        conn: Database connection object.
        team_data (dict): The parsed team data.
    """
    try:
        update_sql = """
            UPDATE teams
            SET name = %s, short_name = %s, user_count = %s, stadium_capacity = %s, primary_tournament_id = %s
            WHERE id = %s
        """
        cursor.execute(update_sql, (
            team_data['name'],
            team_data['short_name'],
            team_data['user_count'],
            team_data['stadium_capacity'],
            team_data['primary_tournament_id'],
            team_data['id'],
        ))
        conn.commit()
    except mysql.connector.errors.IntegrityError as e:
        raise e


def update_squad_value(cursor, conn):
    update_query = """
    UPDATE teams
    SET squad_value = (
        SELECT round(SUM(market_value)/1000000,3)
        FROM players
        WHERE players.team_id = teams.id
    )
    """
    cursor.execute(update_query)
    conn.commit()


def update_team_reputation(cursor, conn):
    update_query = """
    UPDATE teams
    SET reputation = (
        user_count * 0.5 +
        stadium_capacity * 0.3 +
        (SELECT reputation FROM tournaments WHERE id = teams.primary_tournament_id) * 0.2
    )
    """
    cursor.execute(update_query)
    conn.commit()


def main_teams():
    start_time = time.time()

    """
    Main function to update team details in the database.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Initialize counters for inserted and updated teams
    inserted_count = 0
    updated_count = 0
    integrity_error_count = 0

    # Fetch distinct teams from standings
    team_ids = get_distinct_teams(cursor)

    # Fetch existing team details from database
    existing_teams = get_teams_details(cursor)

    for team_id in team_ids:
        team_data = fetch_team_details(team_id)
        parsed_data = parse_team_details(team_data)

        # Check if the team exists and if there are any changes
        if team_id in existing_teams:
            existing_data = existing_teams[team_id]
            needs_update = False
            difference_log = []  # For logging differences

            for key, value in parsed_data.items():
                if key != 'id':  # Skip comparing the ID
                    existing_value = str(existing_data.get(key, None))
                    new_value = str(value)
                    if existing_value != new_value:
                        needs_update = True
                    difference_log.append(f"{key}: {existing_value} -> {new_value}")

            if needs_update:
                try:
                    update_team(cursor, conn, parsed_data)
                    updated_count += 1
                except mysql.connector.errors.IntegrityError:
                    integrity_error_count += 1
        else:
            try:
                insert_team(cursor, conn, parsed_data)
                inserted_count += 1
            except mysql.connector.errors.IntegrityError:
                integrity_error_count += 1

    update_squad_value(cursor, conn)
    update_team_reputation(cursor, conn)

    cursor.close()
    conn.close()

    logger.info(f"Inserted {inserted_count} new teams, updated {updated_count} teams.")
    logger.info(f"Encountered {integrity_error_count} integrity errors.")
    logger.info(f"Total execution time: {time.time() - start_time:.4f} seconds")


if __name__ == "__main__":
    main_teams()
