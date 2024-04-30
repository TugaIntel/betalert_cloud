#!/usr/bin/env python3
import time
from utils import get_db_connection, make_api_call
from log import setup_logger
from config_loader import load_config
from mysql.connector import IntegrityError
import logging

# Setup logger
logger = setup_logger('players', 'INFO')

# Load configurations
config = load_config()


def get_teams(cursor):
    """
    Fetches all team IDs from the teams table.
    Returns:
        list of int: A list containing team IDs.
    """
    cursor.execute("""
        SELECT id FROM teams
    """)
    return [row[0] for row in cursor.fetchall()]


def get_players(cursor):
    """
    Fetches all existing players from the database.
    Returns:
        dict: Existing players keyed by a tuple of (player ID, team ID).
    """
    cursor.execute("""
        SELECT id, name, short_name, position, market_value, team_id FROM players
    """)
    return {(row[0], row[5]): {  # Key by a tuple of (player_id, team_id)
        'name': row[1],
        'short_name': row[2],
        'position': row[3],
        'market_value': row[4],
        'team_id': row[5]
    } for row in cursor.fetchall()}


def fetch_team_players(team_id):
    """
    Fetches player details for a given team from the API.
    Args:
        team_id (int): The ID of the team.
    Returns:
        list of dicts: Player details from the API.
    """
    endpoint = config['api']['endpoints']['players'].format(team_id)
    response = make_api_call(endpoint)
    return response['players'] if response else []


def parse_player_data(player_container, team_id):
    """
    Parses player data from the API response.
    Args:
        player_container (dict): Container with 'player' key holding player data from the API.
        team_id (int): The ID of the team to which the player belongs.
    Returns:
        dict: Parsed player data suitable for database insertion or update.
    """
    player_data = player_container.get('player')
    if not player_data:
        return None

    player_id = player_data.get('id')
    if player_id is None or team_id is None:
        return None

    return {
        'id': player_id,
        'name': player_data.get('name'),
        'short_name': player_data.get('shortName'),
        'position': player_data.get('position'),
        'market_value': player_data.get('proposedMarketValue'),
        'team_id': team_id,
    }


def insert_player(cursor, conn, player_data):
    """
    Inserts a new player into the database.
    Args:
        cursor: Database cursor object.
        conn: Database connection object.
        player_data (dict): The parsed player data.
    """
    try:
        insert_sql = """
            INSERT INTO players (name, short_name, position, market_value, team_id, id)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_sql, (
            player_data['name'],
            player_data['short_name'],
            player_data['position'],
            player_data['market_value'],
            player_data['team_id'],
            player_data['id']
        ))
        conn.commit()
    except IntegrityError as e:
        if 'Column \'team_id\' cannot be null' in str(e):
            logger.warning(f"IntegrityError: Team ID cannot be null for player "
                           f"{player_data['name']} ({player_data['id']}).")
        else:
            logger.error(f"Error inserting player {player_data['id']}: {e}")


def update_player(cursor, conn, player_data):
    """
    Updates an existing player in the database.
    Args:
        cursor: Database cursor object.
        conn: Database connection object.
        player_data (dict): The parsed player data.
    """
    try:
        update_sql = """
            UPDATE players
            SET name = %s, short_name = %s, position = %s, market_value = %s
            WHERE id = %s AND team_id = %s
        """
        cursor.execute(update_sql, (
            player_data['name'],
            player_data['short_name'],
            player_data['position'],
            player_data['market_value'],
            player_data['id'],
            player_data['team_id']
        ))
        conn.commit()
    except IntegrityError as e:
        if 'Column \'team_id\' cannot be null' in str(e):
            logger.warning(f"IntegrityError: Team ID cannot be null for player "
                           f"{player_data['name']} ({player_data['id']}).")
        else:
            logger.error(f"Error updating player {player_data['id']}: {e}")


def main_players():
    start_time = time.time()

    """
    Main function to update player details in the database.
    """

    conn = get_db_connection()
    cursor = conn.cursor()

    inserted_count = 0
    updated_count = 0

    team_ids = get_teams(cursor)
    existing_players = get_players(cursor)

    for team_id in team_ids:
        players_container = fetch_team_players(team_id)
        for player_container in players_container:
            parsed_data = parse_player_data(player_container, team_id)

            # Skip processing if parsed_data is None
            if not parsed_data:
                continue

            # Extract the player ID and team ID for clarity and error-checking
            composite_key = (parsed_data['id'], parsed_data['team_id'])

            existing_data = existing_players.get(composite_key)
            if existing_data:
                comparable_fields = ['name', 'short_name', 'position', 'market_value']
                needs_update = False
                for field in comparable_fields:
                    existing_value = str(existing_data.get(field, ''))
                    new_value = str(parsed_data.get(field, ''))
                    if existing_value != new_value:
                        needs_update = True
                        logger.debug(
                            f"Difference found for player {composite_key}: {field}, "
                            f"DB: {existing_value}, New: {new_value}")
                        break

                if needs_update:
                    try:
                        update_player(cursor, conn, parsed_data)
                        updated_count += 1
                    except Exception as e:
                        logging.error(f"Error updating player {parsed_data['id']}: {e}")
            else:
                try:
                    insert_player(cursor, conn, parsed_data)
                    inserted_count += 1
                except Exception as e:
                    logging.error(f"Error inserting player {parsed_data['id']}: {e}")

    cursor.close()
    conn.close()

    logger.info(f"Inserted {inserted_count} new players, updated {updated_count} existing players.")
    logger.info(f"Total execution time: {time.time() - start_time:.4f} seconds")


if __name__ == "__main__":
    main_players()
