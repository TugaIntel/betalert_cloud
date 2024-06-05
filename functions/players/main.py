#!/usr/bin/env python3
import time
import logging
import urllib.request
import json
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from utils import get_session, close_session  # Import utility functions
from config_loader import load_config  # Import configuration loader
import google.cloud.logging
from urllib import error

# Set up Google Cloud logging with the default client.
client = google.cloud.logging.Client()
client.setup_logging()

# Load configuration settings
config = load_config()


def get_teams(session):
    """
    Fetches all team IDs from the teams that have games to play in the next 24 hours.

    Args:
        session (Session): A database session object.

    Returns:
        list of int: A list containing team IDs.
    """
    query = text("""
        SELECT DISTINCT home_team_id FROM matches 
        WHERE match_time BETWEEN NOW() AND NOW() + INTERVAL 2 DAY
        UNION 
        SELECT DISTINCT away_team_id FROM matches 
        WHERE match_time BETWEEN NOW() AND NOW() + INTERVAL 2 DAY
    """)
    result = session.execute(query)
    return [row[0] for row in result.fetchall()]


def fetch_team_players(team_id):
    url = config['api']['base_url'] + config['api']['endpoints']['players'].format(team_id)
    try:
        with urllib.request.urlopen(url) as response:
            data = response.read()
            json_data = json.loads(data)
            return json_data.get('players', [])
    except urllib.error.URLError:
        return []


def parse_player_data(player_container, team_id):
    player_data = player_container.get('player')
    if not player_data:
        return None

    player_id = player_data.get('id')
    if player_id is None or team_id is None:
        return None

    market_value = player_data.get('proposedMarketValue', 0)
    market_value_k = market_value / 1000

    return {
        'id': player_id,
        'name': player_data.get('name'),
        'short_name': player_data.get('shortName'),
        'position': player_data.get('position'),
        'market_value': market_value_k,
        'team_id': team_id,
    }


def delete_players_by_team(session, team_ids):
    delete_sql = text("""
        DELETE FROM players WHERE team_id IN :team_ids
    """)
    try:
        session.execute(delete_sql, {'team_ids': tuple(team_ids)})
        session.commit()
    except SQLAlchemyError as e:
        logging.error(f"An error occurred while deleting players for teams {team_ids}: {e}")
        session.rollback()
        raise


def insert_players_batch(session, players_data):
    insert_sql = text("""
        INSERT INTO players (name, short_name, position, market_value, team_id, id)
        VALUES (:name, :short_name, :position, :market_value, :team_id, :id)
    """)
    try:
        session.execute(insert_sql, players_data)
        session.commit()
    except IntegrityError as e:
        logging.debug(f"IntegrityError while inserting players batch: {e}")
        session.rollback()
    except SQLAlchemyError as e:
        logging.debug(f"An error occurred while inserting players batch: {e}")
        session.rollback()


def update_squad_value(session):
    """
    Updates the squad value for all teams in the database.

    Args:
        session (Session): A database session object.
    """
    update_query = text("""
    UPDATE teams
    SET squad_value = (SELECT ROUND(SUM(market_value) / COUNT(*), 2)
                        FROM players
                        WHERE players.team_id = teams.id
                        AND players.market_value > 0 )
    """)
    try:
        session.execute(update_query)
        session.commit()
    except SQLAlchemyError as e:
        logging.error(f"An error occurred while updating squad values: {e}")
        session.rollback()


def players_main(request):
    start_time = time.time()
    logging.info("Players function execution started.")

    db_session = get_session()
    session = None

    try:
        session = db_session()
        team_ids = get_teams(session)
        logging.info(f"Number of teams to process: {len(team_ids)}")

        if team_ids:
            # Delete existing players for these teams
            delete_start_time = time.time()
            delete_players_by_team(session, team_ids)
            delete_duration = time.time() - delete_start_time
            logging.info(f"Time of execution for delete operation: {delete_duration:.4f} seconds")
        else:
            logging.info("No teams to process. Skipping delete operation.")

        inserted_count = 0
        teams_with_results = 0
        players_batch = []

        for team_id in team_ids:
            players_container = fetch_team_players(team_id)
            if players_container:
                teams_with_results += 1
            for player_container in players_container:
                parsed_data = parse_player_data(player_container, team_id)

                if not parsed_data:
                    continue

                players_batch.append(parsed_data)
                if len(players_batch) >= 100:  # Insert in batches of 100
                    insert_players_batch(session, players_batch)
                    inserted_count += len(players_batch)
                    players_batch.clear()

        # Insert any remaining players in the batch
        if players_batch:
            insert_players_batch(session, players_batch)
            inserted_count += len(players_batch)

        update_squad_value(session)

        logging.info(f"Inserted {inserted_count} new players.")
        logging.info(f"Number of teams with results from API call: {teams_with_results}")

    except Exception as e:
        if session:
            session.rollback()
        logging.error(f"An error occurred: {e}", exc_info=True)
        return f'An error occurred: {str(e)}', 500

    finally:
        if db_session:
            close_session(db_session)

    total_duration = time.time() - start_time
    logging.info(f"Total execution time: {total_duration:.4f} seconds")
    return 'Function executed successfully', 200
