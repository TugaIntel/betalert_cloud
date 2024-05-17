#!/usr/bin/env python3
import re
import time
import logging
from datetime import datetime
import pymysql
import google.cloud.logging
from utils import get_db_connection, make_api_call
from config_loader import load_config

# Set up Google Cloud logging with the default client.
client = google.cloud.logging.Client()
client.setup_logging()

# Load configuration settings
config = load_config()

EXCEPTION_TIERS = {
    29: 19,  # Norway, NM Cup
    135: 2,  # Austria, 2. Liga
    212: 2,  # Slovenia, PrvaLiga
    247: 2,  # Bulgaria, Parva Liga
    3085: 1,  # Japan, Nadeshiko League, Div. 1, Women
    10609: 1,  # Greece, Super League, Women
    11417: 4,  # Turkey, TFF 3. Lig, Grup 1
    17138: None,
    19293: None,
    20360: None,
    21261: None,
    22327: None
}


def get_countries(cursor):
    """
    Fetches all country IDs from the database.

    Args:
        cursor (cursor): A database cursor object.

    Returns:
        list: A list of integer country IDs.
    """
    cursor.execute("SELECT id FROM countries")
    return [row[0] for row in cursor.fetchall()]


def get_existing_tournaments(cursor):
    """
    Fetches existing tournament data from the database and returns them as a dictionary.

    Args:
        cursor (cursor): A database cursor object.

    Returns:
        dict: A dictionary where keys are tournament IDs and values are dictionaries containing comprehensive
        tournament data.
    """
    cursor.execute("""
        SELECT id, name, tier, user_count, rounds, playoff_series, 
               perf_graph, standings_groups, start_date, end_date, country_id
        FROM tournaments
    """)
    return {
        row[0]: {
            "name": row[1],
            "tier": row[2],
            "user_count": row[3],
            "rounds": row[4],
            "playoff_series": row[5],
            "perf_graph": row[6],
            "standings_groups": row[7],
            "start_date": row[8],
            "end_date": row[9],
            "country_id": row[10]
        } for row in cursor.fetchall()
    }


def insert_tournament(cursor, conn, tournament_data):
    """
    Inserts a new tournament record into the 'tournaments' table with all necessary fields.

    Args:
        cursor (cursor): A database cursor object.
        conn (connection): A database connection object.
        tournament_data (tuple): All necessary tournament data.
    """
    insert_tournament_sql = """
        INSERT INTO tournaments (name, tier, user_count, rounds, playoff_series, perf_graph, standings_groups, 
        start_date, end_date, country_id, id) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
        # Convert boolean values to integers
        tournament_data = (
            tournament_data[0], tournament_data[1], tournament_data[2],
            int(tournament_data[3]), int(tournament_data[4]),
            int(tournament_data[5]), int(tournament_data[6]),
            tournament_data[7], tournament_data[8],
            tournament_data[9], tournament_data[10]
        )
        cursor.execute(insert_tournament_sql, tournament_data)
        conn.commit()
    except pymysql.err.IntegrityError as e:
        if e.args[0] == 1062:
            logging.warning(f"Skipped duplicate tournament with ID {tournament_data[-1]}")
        else:
            raise
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise


def update_tournament(cursor, conn, tournament_data):
    """
    Updates an existing tournament record in the 'tournaments' table with all necessary fields.

    Args:
        cursor (cursor): A database cursor object.
        conn (connection): A database connection object.
        tournament_data (tuple): All necessary tournament data including the tournament ID at the end.
    """
    update_tournament_sql = """
        UPDATE tournaments 
        SET name = %s, tier = %s, user_count = %s, rounds = %s, playoff_series = %s, perf_graph = %s, 
        standings_groups = %s, start_date = %s, end_date = %s, country_id = %s
        WHERE id = %s
    """
    # Convert boolean values to integers
    tournament_data = (
        tournament_data[0], tournament_data[1], tournament_data[2],
        int(tournament_data[3]), int(tournament_data[4]),
        int(tournament_data[5]), int(tournament_data[6]),
        tournament_data[7], tournament_data[8],
        tournament_data[9], tournament_data[10]
    )
    cursor.execute(update_tournament_sql, tournament_data)
    conn.commit()


def delete_outdated_tournaments(cursor, conn):
    """
    Deletes tournaments that have ended before the current date.
    Associated seasons are automatically deleted due to ON DELETE CASCADE.
    """
    current_date_str = datetime.now().strftime('%Y-%m-%d')
    delete_query = "DELETE FROM tournaments WHERE end_date < %s"
    cursor.execute(delete_query, (current_date_str,))
    deleted_count = cursor.rowcount
    conn.commit()
    return deleted_count


def fetch_tournaments_list(country_id):
    """
    Fetches a list of tournaments from the API for a given country ID.

    Args:
        country_id (int): The ID of the country.

    Returns:
        list: A list of tournament data.
    """
    endpoint = config['api']["endpoints"]["tournaments"].format(country_id)
    logging.debug(f"Fetching tournament list from API endpoint: {endpoint}")
    tournaments_data = make_api_call(endpoint)
    if tournaments_data:
        return tournaments_data
    else:
        logging.info(f"No tournaments found for country ID: {country_id}")
        return []


def fetch_tournament_details(tournament_id):
    """
    Fetches detailed information for a specific tournament from the API.

    Args:
        tournament_id (int): The ID of the tournament.

    Returns:
        dict: Detailed tournament data.
    """
    endpoint = config['api']["endpoints"]["tournament_detail"].format(tournament_id)
    logging.debug(f"Fetching tournament details from API endpoint: {endpoint}")
    tournament_details = make_api_call(endpoint)
    if tournament_details:
        return tournament_details
    else:
        logging.info(f"No tournament details found for tournament ID: {tournament_id}")
        return []


def determine_tier(tournament):
    """
    Determines the tier of a tournament based on various rules and exceptions.

    Args:
        tournament (dict): The tournament data.

    Returns:
        int or None: The tier of the tournament, or None if the tournament should be skipped.
    """
    tournament_id = tournament.get("id")
    tier = tournament.get("tier")
    gender = tournament.get("titleHolder", {}).get("gender", "M")
    country_id = tournament.get("category", {}).get("country", {}).get("id", None)
    country_name = tournament.get("category", {}).get("name", "")
    tournament_name = tournament.get("name", "")

    # Skip the record if the category contains "Amateur" and the gender is "M"
    if "Amateur" in country_name and gender == "M":
        return None

    # Handle specific country and gender conditions
    special_country_ids = {1465, 1466, 1467, 1468, 1469, 1470, 1471}
    if country_id in special_country_ids:
        if gender == "M":
            return 20
        elif gender == "F":
            return 10

    # Check for known exceptions
    if tournament_id in EXCEPTION_TIERS:
        corrected_tier = EXCEPTION_TIERS[tournament_id]
        if corrected_tier is None:
            return None
        return corrected_tier

    if tier is not None and 1 <= tier <= 5:
        return tier
    if tier == 0:
        return 11 if gender == "F" else 21

    if tier is None:
        if re.search(r'\b(U20|U21|U23)\b', tournament_name):
            return 2
        if re.search(r'\bU19\b', tournament_name):
            return 3
        if re.search(r'\b(U16|U17)\b', tournament_name):
            return None
        return 12 if gender == "F" else 22

    return 99


def parse_tournaments_details(json_data, current_date_str, country_id):
    """
    Parses the JSON response data containing tournament information.

    Args:
        json_data (dict): The JSON response data from the API call.
        current_date_str (str): The current date in 'YYYY-MM-DD' format.
        country_id (int): The ID of the country.

    Returns:
        list: A list of dictionaries containing parsed tournament data.
    """
    tournaments = []
    if "uniqueTournament" in json_data:
        tournament = json_data["uniqueTournament"]
        end_date_str = datetime.fromtimestamp(tournament.get("endDateTimestamp", 0)).strftime('%Y-%m-%d')
        if end_date_str > current_date_str:
            tier = determine_tier(tournament)
            if tier is None:
                return tournaments
            user_count = tournament.get("userCount", 0)
            rounds = tournament.get("hasRounds", False)
            playoff_series = tournament.get("hasPlayoffSeries", False)
            perf_graph = tournament.get("hasPerformanceGraphFeature", False)
            standings_groups = tournament.get("hasStandingsGroups", False)
            start_date_str = datetime.fromtimestamp(tournament.get("startDateTimestamp", 0)).strftime('%Y-%m-%d')

            parsed_tournament = {
                "id": tournament["id"],
                "name": tournament["name"],
                "tier": tier,
                "user_count": user_count,
                "rounds": int(rounds),  # Convert boolean to int
                "playoff_series": int(playoff_series),  # Convert boolean to int
                "perf_graph": int(perf_graph),  # Convert boolean to int
                "standings_groups": int(standings_groups),  # Convert boolean to int
                "start_date": start_date_str,
                "end_date": end_date_str,
                "country_id": country_id
            }
            tournaments.append(parsed_tournament)

    return tournaments


def check_and_update_tournament(existing_data, new_data, cursor, conn):
    """
    Compares existing tournament data with new data and updates the database if there are changes.

    Args:
        existing_data (dict): Existing tournament data from the database.
        new_data (dict): New tournament data to compare against.
        cursor (cursor): A database cursor object.
        conn (connection): A database connection object.
    """
    fields_to_compare = ['name', 'tier', 'user_count', 'rounds', 'playoff_series', 'perf_graph', 'standings_groups',
                         'start_date', 'end_date', 'country_id']
    needs_update = False
    changed_fields = []

    for field in fields_to_compare:
        new_value = new_data.get(field)
        existing_value = existing_data.get(field)

        if field in ['start_date', 'end_date'] and isinstance(new_value, str):
            new_value = datetime.strptime(new_value, '%Y-%m-%d').date()

        if field in ['rounds', 'playoff_series', 'perf_graph', 'standings_groups']:
            new_value = bool(new_value)
            existing_value = bool(existing_value)

        if new_value != existing_value:
            needs_update = True
            changed_fields.append((field, existing_value, new_value))

    if needs_update:
        logging.debug(f"Updating tournament ID {new_data['id']}: {changed_fields}")
        update_tournament(cursor, conn, (
            new_data['name'],
            new_data['tier'],
            new_data['user_count'],
            int(new_data['rounds']),
            int(new_data['playoff_series']),
            int(new_data['perf_graph']),
            int(new_data['standings_groups']),
            new_data['start_date'],
            new_data['end_date'],
            new_data['country_id'],
            new_data['id']
        ))


def calculate_reputation(user_count, tier):
    """
    Calculates a reputation score for a tournament based on user count and tier.

    Args:
        user_count (int): The number of users participating in the tournament.
        tier (int): The tier of the tournament (potentially user-defined or API-provided).

    Returns:
        int: The calculated reputation score for the tournament (rounded to nearest integer).
    """
    if user_count > 2000 and tier > 20:
        reputation = user_count * 1
    elif tier >20:
        reputation = user_count / 3
    elif 10 <= tier <= 20:
        reputation = user_count / 1.5
    else:
        reputation = user_count / tier

    return round(reputation, 0)


def tier_name(reputation):
    """
    Assigns a tier label to a tournament based on its calculated reputation score.

    Args:
        reputation (int): The calculated reputation score for the tournament.

    Returns:
        str: The designated tier label based on the reputation score thresholds.
    """
    if reputation > 200000:
        return 'top'
    elif reputation > 50000:
        return 'good'
    elif reputation > 10000:
        return 'medium'
    elif reputation > 1000:
        return 'low'
    else:
        return 'bottom'


def update_tournament_reputation(cursor, conn):
    """
    Updates the 'reputation' and 'reputation_tier' fields for all tournaments in the database.

    Args:
        cursor (cursor): A database cursor object.
        conn (connection): A database connection object.
    """
    cursor.execute("SELECT id, user_count, tier FROM tournaments")
    tournaments = cursor.fetchall()
    for tournament in tournaments:
        tournament_id, user_count, tier = tournament
        reputation = calculate_reputation(user_count, tier)
        reputation_tier = tier_name(reputation)

        update_sql = """
            UPDATE tournaments
            SET reputation = %s, reputation_tier = %s
            WHERE id = %s
        """
        cursor.execute(update_sql, (reputation, reputation_tier, tournament_id))
    conn.commit()


def tournaments_main(request):
    """
    Main function to handle tournament data fetching and updates.

    Args:
        request (flask.Request): The request object.

    Returns:
        tuple: A response tuple containing a message and a status code.
    """
    start_time = time.time()
    current_date_str = datetime.now().strftime('%Y-%m-%d')
    logging.info("Tournaments function execution started.")

    engine = get_db_connection()
    conn = engine.raw_connection()
    cursor = conn.cursor()

    try:
        inserted_count = 0
        updated_count = 0

        country_ids = get_countries(cursor)

        for country_id in country_ids:
            groups_data = fetch_tournaments_list(country_id)
            if 'groups' in groups_data:
                for group in groups_data['groups']:
                    if 'uniqueTournaments' in group:
                        for tournament_detail in group['uniqueTournaments']:
                            tournament_id = tournament_detail['id']
                            tournament_details = fetch_tournament_details(tournament_id)
                            parsed_data = parse_tournaments_details(tournament_details, current_date_str, country_id)
                            existing_tournaments = get_existing_tournaments(cursor)

                            for tournament_parsed_detail in parsed_data:
                                if tournament_parsed_detail['id'] in existing_tournaments:
                                    existing_data = existing_tournaments[tournament_parsed_detail['id']]
                                    check_and_update_tournament(existing_data, tournament_parsed_detail, cursor, conn)
                                    updated_count += 1
                                else:
                                    insert_tournament(cursor, conn, (
                                        tournament_parsed_detail['name'],
                                        tournament_parsed_detail['tier'],
                                        tournament_parsed_detail['user_count'],
                                        int(tournament_parsed_detail['rounds']),
                                        int(tournament_parsed_detail['playoff_series']),
                                        int(tournament_parsed_detail['perf_graph']),
                                        int(tournament_parsed_detail['standings_groups']),
                                        tournament_parsed_detail['start_date'],
                                        tournament_parsed_detail['end_date'],
                                        tournament_parsed_detail['country_id'],
                                        tournament_parsed_detail['id']
                                    ))
                                    inserted_count += 1

        deleted_count = delete_outdated_tournaments(cursor, conn)

        logging.info(
            f"{inserted_count} new tournaments inserted, {updated_count} tournaments updated, "
            f"{deleted_count} tournaments deleted.")

        update_tournament_reputation(cursor, conn)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return f'An error occurred: {str(e)}', 500

    finally:
        cursor.close()
        conn.close()

    logging.info(f"Total execution time: {time.time() - start_time:.4f} seconds")
    return 'Function executed successfully', 200
