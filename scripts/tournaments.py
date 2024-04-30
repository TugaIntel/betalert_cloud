#!/usr/bin/env python3
import time
from datetime import datetime
from utils import get_db_connection, make_api_call  # Import utility functions
from log import setup_logger  # Import logging functionality
from config_loader import load_config  # Import configuration loader

# Set up logger for the tournaments entity
logger = setup_logger('tournaments', 'INFO')
config = load_config()


def get_countries():
    """
    Fetches all country IDs from the database.

    Returns:
        list: A list of integer country IDs.
    """

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM countries")
    country_ids = [row[0] for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return country_ids


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

    # Create a dictionary where each key is the tournament ID and the value is another dictionary of that
    # tournament's attributes
    tournaments = {
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

    return tournaments


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
    cursor.execute(insert_tournament_sql, tournament_data)
    conn.commit()


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
    cursor.execute(update_tournament_sql, tournament_data)
    conn.commit()


def delete_outdated_tournaments(cursor, conn):
    """
    Deletes tournaments that have ended before the current date.
    Associated seasons are automatically deleted due to ON DELETE CASCADE.
    """
    # Convert current epoch time to a formatted date string
    current_date_str = datetime.now().strftime('%Y-%m-%d')

    delete_query = "DELETE FROM tournaments WHERE end_date < %s"
    cursor.execute(delete_query, (current_date_str,))
    deleted_count = cursor.rowcount  # Get the number of rows affected
    conn.commit()
    return deleted_count


def fetch_tournaments_list(country_id):
    """Fetches a list of tournaments from the API for a given country ID."""

    endpoint = config['api']["endpoints"]["tournaments"].format(country_id)
    tournaments_data = make_api_call(endpoint)

    if tournaments_data:
        return tournaments_data
    else:
        return []


def fetch_tournament_details(tournament_id):
    """Fetches detailed information for a specific tournament from the API."""

    endpoint = config['api']["endpoints"]["tournament_detail"].format(tournament_id)
    tournament_details = make_api_call(endpoint)

    if tournament_details:
        return tournament_details
    else:
        return {}


def parse_tournaments_details(json_data, current_date_str, country_id):
    """
    Parses the JSON response data containing tournament information.

    Args:
        json_data (dict): The JSON response data from the API call.
        current_time_epoch (int): The current time in epoch seconds to filter out past tournaments.

    Returns:
        list: A list of dictionaries containing parsed tournament data.
        :param country_id:
        :param json_data:
        :param current_date_str:
    """

    tournaments = []

    if "uniqueTournament" in json_data:
        tournament = json_data["uniqueTournament"]

        # Convert the tournament's end timestamp to a date string
        end_date_str = datetime.fromtimestamp(tournament.get("endDateTimestamp", 0)).strftime('%Y-%m-%d')

        # Only proceed if the tournament's end date is in the future
        if end_date_str > current_date_str:
            # Default tier assignment if not specified or based on presence of lowerDivisions
            tier = tournament.get("tier", None)
            lower_divisions = tournament.get("lowerDivisions", [])

            # If no explicit tier is defined and there are lowerDivisions, assume it's a top-tier tournament
            if tier is None and lower_divisions:
                tier = 1  # Assign tier 1 since it has lower divisions
            # Assign default tier for cups or if no tier/lowerDivisions information is available
            elif tier is None and not lower_divisions:
                tier = 99
            # Adjust tier if explicitly set to 0
            elif tier == 0:
                tier = 2  # Convert tier 0 to tier 2
            # default value
            if tier is None:
                tier = 99

            start_date_str = datetime.fromtimestamp(tournament.get("startDateTimestamp", 0)).strftime('%Y-%m-%d')

            parsed_tournament = {
                "id": tournament["id"],
                "name": tournament["name"],
                "tier":  tier,
                "user_count": tournament["userCount"],
                "rounds": tournament["hasRounds"],
                "playoff_series": tournament["hasPlayoffSeries"],
                "perf_graph": tournament["hasPerformanceGraphFeature"],
                "standings_groups": tournament["hasStandingsGroups"],
                "start_date": start_date_str,
                "end_date": end_date_str,
                "country_id": country_id
            }
            tournaments.append(parsed_tournament)

    return tournaments


def calculate_reputation(user_count, tier):
    """
    Calculates a reputation score for a tournament based on user count and tier.

    This function assigns a higher reputation score to tournaments with a larger user count
    and a lower tier (indicating a more prestigious event).

    Args:
        user_count (int): The number of users participating in the tournament.
        tier (int): The tier of the tournament (potentially user-defined or API-provided).

    Returns:
        int: The calculated reputation score for the tournament (rounded to nearest integer).
    """

    if user_count > 2000 and tier == 99:  # Special case for high user count and tier 99 (unclassified)
        reputation = user_count * 1
    elif tier == 99:  # Unclassified tournaments (tier 99) get a lower base reputation
        reputation = user_count / 2
    else:
        reputation = user_count / tier  # Base reputation based on user count divided by tier

    return round(reputation, 0)  # Round the reputation score to the nearest integer


def determine_tier(reputation):
    """
    Assigns a tier label (e.g., 'top', 'good', 'medium', 'low', 'bottom') to a tournament
    based on its calculated reputation score.

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

    This function iterates through existing tournaments, calculates their reputation score using
    the `calculate_reputation` function, determines the corresponding tier label using the
    `determine_tier` function, and updates the database records accordingly.

    Args:
        cursor (cursor): A database cursor object.
        conn (connection): A database connection object.
    """

    cursor.execute("SELECT id, user_count, tier FROM tournaments")
    tournaments = cursor.fetchall()

    for tournament in tournaments:
        tournament_id, user_count, tier = tournament
        reputation = calculate_reputation(user_count, tier)
        reputation_tier = determine_tier(reputation)

        update_sql = """
            UPDATE tournaments
            SET reputation = %s, reputation_tier = %s
            WHERE id = %s
        """
        cursor.execute(update_sql, (reputation, reputation_tier, tournament_id))
    conn.commit()


def main_tournaments():
    start_time = time.time()
    current_date_str = datetime.now().strftime('%Y-%m-%d')

    """Main function to handle tournament data fetching and updates."""

    conn = get_db_connection()
    cursor = conn.cursor()

    # Initialize counters for inserted and updated tournaments
    inserted_count = 0
    updated_count = 0

    # Get the list of country IDs
    country_ids = get_countries()

    for country_id in country_ids:
        # Fetch a list of tournaments for the country
        groups_data = fetch_tournaments_list(country_id)
        if 'groups' in groups_data:
            for group in groups_data['groups']:
                if 'uniqueTournaments' in group:
                    for tournament_detail in group['uniqueTournaments']:
                        tournament_id = tournament_detail['id']
                        # Call Tournament detail API
                        tournament_details = fetch_tournament_details(tournament_id)
                        # Parse Details of each tournament
                        parsed_data = parse_tournaments_details(tournament_details, current_date_str, country_id)
                        # Existing tournaments fetched from the database
                        existing_tournaments = get_existing_tournaments(cursor)

                        for tournament_parsed_detail in parsed_data:
                            # Prepare common data for both insert and update
                            tournament_data = (
                                tournament_parsed_detail['name'],
                                tournament_parsed_detail['tier'],
                                tournament_parsed_detail['user_count'],
                                tournament_parsed_detail['rounds'],
                                tournament_parsed_detail['playoff_series'],
                                tournament_parsed_detail['perf_graph'],
                                tournament_parsed_detail['standings_groups'],
                                tournament_parsed_detail['start_date'],
                                tournament_parsed_detail['end_date'],
                                tournament_parsed_detail['country_id'],
                                tournament_parsed_detail['id']
                            )

                            if tournament_parsed_detail['id'] in existing_tournaments:
                                existing_data = existing_tournaments[tournament_parsed_detail['id']]
                                # List of fields you're interested in comparing
                                fields_to_compare = ['name', 'tier', 'user_count', 'rounds', 'playoff_series',
                                                     'perf_graph', 'standings_groups', 'start_date', 'end_date',
                                                     'country_id']

                                # Check if any of the selected fields have changed
                                needs_update = False
                                changed_fields = []  # To keep track of which fields have changed
                                for field in fields_to_compare:
                                    new_value = tournament_parsed_detail.get(field)
                                    existing_value = existing_data.get(field)

                                    # Special handling for date fields
                                    if field in ['start_date', 'end_date']:
                                        if isinstance(new_value, str):
                                            new_value = datetime.strptime(new_value,
                                                                          '%Y-%m-%d').date()

                                    # Adjusting for potential differences in data types, particularly for boolean values
                                    if field in ['rounds', 'playoff_series', 'perf_graph', 'standings_groups']:
                                        new_value = bool(new_value)
                                        existing_value = bool(existing_value)

                                    if new_value != existing_value:
                                        needs_update = True
                                        changed_fields.append((field, existing_value,
                                                               new_value))  # Log the field and the old vs new values

                                # Check for changes before updating
                                if needs_update:
                                    update_tournament(cursor, conn, tournament_data)
                                    updated_count += 1
                            else:
                                insert_tournament(cursor, conn, tournament_data)
                                inserted_count += 1

    # Be sure to move the deletion and logging outside the loop structure.
    deleted_count = delete_outdated_tournaments(cursor, conn)

    # Log the counters
    logger.info(
        f"{inserted_count} new tournaments inserted, {updated_count} tournaments updated, "
        f"{deleted_count} tournaments deleted.")

    # Call the function to update reputation
    update_tournament_reputation(cursor, conn)

    # Clean-up
    cursor.close()
    conn.close()
    logger.info(f"Total execution time: {time.time() - start_time:.4f} seconds")


if __name__ == "__main__":
    main_tournaments()
