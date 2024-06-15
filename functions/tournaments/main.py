import logging
import re
import google.cloud.logging
import time
from utils import get_session, close_session, make_api_call
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from config_loader import load_config
from datetime import datetime


# Sets up Google Cloud logging with the default client.
client = google.cloud.logging.Client()
client.setup_logging()

# Load configuration settings
config = load_config()


def get_countries(session):
    """
    Fetches all country IDs from the database.

    Args:
        session (Session): A database session object.

    Returns:
        list: A list of integer country IDs.
    """
    result = session.execute(text("SELECT id FROM countries"))
    return [row[0] for row in result.fetchall()]


def get_existing_tournaments(session):
    """
    Fetches existing tournament data from the database and returns them as a dictionary.

    Args:
        session (Session): A database session object.

    Returns:
        dict: A dictionary where keys are tournament IDs and values are dictionaries containing comprehensive
        tournament data.
    """
    result = session.execute(text("""
        SELECT id, name, tier, user_count, rounds, playoff_series, 
               perf_graph, standings_groups, start_date, end_date, country_id
        FROM tournaments
    """))
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
        } for row in result.fetchall()
    }


def insert_tournament(session, tournament_data):
    """
    Inserts a new tournament record into the 'tournaments' table with all necessary fields.

    Args:
        session (Session): A database session object.
        tournament_data (dict): A dictionary containing tournament data.
    """
    insert_tournament_sql = text("""
        INSERT INTO tournaments (name, tier, user_count, rounds, playoff_series, perf_graph, standings_groups, 
        start_date, end_date, country_id, id) 
        VALUES (:name, :tier, :user_count, :rounds, :playoff_series, :perf_graph, :standings_groups, 
                :start_date, :end_date, :country_id, :id)
    """)
    try:
        # Convert boolean values to integers
        tournament_data['rounds'] = int(tournament_data['rounds'])
        tournament_data['playoff_series'] = int(tournament_data['playoff_series'])
        tournament_data['perf_graph'] = int(tournament_data['perf_graph'])
        tournament_data['standings_groups'] = int(tournament_data['standings_groups'])

        session.execute(insert_tournament_sql, tournament_data)
        session.commit()
    except IntegrityError as e:
        if "1062" in str(e.orig):
            logging.warning(f"Skipped duplicate tournament with ID {tournament_data['id']}")
        else:
            logging.error(f"Failed to insert tournament {tournament_data['name']} ({tournament_data['id']}): {e}")
            raise
    except SQLAlchemyError as e:
        logging.error(f"Failed to insert tournament {tournament_data['name']} ({tournament_data['id']}): {e}")
        raise


def update_tournament(session, tournament_data):
    """
    Updates an existing tournament record in the 'tournaments' table with all necessary fields.

    Args:
        session (Session): A database session object.
        tournament_data (dict): A dictionary containing tournament data including the tournament ID.
    """
    update_tournament_sql = text("""
        UPDATE tournaments 
        SET name = :name, tier = :tier, user_count = :user_count, rounds = :rounds, playoff_series = :playoff_series, 
            perf_graph = :perf_graph, standings_groups = :standings_groups, start_date = :start_date, 
            end_date = :end_date, country_id = :country_id
        WHERE id = :id
    """)
    # Convert boolean values to integers
    tournament_data['rounds'] = int(tournament_data['rounds'])
    tournament_data['playoff_series'] = int(tournament_data['playoff_series'])
    tournament_data['perf_graph'] = int(tournament_data['perf_graph'])
    tournament_data['standings_groups'] = int(tournament_data['standings_groups'])

    try:
        session.execute(update_tournament_sql, tournament_data)
        session.commit()
    except SQLAlchemyError as e:
        logging.error(f"Failed to update tournament {tournament_data['id']}: {e}")
        raise


def delete_outdated_tournaments(session):
    """
    Deletes tournaments that have ended before the current date.
    Associated seasons are automatically deleted due to ON DELETE CASCADE.
    """
    current_date_str = datetime.now().strftime('%Y-%m-%d')
    delete_query = text("DELETE FROM tournaments WHERE end_date < :current_date")
    result = session.execute(delete_query, {'current_date': current_date_str})
    deleted_count = result.rowcount
    session.commit()
    return deleted_count


def fetch_tournaments_list(country_id):
    """
    Fetches a list of tournaments from the API for a given country ID.

    Args:
        country_id (int): The ID of the country.

    Returns:
        dict: A dictionary containing the tournaments data.
    """
    endpoint = config['api']["endpoints"]["tournaments"].format(country_id)
    logging.debug(f"Fetching tournament list from API endpoint: {endpoint}")
    return make_api_call(endpoint)


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
    return make_api_call(endpoint)


def get_forced_tier(tournament_id):
    """
    Returns the forced tier for a given tournament ID if defined in FORCED_TIERS.

    Args:
        tournament_id (int): The ID of the tournament.

    Returns:
        int or None: The forced tier if defined, otherwise None.
    """
    forced_tiers = {
        None: [17138, 19293, 20360, 21261, 22327],
        1: [3085, 10609, 16601],
        2: [135, 212, 247, 777],
        3: [11085],
        4: [11417],
        19: [29]
    }

    for tier, ids in forced_tiers.items():
        if tournament_id in ids:
            return tier
    return None


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

    # Check for forced tier
    forced_tier = get_forced_tier(tournament_id)
    if forced_tier is not None:
        return forced_tier

    # Skip the record if the category contains "Amateur" and the gender is "M"
    if "Amateur" in country_name and gender == "M":
        return None

    # Check for lowerDivisions tier if tier is not available at the top level
    if tier is None and "lowerDivisions" in tournament:
        for lower_div in tournament["lowerDivisions"]:
            if lower_div.get("tier") is not None:
                tier = lower_div.get("tier") - 1
                break

    # Handle specific country and gender conditions
    special_country_ids = {1465, 1466, 1467, 1468, 1469, 1470, 1471}
    if country_id in special_country_ids:
        if gender == "M":
            return 20
        elif gender == "F":
            return 10

    if tier is not None and 1 <= tier <= 5:
        return tier
    if tier == 0:
        return 11 if gender is "F" else 21

    if tier is None:
        if re.search(r'\b(U20|U21|U23)\b', tournament_name):
            return 2
        if re.search(r'\bU19\b', tournament_name):
            return 3
        if re.search(r'\b(U16|U17)\b', tournament_name):
            return None
        return 12 if gender is "F" else 22

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


def check_and_update_tournament(existing_data, new_data, session):
    """
    Compares existing tournament data with new data and updates the database if there are changes.

    Args:
        existing_data (dict): Existing tournament data from the database.
        new_data (dict): New tournament data to compare against.
        session (Session): A database session object.
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
        update_tournament(session, new_data)


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
    elif tier > 20:
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


def update_tournament_reputation(session):
    """
    Updates the 'reputation' and 'reputation_tier' fields for all tournaments in the database.

    Args:
        session (Session): A database session object.
    """
    result = session.execute(text("SELECT id, user_count, tier FROM tournaments"))
    tournaments = result.fetchall()
    for tournament in tournaments:
        tournament_id, user_count, tier = tournament
        reputation = calculate_reputation(user_count, tier)
        reputation_tier = tier_name(reputation)

        update_sql = text("""
            UPDATE tournaments
            SET reputation = :reputation, reputation_tier = :reputation_tier
            WHERE id = :id
        """)
        session.execute(update_sql, {'reputation': reputation, 'reputation_tier': reputation_tier, 'id': tournament_id})
    session.commit()


def tournaments_main(request):
    """
    Main function to handle tournament data fetching and updates.

    Args:
        request (flask.Request): The request object.

    Returns:
        tuple: A response tuple containing a message and a status code.
    """
    current_date_str = datetime.now().strftime('%Y-%m-%d')

    start_time = time.time()
    logging.info("Tournaments function execution started.")

    inserted_count = 0
    updated_count = 0
    db_session = get_session()
    session = None

    try:
        session = db_session()
        country_ids = get_countries(session)
        existing_tournaments = get_existing_tournaments(session)

        tournaments_to_insert = []
        tournaments_to_update = []

        for country_id in country_ids:
            groups_data = fetch_tournaments_list(country_id)
            if 'groups' in groups_data:
                for group in groups_data['groups']:
                    if 'uniqueTournaments' in group:
                        for tournament_detail in group['uniqueTournaments']:
                            tournament_id = tournament_detail['id']
                            tournament_details = fetch_tournament_details(tournament_id)
                            parsed_data = parse_tournaments_details(tournament_details, current_date_str, country_id)

                            for tournament_parsed_detail in parsed_data:
                                if tournament_parsed_detail['id'] in existing_tournaments:
                                    existing_data = existing_tournaments[tournament_parsed_detail['id']]
                                    check_and_update_tournament(existing_data, tournament_parsed_detail, session)
                                    tournaments_to_update.append(tournament_parsed_detail)
                                    if len(tournaments_to_update) >= 100:
                                        for tournament_data in tournaments_to_update:
                                            update_tournament(session, tournament_data)
                                        updated_count += len(tournaments_to_update)
                                        tournaments_to_update.clear()
                                else:
                                    tournaments_to_insert.append(tournament_parsed_detail)
                                    if len(tournaments_to_insert) >= 100:
                                        for tournament_data in tournaments_to_insert:
                                            insert_tournament(session, tournament_data)
                                        inserted_count += len(tournaments_to_insert)
                                        tournaments_to_insert.clear()

        # Insert any remaining tournaments in the batch
        if tournaments_to_insert:
            for tournament_data in tournaments_to_insert:
                insert_tournament(session, tournament_data)
            inserted_count += len(tournaments_to_insert)

        # Update any remaining tournaments in the batch
        if tournaments_to_update:
            for tournament_data in tournaments_to_update:
                update_tournament(session, tournament_data)
            updated_count += len(tournaments_to_update)

        deleted_count = delete_outdated_tournaments(session)
        logging.info(
            f"{inserted_count} new tournaments inserted, {updated_count} tournaments updated, "
            f"{deleted_count} tournaments deleted."
        )

        update_tournament_reputation(session)

    except Exception as e:
        if session:
            session.rollback()
        logging.error(f"An error occurred during the country update process: {e}", exc_info=True)
        return f'An error occurred: {str(e)}', 500

    finally:
        if db_session:
            close_session(db_session)

    logging.info(f"Total execution time: {time.time() - start_time:.4f} seconds")
    return 'Function executed successfully', 200
