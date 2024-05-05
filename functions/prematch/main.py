#!/usr/bin/env python3
import time
import logging
import google.cloud.logging
from utils import get_db_connection, send_alert
from config_loader import load_config

# Sets up Google Cloud logging with the default client.
client = google.cloud.logging.Client()
client.setup_logging()

# Load configuration settings
config = load_config()


def fetch_pre_match_info(cursor):
    # Query to fetch matches that will start in the next two hours from v_pre_match_analysis view
    query = """
        SELECT label, DATE_FORMAT(match_time, '%H:%i') AS match_time, country, tournament, 
               home, away, h_squad_m, a_squad_m, squad_ratio, score_ratio, conceded_ratio, 
               h_lineup_m, a_lineup_m, home_pos, away_pos, round_number
        FROM v_pre_match_analysis
        WHERE match_time BETWEEN NOW() + INTERVAL 25 MINUTE AND NOW() + INTERVAL 100 MINUTE
          AND label IS NOT NULL
          AND ((reputation_tier ='bottom' AND tier =99 AND user_count > 1000)
                    OR (tournament LIKE '%Women%')
                    OR (reputation_tier ='bottom' AND tier <=3)
                    OR reputation_tier != 'bottom')
        ORDER BY match_time, tournament_reputation DESC
    """
    cursor.execute(query)
    return cursor.fetchall()


def construct_alert_message(matches):
    message = "Upcoming Matches:\n\n"
    for row in matches:
        (label, match_time, country, tournament, home, away, h_squad_m, a_squad_m, squad_ratio, score_ratio,
         conceded_ratio, h_lineup_m, a_lineup_m, home_pos, away_pos, round_number) = row

        home_value = f"{h_lineup_m}M" if h_lineup_m and h_lineup_m != 0 else f"{h_squad_m}M"
        away_value = f"{a_lineup_m}M" if a_lineup_m and a_lineup_m != 0 else f"{a_squad_m}M"
        home_score_char = score_ratio[0] if score_ratio else ''
        away_score_char = score_ratio[1] if score_ratio else ''
        home_concede_char = conceded_ratio[0] if conceded_ratio else ''
        away_concede_char = conceded_ratio[1] if conceded_ratio else ''

        message += f"{label} in {country} {tournament} - {match_time}\n"
        message += f"Round {round_number}: {home}({home_pos}) vs {away}({away_pos})\n"
        message += f"Goal Ratio: {home_score_char}/{home_concede_char} vs {away_score_char}/{away_concede_char}\n"
        message += f"Values: {home_value} vs {away_value} (Ratio: {squad_ratio})\n\n"

    return message


def prematch_main(request):
    """ Main function to send pre match alerts. """
    start_time = time.time()
    logging.info("PreMatch function execution started.")

    engine = get_db_connection()  # This is an SQLAlchemy engine now
    conn = engine.raw_connection()  # Gets a raw connection from the engine
    cursor = conn.cursor()

    try:
        logging.info("Fetching pre-match information...")

        pre_match_info = fetch_pre_match_info(cursor)
        if pre_match_info:
            message = construct_alert_message(pre_match_info)
            send_alert(message)
            logging.info("Pre-match alert sent.")
        else:
            logging.info("No matches to alert.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return f'An error occurred: {str(e)}', 500

    finally:
        cursor.close()  # Ensure the cursor is closed after operations
        conn.close()  # Ensure the connection is closed after operations

    logging.info(f"Total execution time: {time.time() - start_time:.4f} seconds")
    return 'Function executed successfully', 200
