#!/usr/bin/env python3
from utils import get_db_connection, send_alert
from log import setup_logger
from config_loader import load_config

# Setup logger
logger = setup_logger('pre-match', 'INFO')

# Load configurations
config = load_config()


def fetch_pre_match_info(cursor):
    # Query to fetch matches that will start in the next two hours from v_pre_match_analysis view
    query = """
        SELECT label, DATE_FORMAT(match_time, '%H:%i') AS match_time, country, tournament, 
               home, away, h_squad_m, a_squad_m, squad_ratio, score_ratio, conceded_ratio, 
               h_lineup_m, a_lineup_m, home_pos, away_pos, round_number
        FROM v_pre_match_analysis
        WHERE match_time BETWEEN NOW() + INTERVAL 28 MINUTE AND NOW() + INTERVAL 155 MINUTE
          AND label IS NOT NULL
          AND reputation_tier != 'bottom'
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


def main_pre_match_processing():
    logger.info("Fetching pre-match information...")
    conn = get_db_connection()
    cursor = conn.cursor()

    pre_match_info = fetch_pre_match_info(cursor)
    if pre_match_info:
        message = construct_alert_message(pre_match_info)
        send_alert(message)
        logger.info("Pre-match alert sent.")
    else:
        logger.info("No matches to alert.")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main_pre_match_processing()
