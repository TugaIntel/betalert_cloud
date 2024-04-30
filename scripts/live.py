#!/usr/bin/env python3
from utils import get_db_connection, make_api_call, send_alert
from log import setup_logger
from config_loader import load_config

# Setup logger
logger = setup_logger('live', 'INFO')

# Load configurations
config = load_config()


def fetch_current_live_matches():
    response = make_api_call(config['api']['endpoints']['live_matches'])
    if not response:
        logger.error("Failed to fetch live matches.")
        return []
    try:
        live_matches_data = response.get('events', [])
        logger.info(f"Found {len(live_matches_data)} live matches.")
        return [match['id'] for match in live_matches_data]
    except KeyError:
        logger.error("Incorrect data format for live matches.")
        return []


def fetch_live_match_incidents(match_id):
    endpoint_template = config['api']['endpoints']['incidents']
    endpoint = endpoint_template.format(match_id)
    response = make_api_call(endpoint)
    if not response:
        return []
    return response.get('incidents', [])


def fetch_teams_info(cursor, match_id):
    cursor.execute("""
         SELECT minutes, country, tournament, home, away, home_score, away_score, home_pos, away_pos,
               score_ratio, conceded_ratio, h_squad_m, a_squad_m, squad_ratio,  h_lineup_m, a_squad_m
        FROM v_matches_live WHERE match_id = %s
        AND h_squad_m is not null and a_squad_m is not null 
    """, (match_id,))
    result = cursor.fetchone()
    if result:
        # Explicitly map the tuple to a dictionary, including new fields
        teams_info = {
            'home': {
                'team': result[3],
                'score': result[5],
                'squad_value': result[11],
                'lineup_value': result[14],
                'standing_position': result[7],
            },
            'away': {
                'team': result[4],
                'score': result[6],
                'squad_value': result[12],
                'lineup_value': result[15],
                'standing_position': result[8],
            },
            'match_minutes': result[0],
            'tournament': result[2],
            'country': result[1],
            'squad_ratio': result[13],
            'score_ratio': result[9],
            'concede_ratio': result[10],
        }
        return teams_info
    return {}  # Return an empty dict if no result


def process_red_card_alerts(cursor, conn, match_id, incidents):
    teams_info = fetch_teams_info(cursor, match_id)
    if not teams_info:
        return

    for incident in incidents:
        if (incident['incidentType'] == 'card' and incident.get('incidentClass') in ['red', 'yellowRed']
                and incident['time'] < 80):

            incident_id = incident['id']

            try:
                cursor.execute("SELECT 1 FROM incidents WHERE id = %s AND is_processed = 1", (incident_id,))

                if cursor.fetchone():
                    continue

                cursor.execute("""
                                INSERT INTO incidents (id, is_processed, processed_at)
                                VALUES (%s, %s, NOW())
                                ON DUPLICATE KEY UPDATE processed_at = NOW(), is_processed = 1
                            """, (incident_id, 1))

                message = construct_alert_message("Red Card", teams_info, incident)
                send_alert(message)
                logger.info(f"Red card alert sent for match ID: {match_id}.")
                conn.commit()

            except Exception as e:
                logger.error(f"Failed to insert/update incident for match ID {match_id}. Error: {e}")
                conn.rollback()


def construct_alert_message(incident_type, teams_info, incident):
    team_received = "Home team" if incident.get('isHome', False) else "Away team"
    home_value = f"{teams_info['home']['lineup_value']}M" if teams_info['home']['lineup_value'] \
        else f"{teams_info['home']['squad_value']}M"
    away_value = f"{teams_info['away']['lineup_value']}M" if teams_info['away']['lineup_value'] \
        else f"{teams_info['away']['squad_value']}M"

    message = (
        f"Alert: {incident_type}\n"
        f"{teams_info['tournament']} ({teams_info['country']})\n"
        f"{teams_info['home']['team']} vs. {teams_info['away']['team']}\n"
        f"Current Score: {teams_info['home']['score']} - {teams_info['away']['score']}\n"
        f"Incident Time: {incident['time']} minutes\n"
        f"{team_received} received a red card.\n"
        f"Team info: Pos {teams_info['home']['standing_position']} vs Pos {teams_info['away']['standing_position']}\n"
        f"Goal Ratio: {teams_info['score_ratio']}/{teams_info['concede_ratio']}\n"
        f"Values: {home_value} vs {away_value} (Squad Ratio: {teams_info['squad_ratio']})\n"
    )
    return message


def main_real_time_processing():
    logger.info("Starting real-time match processing.")
    conn = get_db_connection()
    cursor = conn.cursor()

    live_match_ids = fetch_current_live_matches()
    for match_id in live_match_ids:
        incidents = fetch_live_match_incidents(match_id)
        if not incidents:
            logger.debug(f"No incidents found for match ID: {match_id}")
            continue

        # Call specific function to process red card alerts
        process_red_card_alerts(cursor, conn, match_id, incidents)

    cursor.close()
    conn.close()
    logger.info("Real-time match processing completed.")


if __name__ == "__main__":
    main_real_time_processing()
