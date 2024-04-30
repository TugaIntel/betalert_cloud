import os
import json
import requests
import logging
import mysql.connector
from config_loader import load_config
from log import setup_logger
from google.cloud import secretmanager

# Setup logger for API calls
logger = setup_logger('api_caller', 'INFO')
config = load_config()


def make_api_call(endpoint, params=None, log_level='INFO'):
    """
    Makes an API call to the given endpoint and returns the JSON data if the response is 200.

    :param endpoint: The endpoint URL (str)
    :param params: Parameters to be sent in the query string (dict)
    :param log_level: Desired logging level for this API call ('DEBUG', 'INFO', etc.)
    :return: The JSON response data if successful, otherwise None
    """
    full_url = config['api']['base_url'] + endpoint
    headers = config['headers']

    # Temporarily set the logger to the desired level
    original_level = logger.getEffectiveLevel()
    logger.setLevel(getattr(logging, log_level.upper()))

    try:
        response = requests.get(full_url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            logger.log(getattr(logging, log_level.upper()),
                       f"API call to {full_url} returned unexpected status code {response.status_code}.")
            return None
    except requests.RequestException as e:
        logger.log(getattr(logging, log_level.upper()), f"API request to {full_url} failed with an error: {e}")
        return None
    finally:
        # Restore the original logging level
        logger.setLevel(original_level)


def load_secret_version(secret_id):
    """Load a secret version from Google Cloud Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode('UTF-8')


def get_db_connection():
    """Establishes a database connection using configuration from environment variables and secret manager."""
    # Fetch database password from Secret Manager
    db_password = load_secret_version('DB_PASSWORD')

    database_config = config['database']  # Correction here
    conn = mysql.connector.connect(
        host=database_config['DB_HOST'],
        user=database_config['DB_USER'],
        password=db_password,
        db=database_config['DB_NAME']
    )
    return conn


def send_alert(message):
    bot_token = load_secret_version('telegram-bot-token')
    chat_ids = json.loads(load_secret_version('telegram-chat-ids'))
    send_url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    for chat_id in chat_ids:
        requests.post(send_url, data={'chat_id': chat_id, 'text': message})
