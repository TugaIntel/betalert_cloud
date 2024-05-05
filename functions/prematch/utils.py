import os
import requests
import pymysql
import sqlalchemy
import logging
import google.cloud.logging
from google.cloud import secretmanager
from google.cloud.sql.connector import Connector, IPTypes
from config_loader import load_config  # Import configuration loader

# Sets up Google Cloud logging with the default client.
client = google.cloud.logging.Client()
client.setup_logging()

# Load configuration settings
config = load_config()


def get_db_connection() -> sqlalchemy.engine.base.Engine:
    """
    Establishes a connection to the Google Cloud SQL instance using the Connector and returns a connection pool.

    Returns:
        sqlalchemy.engine.base.Engine: A SQLAlchemy connection pool to the MySQL database.
    """
    instance_connection_name = os.getenv("INSTANCE_CONNECTION_NAME")
    db_user = os.getenv("DB_USER")
    db_pass = load_secret_version('DB_PASS')
    db_name = os.getenv("DB_NAME")
    use_private_ip = os.getenv("PRIVATE_IP", "false").lower() == "true"

    ip_type = IPTypes.PRIVATE if use_private_ip else IPTypes.PUBLIC

    connector = Connector(ip_type=ip_type)

    def getconn() -> pymysql.connections.Connection:
        """
        Returns a new connection from the pool.
        Used internally by SQLAlchemy's create_engine.
        """
        return connector.connect(
            instance_connection_name,
            "pymysql",
            user=db_user,
            password=db_pass,
            db=db_name,
        )

    pool = sqlalchemy.create_engine(
        "mysql+pymysql://",
        creator=getconn,
    )
    logging.info("SQLAlchemy connection pool created.")
    return pool


def make_api_call(endpoint, params=None):
    """
    Performs an API call to a specified endpoint.

    Args:
        endpoint (str): The API endpoint to call.
        params (dict, optional): Parameters to pass to the API call.

    Returns:
        dict or None: The JSON response from the API call or None if an error occurs.
    """
    full_url = config['api']['base_url'] + endpoint
    headers = config['headers']
    logging.debug(f"Making API call to {full_url}.")

    try:
        response = requests.get(full_url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except requests.RequestException as e:
        logging.error(f"API call failed: {e}")
        return None


def load_secret_version(secret_id):
    """
        Retrieves a secret from Google Cloud Secret Manager.

        Args:
            secret_id (str): The ID of the secret to retrieve.

        Returns:
            str: The secret value.
    """
    client = secretmanager.SecretManagerServiceClient()
    project_id = os.getenv('PROJECT_ID')
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode('UTF-8')


def send_alert(message):
    """
    Sends an alert message to specified Telegram chat IDs using a bot.

    Args:
        message (str): The message to send.
    """
    engine = get_db_connection()
    conn = engine.raw_connection()
    cursor = conn.cursor()

    try:
        # Fetch chat IDs from the database
        cursor.execute("SELECT id FROM chats")
        chat_ids = [row[0] for row in cursor.fetchall()]

        # Load bot token from secrets or config
        bot_token = load_secret_version('telegram-bot-token')
        send_url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
        logging.info(f"send url {send_url}")
        # Send message to each chat ID
        for chat_id in chat_ids:
            try:
                response = requests.post(send_url, data={'chat_id': chat_id, 'text': message})
                logging.info(f"response {response}")
                # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
                response.raise_for_status()
            except requests.RequestException as e:
                logging.error(f"Failed to send message to chat ID {chat_id}: {e}")

    except Exception as e:
        logging.error(f"Error fetching chat IDs or sending messages: {e}")

    finally:
        cursor.close()
        conn.close()
