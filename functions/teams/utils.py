import os
import requests
import sqlalchemy
import logging
import google.cloud.logging
from sqlalchemy import text
from google.cloud import secretmanager
from google.cloud.sql.connector import Connector, IPTypes
from config_loader import load_config  # Import configuration loader
from sqlalchemy.orm import sessionmaker, scoped_session

# Sets up Google Cloud logging with the default client.
client = google.cloud.logging.Client()
client.setup_logging()

# Load configuration settings
config = load_config()


def load_secret_version(secret_id):
    """
    Retrieves a secret from Google Cloud Secret Manager.

    Args:
        secret_id (str): The ID of the secret to retrieve.

    Returns:
        str: The secret value.
    """
    secret = secretmanager.SecretManagerServiceClient()
    project_id = os.getenv('PROJECT_ID')
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = secret.access_secret_version(request={"name": name})
    return response.payload.data.decode('UTF-8')


def get_engine():
    """
    Create a SQLAlchemy engine for the MySQL database using the Cloud SQL Connector.

    Returns:
        engine (sqlalchemy.engine.Engine): A SQLAlchemy engine object.
    """
    # Retrieve database connection details from environment variables or other secure methods
    instance_connection_name = os.getenv("INSTANCE_CONNECTION_NAME")
    db_user = os.getenv("DB_USER")
    db_pass = load_secret_version('DB_PASS')
    db_name = os.getenv("DB_NAME")
    use_private_ip = os.getenv("PRIVATE_IP", "false").lower() == "true"

    ip_type = IPTypes.PRIVATE if use_private_ip else IPTypes.PUBLIC

    # Create a Connector object
    connector = Connector(ip_type=ip_type)

    # Create the SQLAlchemy engine using the Cloud SQL Connector
    engine = sqlalchemy.create_engine(
        "mysql+pymysql://",
        creator=lambda: connector.connect(
            instance_connection_name,
            "pymysql",
            user=db_user,
            password=db_pass,
            db=db_name
        ),
        pool_recycle=3600,  # Recycle connections after 1 hour
        pool_pre_ping=True  # Ensure the connection is alive
    )
    return engine


def get_session():
    """
    Create a scoped session for the MySQL database.

    Returns:
        scoped_session: A scoped session object.
    """
    engine = get_engine()
    db_session = sessionmaker(bind=engine)
    session = scoped_session(db_session)
    return session


def close_session(db_session):
    """
    Close the scoped session.

    Args:
        db_session (scoped_session): The scoped session to close.
    """
    db_session.remove()


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


def send_alert(message):
    """
    Sends an alert message to specified Telegram chat IDs using a bot.

    Args:
        message (str): The message to send.
    """
    bot_token = load_secret_version('telegram-bot-token')
    send_url = f'https://api.telegram.org/bot{bot_token}/sendMessage'

    # Fetch chat IDs from the database
    session = get_session()
    try:
        chat_ids = session.execute(text("SELECT id FROM chats")).fetchall()
        chat_ids = [row[0] for row in chat_ids]
    except Exception as e:
        logging.error(f"Failed to fetch chat IDs from database: {e}")
        return
    finally:
        close_session(session)

    for chat_id in chat_ids:
        try:
            response = requests.post(send_url, data={'chat_id': chat_id, 'text': message})
            response.raise_for_status()
        except requests.RequestException as e:
            logging.error(f"Failed to send message to chat ID {chat_id}: {e}")
