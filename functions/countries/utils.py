import os
import json
import requests
import pymysql
import sqlalchemy
from config_loader import load_config
from google.cloud import secretmanager
from google.cloud.sql.connector import Connector, IPTypes

# Setup logger for API calls
config = load_config()


def get_db_connection() -> sqlalchemy.engine.base.Engine:
    """
    Establishes a connection to the Google Cloud SQL instance using the Connector and returns a connection pool.

    Returns:
        sqlalchemy.engine.base.Engine: A SQLAlchemy connection pool to the MySQL database.
    """
    instance_connection_name = os.getenv("INSTANCE_CONNECTION_NAME")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    db_name = os.getenv("DB_NAME")
    use_private_ip = os.getenv("PRIVATE_IP", "false").lower() == "true"

    ip_type = IPTypes.PRIVATE if use_private_ip else IPTypes.PUBLIC

    connector = Connector(ip_type=ip_type)

    def getconn() -> pymysql.connections.Connection:
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

    return pool


def make_api_call(endpoint, params=None):
    full_url = config['api']['base_url'] + endpoint
    headers = config['headers']

    try:
        response = requests.get(full_url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except requests.RequestException:
        return None


def load_secret_version(secret_id):
    """Load a secret version from Google Cloud Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode('UTF-8')


def send_alert(message):
    bot_token = load_secret_version('telegram-bot-token')
    chat_ids = json.loads(load_secret_version('telegram-chat-ids'))
    send_url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    for chat_id in chat_ids:
        requests.post(send_url, data={'chat_id': chat_id, 'text': message})
