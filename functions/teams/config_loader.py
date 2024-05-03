import json
import google.auth
from google.cloud import secretmanager
from google.cloud import storage


def load_config():
    """Load configuration JSON file from Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket('betalert_cloud')
    blob = bucket.blob('config.json')
    data = blob.download_as_text()
    return json.loads(data)


def load_secret_config(secret_id):
    """
    Load a secret from Google Cloud Secret Manager.

    Args:
    secret_id (str): The ID of the secret to retrieve, e.g., 'DB_PASSWORD'

    Returns:
    str: The secret value.
    """
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret.
    project_id = google.auth.default()[1]
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


