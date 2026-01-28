# 1. Standard library imports (Python built-ins)
import os
import logging

# 2. Third-party imports
from google.cloud import secretmanager


BQ_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")


def get_secret(secret_id: str, project_id: str = BQ_PROJECT_ID) -> str:
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(name=name)
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logging.error(f"Error accessing secret '{secret_id}': {e}")
        raise

