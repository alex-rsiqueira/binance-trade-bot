import os
from google.cloud import secretmanager

PROJECT_ID = os.environ.get("PROJECT_ID")

def read_secret(secret_name):

    # Instantiate Secret Manager client
    client = secretmanager.SecretManagerServiceClient()

    # Build secret path
    name = client.secret_version_path(PROJECT_ID, secret_name, 'latest')

    # Get secret content
    response = client.access_secret_version(request={"name": name})

    # Decode secret content
    secret_value = response.payload.data.decode("UTF-8")

    return secret_value

if _name_ == '_main_':
    secret_data = read_secret('binance-cred')

    app_key = secret_data['app_key']
    app_secret = secret_data['app_secret']