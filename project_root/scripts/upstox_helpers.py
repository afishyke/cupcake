import os
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import requests

# --- Configuration ---
BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_FILE = BASE_DIR / "config" / "name.json"
SECRETS_FILE = BASE_DIR / "credentials" / "secrets.json"
OUTPUT_DIR = BASE_DIR / "outputs"
SECRETS_CACHE = {}

# --- Local Secret Management ---

def load_secrets():
    """Loads secrets from the secrets.json file into a cache."""
    global SECRETS_CACHE
    try:
        with open(SECRETS_FILE, 'r') as f:
            SECRETS_CACHE = json.load(f)
    except FileNotFoundError:
        print(f"FATAL: Secrets file not found at {SECRETS_FILE}")
        print("Please create it based on the documentation.")
        raise
    except json.JSONDecodeError:
        print(f"FATAL: Could not decode JSON from {SECRETS_FILE}. Please check its format.")
        raise

def get_secret(secret_id: str) -> str:
    """Fetches a secret from the local cache."""
    if not SECRETS_CACHE:
        load_secrets()
    return SECRETS_CACHE.get(secret_id)

def update_secrets_file(new_data: dict):
    """Updates the secrets.json file with new data (e.g., a new refresh token)."""
    global SECRETS_CACHE
    SECRETS_CACHE.update(new_data)
    with open(SECRETS_FILE, 'w') as f:
        json.dump(SECRETS_CACHE, f, indent=2)
    print("Secrets file updated successfully.")

# --- Upstox Authentication ---
def get_upstox_access_token():
    """
    Handles OAuth2 flow using local secrets.json file.
    Uses refresh_token if available, otherwise falls back to auth_code.
    Automatically saves the new refresh_token to the file.
    """
    client_id = get_secret("UPSTOX_CLIENT_ID")
    client_secret = get_secret("UPSTOX_CLIENT_SECRET")
    refresh_token = get_secret("UPSTOX_REFRESH_TOKEN")

    url = "https://api.upstox.com/v3/login/authorization/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"}
    
    # Priority 1: Use Refresh Token
    if refresh_token:
        print("Attempting to refresh access token using refresh_token...")
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        }
        try:
            response = requests.post(url, headers=headers, data=data)
            response.raise_for_status()
            token_data = response.json()
            print("Successfully refreshed access token.")
            # If a new refresh token is issued, save it.
            if 'refresh_token' in token_data and token_data['refresh_token'] != refresh_token:
                update_secrets_file({"UPSTOX_REFRESH_TOKEN": token_data['refresh_token']})
            return token_data["access_token"]
        except requests.HTTPError as e:
            print(f"Could not refresh token (it might have expired): {e.response.text}. Falling back to auth_code.")
            
    # Priority 2: Use Auth Code
    auth_code = get_secret("UPSTOX_AUTH_CODE")
    if auth_code:
        print("Attempting to get new token using auth_code...")
        redirect_uri = get_secret("UPSTOX_REDIRECT_URI")
        data = {
            "code": auth_code,
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        }
        try:
            response = requests.post(url, headers=headers, data=data)
            response.raise_for_status()
            token_data = response.json()
            
            print("Successfully obtained new access and refresh tokens.")
            # IMPORTANT: Save the new refresh token for future runs
            update_secrets_file({
                "UPSTOX_REFRESH_TOKEN": token_data['refresh_token'],
                "UPSTOX_AUTH_CODE": "USED - GET A NEW ONE IF REFRESH FAILS" # Invalidate old auth code
            })
            return token_data["access_token"]
        except requests.HTTPError as e:
            print(f"FATAL: Auth code failed: {e.response.text}")
            print("Please generate a new auth_code and update it in credentials/secrets.json")
            raise

    raise ValueError("Could not find a valid UPSTOX_REFRESH_TOKEN or UPSTOX_AUTH_CODE in secrets.json")

# --- Common Utilities (Unchanged) ---
def get_symbol_config():
    """Loads the symbol configuration from name.json."""
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)

def get_utc_now():
    """Returns the current time in UTC."""
    return datetime.now(timezone.utc)

def get_ist_now():
    """Returns the current time in IST (UTC+5:30)."""
    return datetime.now(timezone(timedelta(hours=5, minutes=30)))