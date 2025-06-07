# Replace the contents of scripts/google_ai_uploader.py

import requests
import json
from upstox_helpers import get_secret
from scripts.ml_processor import process_with_sklearn

def upload_to_google_ai(final_data: list):
    """
    Takes the final list of data, enriches it with ML insights, and sends it.
    This function is designed to be run in a separate thread.
    """
    print("--- Preparing data for Google AI Studio upload ---")
    if not final_data:
        print("No data to upload.")
        return

    # --- Enrich with Scikit-learn predictions ---
    data_with_ml = process_with_sklearn(final_data)
    
    api_endpoint = get_secret("AI_STUDIO_API_ENDPOINT")
    request_body = {"instances": data_with_ml}
    headers = {"Content-Type": "application/json"}
    
    try:
        print(f"Uploading {len(data_with_ml)} records to AI Studio...")
        response = requests.post(api_endpoint, json=request_body, headers=headers)
        response.raise_for_status()
        print(f"Successfully uploaded data to AI Studio. Status: {response.status_code}")
    except Exception as e:
        print(f"Failed to upload data to AI Studio: {e}")