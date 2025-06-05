# aunthenticator.py
import json
import os
import pandas as pd
import subprocess
import sys

def find_and_fetch_new_symbols():
    """
    Compares SYMBOLS.json with the CSV database, finds new symbols,
    and calls yffetch.py to update the database.
    Returns True if an update was run, False otherwise.
    """
    print("\n--- [Stage 1: Checking for New Symbols] ---")
    
    # 1. Load symbols from CSV
    csv_filename = "indian_stocks_data.csv"
    try:
        df = pd.read_csv(csv_filename, usecols=["symbol"], dtype=str)
        csv_symbols = set(df["symbol"].str.strip())
    except FileNotFoundError:
        print(f"Info: '{csv_filename}' not found. Will create a new one.")
        csv_symbols = set() # Empty set if file doesn't exist
    except ValueError:
        print(f"Warning: 'symbol' column not found in '{csv_filename}'. Assuming no existing symbols.")
        csv_symbols = set()
    except Exception as e:
        print(f"An error occurred reading {csv_filename}: {e}")
        return False

    # 2. Load symbols from primary JSON (SYMBOLS.json)
    json_filename = "SYMBOLS.json"
    try:
        with open(json_filename, "r", encoding="utf-8") as jf:
            data = json.load(jf)
            json_symbols_list = data.get("symbols", [])
        json_symbols = set(s.strip() for s in json_symbols_list)
    except Exception as e:
        print(f"Fatal Error: Could not read master symbol list '{json_filename}': {e}")
        return False # This is a fatal error, can't proceed.

    # 3. Find new symbols
    new_symbols = json_symbols - csv_symbols

    if not new_symbols:
        print("✓ No new symbols found. Fundamental database is up to date.")
        return False
    else:
        print(f"i Found {len(new_symbols)} new symbols to process: {new_symbols}")

        # 4. Write new symbols to a temporary file for yffetch
        temp_fetch_file = "json.json" # As yffetch expects this filename
        output = {"stocks": sorted(list(new_symbols))}
        try:
            with open(temp_fetch_file, "w", encoding="utf-8") as out_f:
                json.dump(output, out_f, indent=2)
            print(f"✓ Wrote new symbols to '{temp_fetch_file}'.")
        except Exception as e:
            print(f"Error writing to '{temp_fetch_file}': {e}")
            return False

        # 5. Invoke yffetch.py
        command = [sys.executable, "yffetch.py"]
        print(f"--> Invoking yffetch.py to update fundamental data...")
        try:
            # We run yffetch and let it print its own output directly
            result = subprocess.run(command, check=True)
            print("✓ yffetch.py completed successfully.")
            return True
        except subprocess.CalledProcessError as e:
            print(f"✗ Error: yffetch.py failed with exit code {e.returncode}.")
            return False
        except FileNotFoundError:
            print("✗ Error: Could not find yffetch.py to run.")
            return False

if __name__ == "__main__":
    # You can still run this file by itself to test Stage 1
    find_and_fetch_new_symbols()