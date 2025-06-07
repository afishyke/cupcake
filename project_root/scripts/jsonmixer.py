"""
jsonmixer.py

— Reads:
     • `outputs/signals/upstox_tech_signals.json`
     • `outputs/signals/upstox_static_meta.json`
— Merges them into one final JSON suitable for an LLM. 
— Saves merged JSON as `outputs/final_output.json`.
"""
import json
from upstox_helpers import get_symbol_config, OUTPUT_DIR

def main():
    print("--- Starting JSON Mixer ---")
    tech_signals_path = OUTPUT_DIR / "signals" / "upstox_tech_signals.json"
    static_meta_path = OUTPUT_DIR / "signals" / "upstox_static_meta.json"

    try:
        with open(tech_signals_path) as f:
            tech_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: {tech_signals_path} not found.")
        tech_data = {}

    try:
        with open(static_meta_path) as f:
            static_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: {static_meta_path} not found.")
        static_data = {}

    config = get_symbol_config()
    all_keys = set(tech_data.keys()) | set(static_data.keys())
    
    final_json = {}
    for key in all_keys:
        final_json[key] = {
            "symbol_name": config['symbol_map'].get(key, 'Unknown'),
            "technical": tech_data.get(key, {"error": "No technical data found"}),
            "static": static_data.get(key, {"error": "No static data found"})
        }

    # Save final output
    output_path = OUTPUT_DIR / "final_output.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(final_json, f, indent=2)

    print(f"Final merged JSON saved to {output_path}")
    print("--- Finished JSON Mixer ---")

if __name__ == "__main__":
    main()