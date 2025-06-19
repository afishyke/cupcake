import os
import sys
import subprocess

SCRIPT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'scripts')
PYTHON = sys.executable

SCRIPTS = {
    '1': ('Run historical data fetcher', 'historical_data_fetcher.py'),
    '2': ('Run enhanced live data fetcher', 'enhanced_live_data_fetcher.py'),
    '3': ('Run Data_Fetcher_Runner', 'Data_Fetcher_Runner.py'),
    '4': ('Run websocket client', 'websocket_client.py'),
}

def check_redis():
    try:
        import redis
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.ping()
        print('✅ Redis connection successful!')
    except Exception as e:
        print(f'❌ Redis connection failed: {e}')

def run_script(script_name):
    script_path = os.path.join(SCRIPT_DIR, script_name)
    if not os.path.exists(script_path):
        print(f'❌ Script not found: {script_path}')
        return
    try:
        subprocess.run([PYTHON, script_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f'❌ Script exited with error: {e}')
    except Exception as e:
        print(f'❌ Failed to run script: {e}')

def main():
    while True:
        print('\n==== Project Control Panel ====' )
        for key, (desc, _) in SCRIPTS.items():
            print(f'{key}. {desc}')
        print('5. Check Redis connection')
        print('6. Exit')
        choice = input('Select an option: ').strip()
        if choice in SCRIPTS:
            run_script(SCRIPTS[choice][1])
        elif choice == '5':
            check_redis()
        elif choice == '6':
            print('Exiting control panel.')
            break
        else:
            print('Invalid option. Please try again.')

if __name__ == '__main__':
    main() 