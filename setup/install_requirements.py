import sys
import subprocess

REQUIRED_PACKAGES = [
    'requests',
    'redis',
    'pytz',
    'numpy',
    'numba',
    'protobuf',
    'websocket-client',
]

def check_pip():
    try:
        import pip  # noqa: F401
        return True
    except ImportError:
        return False

def install_packages():
    for package in REQUIRED_PACKAGES:
        print(f'Installing {package}...')
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--upgrade', package])

def main():
    if not check_pip():
        print('pip is not installed. Please install pip and rerun this script.')
        sys.exit(1)
    install_packages()
    print('All required packages are installed.')

if __name__ == '__main__':
    main() 