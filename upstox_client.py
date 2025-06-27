import requests
import json
import os
import logging
from datetime import datetime, time, timedelta, timezone
from typing import Dict, Optional

class UpstoxAuthClient:
    """Minimal Upstox client for authentication and token management only"""
    
    def __init__(self,
                 config_file: str = None):
        self.config_file = config_file or os.environ.get('CREDENTIALS_PATH', os.path.join(os.getcwd(), 'credentials.json'))
        self.config = self._load_config()
        self.base_url = "https://api-v2.upstox.com"
        self.session = requests.Session()
        self.logger = self._setup_logger()
        self._setup_session()
    
    def _setup_logger(self):
        """Setup logger for authentication events"""
        log_dir = os.environ.get('LOG_DIR', os.path.join(os.getcwd(), 'logs'))
        os.makedirs(log_dir, exist_ok=True)
        
        # Create logger
        logger = logging.getLogger('upstox_auth')
        logger.setLevel(logging.INFO)
        
        # Remove existing handlers to avoid duplicates
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # Create file handler with IST timezone
        log_file = os.path.join(log_dir, 'upstox_auth.log')
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        
        # Create formatter with IST timestamp
        class ISTFormatter(logging.Formatter):
            def formatTime(self, record, datefmt=None):
                # Convert to IST
                utc_dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
                ist_dt = utc_dt + timedelta(hours=5, minutes=30)
                return ist_dt.strftime('%Y-%m-%d %H:%M:%S IST')
        
        formatter = ISTFormatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        return logger
    
    def _log_event(self, event_type: str, details: str = ""):
        """Log authentication events with IST timestamp"""
        self.logger.info(f"{event_type}: {details}")
    
    def _get_ist_timestamp(self) -> str:
        """Get current timestamp in IST"""
        utc_now = datetime.now(timezone.utc)
        ist_now = utc_now + timedelta(hours=5, minutes=30)
        return ist_now.strftime('%Y-%m-%d %H:%M:%S IST')
    
    def _load_config(self) -> Dict:
        """Load configuration from file"""
        if not os.path.exists(self.config_file):
            raise FileNotFoundError(
                f"Config file {self.config_file} not found. Please create it first."
            )
        with open(self.config_file, 'r') as f:
            return json.load(f)
    
    def _save_config(self):
        """Save configuration to file"""
        with open(self.config_file, 'w') as f:
            json.dump(self.config, f, indent=2)
    
    def _setup_session(self):
        """Setup session with auth headers"""
        self.session.headers.update({
            'Accept': 'application/json',
            'Api-Version': '2.0'
        })
        if self.config.get('access_token'):
            self.session.headers['Authorization'] = (
                f"Bearer {self.config['access_token']}"
            )
    
    def is_token_valid(self) -> bool:
        """Check if current token is valid (with 5 min buffer)"""
        if not self.config.get('access_token') or not self.config.get('token_expires_at'):
            self._log_event("TOKEN_CHECK", "No token or expiry time found - token invalid")
            return False
        
        expires_at = datetime.fromisoformat(self.config['token_expires_at'])
        
        # Ensure expires_at is timezone-aware
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        
        # Get current time in UTC (timezone-aware)
        current_time = datetime.now(timezone.utc)
        
        # Calculate expiry time with buffer (both are now timezone-aware)
        expiry_with_buffer = expires_at - timedelta(minutes=5)
        
        is_valid = current_time < expiry_with_buffer
        
        if is_valid:
            self._log_event("TOKEN_CHECK", f"Token is valid, expires at {expires_at}")
        else:
            self._log_event("TOKEN_CHECK", f"Token expired or expiring soon, expired at {expires_at}")
        
        return is_valid
    
    def authenticate(self) -> bool:
        """Interactive authentication flow"""
        if self.is_token_valid():
            print("✓ Token is still valid")
            self._log_event("AUTH_ATTEMPT", "Token still valid, skipping authentication")
            return True
        
        print("🔐 Starting authentication…")
        self._log_event("AUTH_ATTEMPT", "Starting new authentication flow")
        
        login_url = (
            f"{self.base_url}/login/authorization/dialog"
            f"?response_type=code"
            f"&client_id={self.config['api_key']}"
            f"&redirect_uri={self.config['redirect_uri']}"
        )
        print(f"📱 Visit this URL: {login_url}")
        auth_code = input("🔑 Paste the authorization code: ").strip()
        
        token_data = {
            'code': auth_code,
            'client_id': self.config['api_key'],
            'client_secret': self.config['api_secret'],
            'redirect_uri': self.config['redirect_uri'],
            'grant_type': 'authorization_code'
        }
        response = self.session.post(
            f"{self.base_url}/login/authorization/token",
            data=token_data,
            headers={'Content-Type': 'application/x-www-form-urlencoded'}
        )
        
        if response.status_code == 200:
            token_info = response.json()
            self.config['access_token'] = token_info['access_token']
            
            # ==== FORCE EXPIRY AT NEXT 3:30 AM IST ====
            now_utc = datetime.now(timezone.utc)
            ist_offset = timedelta(hours=5, minutes=30)
            
            # Create IST timezone
            ist_tz = timezone(ist_offset)
            now_ist = now_utc.astimezone(ist_tz)

            # Create timezone-aware datetime for 3:30 AM IST
            today_330_ist = datetime.combine(now_ist.date(), time(3, 30)).replace(tzinfo=ist_tz)
            if now_ist >= today_330_ist:
                target_ist = today_330_ist + timedelta(days=1)
            else:
                target_ist = today_330_ist

            # Convert to UTC for storage
            expires_at_utc = target_ist.astimezone(timezone.utc)
            self.config['token_expires_at'] = expires_at_utc.isoformat()
            # ============================================
            
            self._save_config()
            self._setup_session()
            self._log_event("AUTH_SUCCESS", f"Authentication successful, token expires at {expires_at_utc}")
            print("✅ Authentication successful!")
            return True
        else:
            self._log_event("AUTH_FAILED", f"Authentication failed with status {response.status_code}: {response.text}")
            print(f"❌ Authentication failed: {response.text}")
            return False
    
    def get_valid_token(self) -> Optional[str]:
        """Get a valid access token, refreshing if necessary"""
        if not self.is_token_valid():
            print("🔄 Token expired, re-authenticating…")
            self._log_event("TOKEN_REFRESH", "Token expired, starting re-authentication")
            if not self.authenticate():
                self._log_event("TOKEN_REFRESH_FAILED", "Re-authentication failed")
                return None
            self._log_event("TOKEN_REFRESH_SUCCESS", "Re-authentication successful")
        return self.config.get('access_token')
    
    def get_auth_headers(self) -> Dict[str, str]:
        """Get headers with valid authorization token"""
        token = self.get_valid_token()
        if not token:
            return {}
        return {
            'Accept': 'application/json',
            'Api-Version': '2.0',
            'Authorization': f'Bearer {token}'
        }
    
    def test_connection(self) -> bool:
        """Test if authentication is working by checking profile"""
        headers = self.get_auth_headers()
        if not headers:
            self._log_event("CONNECTION_TEST", "No valid headers available for connection test")
            return False
        try:
            response = self.session.get(
                f"{self.base_url}/user/profile", headers=headers
            )
            if response.status_code == 200 and response.json().get('status') == 'success':
                user_data = response.json()['data']
                user_name = user_data.get('user_name', 'Unknown')
                user_id = user_data.get('user_id', 'Unknown')
                self._log_event("CONNECTION_SUCCESS", f"Connected as User: {user_name} (ID: {user_id})")
                print(f"✅ Connection successful! User: {user_name}")
                return True
            self._log_event("CONNECTION_FAILED", f"Connection test failed with status {response.status_code}: {response.text}")
            print(f"❌ Connection test failed: {response.text}")
            return False
        except Exception as e:
            self._log_event("CONNECTION_ERROR", f"Connection test error: {str(e)}")
            print(f"❌ Connection test error: {e}")
            return False

def create_config_template():
    """Create a template credentials.json file"""
    template = {
        "api_key": "your-api-key-here",
        "api_secret": "your-api-secret-here", 
        "redirect_uri": "http://localhost:8000",
        "access_token": "",
        "token_expires_at": ""
    }
    config_dir = os.environ.get('CONFIG_DIR', os.path.join(os.getcwd(), 'authentication'))
    os.makedirs(config_dir, exist_ok=True)
    config_file = os.path.join(config_dir, "credentials.json")
    
    with open(config_file, 'w') as f:
        json.dump(template, f, indent=2)
    print(f"📝 Created credentials.json template at {config_file}")
    print("🔧 Please update it with your actual API credentials")

def main():
    """Example usage - authentication only"""
    try:
        auth_client = UpstoxAuthClient()
        auth_client._log_event("PROGRAM_START", "Upstox Authentication Client started")
        
        if auth_client.authenticate():
            print("\n🔍 Testing connection…")
            auth_client.test_connection()
            token = auth_client.get_valid_token()
            print(f"\n🎯 Your valid token: {token[:20]}…")
            print("✅ Ready for trading! Use this client for authentication.")
            auth_client._log_event("PROGRAM_SUCCESS", "Authentication and connection test completed successfully")
        else:
            auth_client._log_event("PROGRAM_FAILED", "Authentication failed")
            
    except FileNotFoundError:
        print("❌ Configuration file not found")
        create_config_template()
        # Create a temporary logger for this error
        log_dir = os.environ.get('LOG_DIR', os.path.join(os.getcwd(), 'logs'))
        os.makedirs(log_dir, exist_ok=True)
        logger = logging.getLogger('upstox_auth_error')
        handler = logging.FileHandler(os.path.join(log_dir, 'upstox_auth.log'))
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.info("PROGRAM_ERROR: Configuration file not found, created template")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        # Create a temporary logger for this error
        log_dir = os.environ.get('LOG_DIR', os.path.join(os.getcwd(), 'logs'))
        os.makedirs(log_dir, exist_ok=True)
        logger = logging.getLogger('upstox_auth_error')
        handler = logging.FileHandler(os.path.join(log_dir, 'upstox_auth.log'))
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.info(f"PROGRAM_ERROR: Unexpected error - {str(e)}")

if __name__ == "__main__":
    main()