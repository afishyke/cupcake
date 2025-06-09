import requests
import json
import os
from datetime import datetime, timedelta
from typing import Dict, Optional

class UpstoxAuthClient:
    """Minimal Upstox client for authentication and token management only"""
    
    def __init__(self, config_file: str = "upstox_config.json"):
        self.config_file = config_file
        self.config = self._load_config()
        self.base_url = "https://api-v2.upstox.com"
        self.session = requests.Session()
        self._setup_session()
    
    def _load_config(self) -> Dict:
        """Load configuration from file"""
        if not os.path.exists(self.config_file):
            raise FileNotFoundError(f"Config file {self.config_file} not found. Please create it first.")
        
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
            self.session.headers['Authorization'] = f"Bearer {self.config['access_token']}"
    
    def is_token_valid(self) -> bool:
        """Check if current token is valid"""
        if not self.config.get('access_token') or not self.config.get('token_expires_at'):
            return False
        
        expires_at = datetime.fromisoformat(self.config['token_expires_at'])
        return datetime.now() < expires_at - timedelta(minutes=5)  # 5 min buffer
    
    def authenticate(self) -> bool:
        """Interactive authentication flow"""
        if self.is_token_valid():
            print("✓ Token is still valid")
            return True
        
        print("🔐 Starting authentication...")
        
        # Generate login URL
        login_url = (f"{self.base_url}/login/authorization/dialog"
                    f"?response_type=code"
                    f"&client_id={self.config['api_key']}"
                    f"&redirect_uri={self.config['redirect_uri']}")
        
        print(f"📱 Visit this URL: {login_url}")
        auth_code = input("🔑 Paste the authorization code: ").strip()
        
        # Exchange code for token
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
            self.config['token_expires_at'] = (datetime.now() + timedelta(hours=23)).isoformat()
            self._save_config()
            self._setup_session()
            print("✅ Authentication successful!")
            return True
        else:
            print(f"❌ Authentication failed: {response.text}")
            return False
    
    def get_valid_token(self) -> Optional[str]:
        """Get a valid access token, refreshing if necessary"""
        if not self.is_token_valid():
            print("🔄 Token expired, re-authenticating...")
            if not self.authenticate():
                return None
        
        return self.config['access_token']
    
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
            return False
        
        try:
            response = self.session.get(f"{self.base_url}/user/profile", headers=headers)
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success':
                    print(f"✅ Connection successful! User: {data['data'].get('user_name', 'Unknown')}")
                    return True
            
            print(f"❌ Connection test failed: {response.text}")
            return False
            
        except Exception as e:
            print(f"❌ Connection test error: {e}")
            return False


def create_config_template():
    """Create a template configuration file"""
    config = {
        "api_key": "your-api-key-here",
        "api_secret": "your-api-secret-here", 
        "redirect_uri": "http://localhost:8000",
        "access_token": "",
        "token_expires_at": ""
    }
    
    with open('upstox_config.json', 'w') as f:
        json.dump(config, f, indent=2)
    
    print("📝 Created upstox_config.json template")
    print("🔧 Please update it with your actual API credentials")


def main():
    """Example usage - authentication only"""
    try:
        # Initialize auth client
        auth_client = UpstoxAuthClient()
        
        # Authenticate and test
        if auth_client.authenticate():
            print("\n🔍 Testing connection...")
            auth_client.test_connection()
            
            print(f"\n🎯 Your valid token: {auth_client.get_valid_token()[:20]}...")
            print("✅ Ready for trading! Use this client for authentication.")
        
    except FileNotFoundError:
        print("❌ Configuration file not found")
        create_config_template()
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()