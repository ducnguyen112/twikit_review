from .db_manager import DBManager
from login_playwright import XComLogin
import logging
from typing import Optional, Dict
import asyncio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AccountManager:
    def __init__(self):
        self.db = DBManager()
        self.login_client = XComLogin()

    def initialize(self):
        """Initialize the database connection"""
        self.db.connect()

    async def get_valid_account(self, api_name: str) -> Optional[Dict]:
        """Get a valid account with cookies for the specified API"""
        account = self.db.get_available_account(api_name)
        if not account:
            logger.warning(f"No available accounts for {api_name}")
            return None

        # If account has valid cookies, return it
        if account['ct0'] and account['auth_token']:
            self.db.increment_api_usage(account['id'], api_name)
            return {
                'id': account['id'],
                'ct0': account['ct0'],
                'auth_token': account['auth_token']
            }

        # Otherwise, try to login and get new cookies
        try:
            ct0, auth_token = await self.login_client.login(
                account['email'],
                account['password'],
                account['totp_secret'],
                account['email_url']
            )
            self.db.update_cookies(account['id'], ct0, auth_token)
            self.db.increment_api_usage(account['id'], api_name)
            
            return {
                'id': account['id'],
                'ct0': ct0,
                'auth_token': auth_token
            }
        except Exception as e:
            logger.error(f"Failed to login account {account['email']}: {str(e)}")
            return None

    def handle_account_error(self, account_id: int, error_message: str):
        """Handle API errors and update account status accordingly"""
        if "Rate limit exceeded" in error_message:
            # Just let the rate limit cool down
            logger.warning(f"Rate limit exceeded for account {account_id}")
            return
        
        if any(msg in error_message.lower() for msg in ["suspended", "banned", "restricted"]):
            logger.error(f"Account {account_id} has been banned or restricted")
            self.db.mark_account_banned(account_id)

    def close(self):
        """Close all connections"""
        self.db.close()
