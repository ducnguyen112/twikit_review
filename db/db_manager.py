import psycopg2
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import json
import os
from psycopg2.sql import Identifier, SQL
from psycopg2.extras import DictCursor
import logging

logger = logging.getLogger(__name__)

class DBManager:
    def __init__(self):
        self.conn = None

    def connect(self):
        """Initialize connection to PostgreSQL database"""
        self.conn = psycopg2.connect(
            user=os.environ.get('DB_USERNAME'),
            password=os.environ.get('DB_PASSWORD'),
            # dbname=os.environ.get('DB_DATABASE'),
            dbname='postgres',
            host=os.environ.get('DB_HOST'),
            port=os.environ.get('DB_PORT'),
        )

        # Create tables if they don't exist
        self.init_database()

    def init_database(self):
        """Initialize database tables"""
        with self.conn.cursor() as cur:
            # Create accounts table
            cur.execute('''
                CREATE TABLE IF NOT EXISTS accounts (
                    id SERIAL PRIMARY KEY,
                    email VARCHAR(255) UNIQUE NOT NULL,
                    password VARCHAR(255) NOT NULL,
                    totp_secret VARCHAR(255) NOT NULL,
                    is_active BOOLEAN DEFAULT true,
                    is_banned BOOLEAN DEFAULT false,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    email_url VARCHAR(255)
                )
            ''')

            # Create cookies table
            cur.execute('''
                CREATE TABLE IF NOT EXISTS cookies (
                    id SERIAL PRIMARY KEY,
                    account_id INTEGER REFERENCES accounts(id),
                    ct0 VARCHAR(255) NOT NULL,
                    auth_token VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP,
                    is_valid BOOLEAN DEFAULT true
                )
            ''')

            # Create api_usage table
            cur.execute('''
                CREATE TABLE IF NOT EXISTS api_usage (
                    id SERIAL PRIMARY KEY,
                    account_id INTEGER REFERENCES accounts(id),
                    api_name VARCHAR(50) NOT NULL,
                    request_count INTEGER DEFAULT 0,
                    is_banned BOOLEAN DEFAULT false,
                    last_reset TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(account_id, api_name)
                )
            ''')

            # Create person table
            cur.execute('''
                CREATE TABLE IF NOT EXISTS person (
                    id VARCHAR(64) NOT NULL PRIMARY KEY,
                    source_url VARCHAR(255),
                    data TEXT,
                    last_crawled TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status_code INTEGER,
                    slug VARCHAR(255),
                    retries INTEGER DEFAULT 0,
                    follow_crawled BOOLEAN DEFAULT false
                )
            ''')

            # Create following table
            cur.execute('''
                CREATE TABLE IF NOT EXISTS following (
                    id SERIAL PRIMARY KEY,
                    person_id VARCHAR(64) REFERENCES person(id),
                    following_id VARCHAR(64),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (person_id, following_id)
                )
            ''')

            # Create follower table
            cur.execute('''
                CREATE TABLE IF NOT EXISTS follower (
                    id SERIAL PRIMARY KEY,
                    person_id VARCHAR(64) REFERENCES person(id),
                    follower_id VARCHAR(64),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (person_id, follower_id)
                )
            ''')

            # Create verified_followers table
            cur.execute('''
                CREATE TABLE IF NOT EXISTS verified_followers (
                    id SERIAL PRIMARY KEY,
                    person_id VARCHAR(64) REFERENCES person(id),
                    follower_id VARCHAR(64),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (person_id, follower_id)
                )
            ''')

            self.conn.commit()

    def add_account(self, table_name: str, email: str, password: str, totp_secret: str) -> int:
        """Add a new account to the database"""
        with self.conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(SQL("INSERT INTO {} (email, password, totp_secret, is_active, is_banned, created_at) VALUES (%s, %s, %s, %s, %s, NOW()) RETURNING id").format(
                Identifier(table_name)), (email, password, totp_secret, True, False))
            account_id = cur.fetchone()['id']
            self.conn.commit()
            return account_id

    def update_cookies(self, account_id: int, ct0: str, auth_token: str):
        """Update cookies for an account"""
        with self.conn.cursor() as cur:
            # Mark old cookies as invalid
            cur.execute(
                '''
                UPDATE cookies SET is_valid = false
                WHERE account_id = %s AND is_valid = true
                ''',
                (account_id,)
            )

            # Add new cookies
            cur.execute(
                '''
                INSERT INTO cookies (account_id, ct0, auth_token, expires_at)
                VALUES (%s, %s, %s, %s)
                ''',
                (account_id, ct0, auth_token,
                 datetime.now() + timedelta(hours=3))  # Assuming 3-hour expiry
            )
            self.conn.commit()

    def get_available_account(self, api_name: str) -> Optional[Dict]:
        """Get an available account that hasn't exceeded rate limits for the specified API"""
        # Get rate limits from the configuration
        rate_limits = {
            'get_user_by_id': 500,
            'get_user_followers': 50,
            'get_user_following': 500,
            'get_user_verified_followers': 500,
        }

        with self.conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                '''
                SELECT a.id, a.email, a.password, a.totp_secret, a.email_url,
                       c.ct0, c.auth_token,
                       COALESCE(u.request_count, 0) as request_count,
                       u.last_reset
                FROM accounts a
                LEFT JOIN cookies c ON a.id = c.account_id AND c.is_valid = true
                LEFT JOIN api_usage u ON a.id = u.account_id AND u.api_name = %s
                WHERE a.is_active = true AND a.is_banned = false
                  AND (u.request_count IS NULL 
                       OR u.request_count < %s 
                       OR u.last_reset < NOW() - INTERVAL '15' MINUTE)
                ORDER BY 
                    u.last_reset ASC,
                    COALESCE(u.request_count, 0) ASC
                LIMIT 1
                ''',
                (api_name, rate_limits[api_name])
            )
            return cur.fetchone()
    def get_account_by_id(self, account_id: int) -> Optional[Dict]:
        with self.conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute('SELECT * FROM accounts WHERE id = %s', (account_id,))
            return cur.fetchone()
    def check_valid_api_usage(self, account_id: int, api_name: str) -> bool:
        """Check if API usage is valid and reset if needed"""
        # First try to reset if 15 minutes have passed
        self.reset_api_usage_if_needed(account_id, api_name)
        
        rate_limits = {
            'get_user_by_id': 500,
            'get_user_followers': 50,
            'get_user_following': 500,
            'get_user_verified_followers': 500,
        }
        
        with self.conn.cursor() as cur:
            cur.execute(
                '''
                SELECT request_count, last_reset, is_banned
                FROM api_usage
                WHERE account_id = %s AND api_name = %s
                ''',
                (account_id, api_name)
            )
            result = cur.fetchone()
            
            if result is None:
                return True
                
            request_count, last_reset, is_banned = result
            
            # Check if account is banned for this API
            if is_banned:
                return False
                
            # Check if we're within rate limits
            return request_count < rate_limits[api_name] - 2

    def reset_api_usage_if_needed(self, account_id: int, api_name: str) -> bool:
        """Reset API usage if 15 minutes have passed since last reset"""
        with self.conn.cursor() as cur:
            cur.execute(
                '''
                UPDATE api_usage 
                SET request_count = 0, last_reset = CURRENT_TIMESTAMP
                WHERE account_id = %s 
                AND api_name = %s 
                AND last_reset < CURRENT_TIMESTAMP - INTERVAL '15 minutes'
                RETURNING id
                ''',
                (account_id, api_name)
            )
            self.conn.commit()
            return cur.fetchone() is not None
        
    def reset_all_api_usage(self):
        with self.conn.cursor() as cur:
            cur.execute(
                '''
                UPDATE api_usage 
                SET request_count = 0, last_reset = CURRENT_TIMESTAMP
                WHERE last_reset < CURRENT_TIMESTAMP - INTERVAL '15' MINUTE
                AND request_count > 0
                AND account_id IN (SELECT id FROM accounts WHERE is_active = true AND is_banned = false)
                '''
            )
            self.conn.commit()

    def check_and_refresh_cookies(self, account_id: int) -> bool:
        """Check if cookies need refresh and return True if they do"""
        with self.conn.cursor() as cur:
            cur.execute(
                '''
                SELECT expires_at 
                FROM cookies 
                WHERE account_id = %s 
                ORDER BY created_at DESC 
                LIMIT 1
                ''',
                (account_id,)
            )
            result = cur.fetchone()
            if not result or result[0] < datetime.now():
                return True
            return False

    def increment_api_usage(self, account_id: int, api_name: str, request_count: int = 1):
        """Increment the API usage counter for an account"""
        with self.conn.cursor() as cur:
            # Reset counter if 15 minutes have passed
            cur.execute(
                '''
                INSERT INTO api_usage (account_id, api_name, request_count, last_reset)
                VALUES (%s, %s, 0, CURRENT_TIMESTAMP)
                ON CONFLICT (account_id, api_name) 
                DO UPDATE SET 
                    request_count = CASE 
                        WHEN api_usage.last_reset < NOW() - INTERVAL '15' MINUTE
                        THEN 0
                        ELSE api_usage.request_count + %s
                    END,
                    last_reset = CASE 
                        WHEN api_usage.last_reset < NOW() - INTERVAL '15' MINUTE
                        THEN CURRENT_TIMESTAMP
                        ELSE api_usage.last_reset
                    END
                ''',
                (account_id, api_name, request_count)
            )
            self.conn.commit()

    def mark_account_banned(self, account_id: int):
        """Mark an account as banned"""
        with self.conn.cursor() as cur:
            cur.execute(
                '''
                UPDATE accounts SET is_banned = true
                WHERE id = %s
                ''',
                (account_id,)
            )
            self.conn.commit()
            
    def mark_api_banned(self, account_id: int, api_name: str):
        """Mark an api as banned"""
        with self.conn.cursor() as cur:
            cur.execute(
                '''
                UPDATE api_usage SET is_banned = true
                WHERE account_id = %s AND api_name = %s
                ''',
                (account_id, api_name)
            )
            self.conn.commit()

    def add_person(self, person_id: str, source_url: str, data: str = None, status_code: int = None, slug: str = None) -> bool:
        """Add or update a person record"""
        with self.conn.cursor() as cur:
            try:
                cur.execute('''
                    INSERT INTO person (id, source_url, data, status_code, slug)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (id) 
                    DO UPDATE SET
                        source_url = EXCLUDED.source_url,
                        data = EXCLUDED.data,
                        status_code = EXCLUDED.status_code,
                        slug = EXCLUDED.slug
                    RETURNING id
                ''', (person_id, source_url, data, status_code, slug))
                self.conn.commit()
                return True
            except Exception as e:
                logger.error(f"Error adding person: {str(e)}")
                self.conn.rollback()
                return False

    def add_following(self, person_id: str, following_id: str) -> bool:
        """Add a following relationship"""
        with self.conn.cursor() as cur:
            try:
                cur.execute('''
                    INSERT INTO following (person_id, following_id)
                    VALUES (%s, %s)
                    ON CONFLICT (person_id, following_id) DO NOTHING
                    RETURNING id
                ''', (person_id, following_id))
                self.conn.commit()
                return True
            except Exception as e:
                logger.error(f"Error adding following: {str(e)}")
                self.conn.rollback()
                return False

    def add_follower(self, person_id: str, follower_id: str) -> bool:
        """Add a follower relationship"""
        with self.conn.cursor() as cur:
            try:
                cur.execute('''
                    INSERT INTO follower (person_id, follower_id)
                    VALUES (%s, %s)
                    ON CONFLICT (person_id, follower_id) DO NOTHING
                    RETURNING id
                ''', (person_id, follower_id))
                self.conn.commit()
                return True
            except Exception as e:
                logger.error(f"Error adding follower: {str(e)}")
                self.conn.rollback()
                return False
    def add_verified_follower(self, person_id: str, follower_id: str) -> bool:
        """Add a verified follower relationship"""
        with self.conn.cursor() as cur:
            try:
                cur.execute('''
                    INSERT INTO verified_followers (person_id, follower_id)
                    VALUES (%s, %s)
                    ON CONFLICT (person_id, follower_id) DO NOTHING
                    RETURNING id
                ''', (person_id, follower_id))
                self.conn.commit()
                return True
            except Exception as e:
                logger.error(f"Error adding verified follower: {str(e)}")
                self.conn.rollback()
                return False

    def get_person(self, person_id: str) -> dict:
        """Get person data by ID"""
        with self.conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute('SELECT * FROM person WHERE id = %s', (person_id,))
            return cur.fetchone()

    def get_following(self, person_id: str) -> list:
        """Get all following IDs for a person"""
        with self.conn.cursor() as cur:
            cur.execute('SELECT following_id FROM following WHERE person_id = %s', (person_id,))
            return [row[0] for row in cur.fetchall()]

    def get_followers(self, person_id: str) -> list:
        """Get all follower IDs for a person"""
        with self.conn.cursor() as cur:
            cur.execute('SELECT follower_id FROM follower WHERE person_id = %s', (person_id,))
            return [row[0] for row in cur.fetchall()]

    def get_uncrawled_users(self, limit: int = 2) -> List[str]:
        """Get user IDs that haven't been crawled yet, ordered by last_crawled null first"""
        with self.conn.cursor() as cur:
            cur.execute('''
                SELECT id 
                FROM person 
                WHERE follow_crawled = false
                ORDER BY 
                    last_crawled IS NULL DESC,
                    last_crawled ASC,
                    retries ASC
                LIMIT %s
            ''', (limit,))
            return [row[0] for row in cur.fetchall()]
    def set_crawled(self, person_id: str):
        with self.conn.cursor() as cur:
            cur.execute('''
                UPDATE person SET follow_crawled = true, retries = retries+1, last_crawled = CURRENT_TIMESTAMP
                WHERE id = %s
            ''', (person_id,))
            self.conn.commit()
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
