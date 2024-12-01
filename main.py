from twikit import Client
import logging
import sys
import asyncio
from db.account_manager import AccountManager
from db.db_manager import DBManager
from typing import List, Dict
import json
import time
import random
import traceback

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def prepare_item_for_json(item):
    """Prepare item for JSON serialization by removing non-serializable attributes"""
    item_dict = item.__dict__.copy()
    item_dict.pop('_client', None)  # Remove client reference
    return item_dict

async def get_user_data(client: Client, account_manager: AccountManager, account: dict, user_id: str, api_name: str):
    """Get user's followers, and following"""
    account_id = account['id']
    try:
        # Check if cookies need refresh
        if account_manager.db.check_and_refresh_cookies(account_id):
            logger.info("Refreshing cookies...")
            logger.info(f"Account ID: {account_id}")
            current_account = account_manager.db.get_account_by_id(account_id)
            cookies = await account_manager.login_client.login(
                current_account['email'],
                current_account['password'],
                current_account['totp_secret'],
                current_account['email_url']
            )
            if cookies:
                client.set_cookies(cookies)
                logger.info("Cookies refreshed successfully")
            else:
                logger.error("Failed to refresh cookies")
                return False

        # Check API usage and get data
        if not account_manager.db.check_valid_api_usage(account_id, api_name):
            logger.info("Rate limit reached")
            return False

        if api_name == 'get_user_followers':
            # Get user follower with pagination
            data = await client.get_user_followers(user_id)
            client.get_user_followers_you_know
        elif api_name == 'get_user_following':
            # Get user following with pagination
            data = await client.get_user_following(user_id)
        elif api_name == 'get_user_verified_followers':
            # Get user verified followers with pagination
            data = await client.get_user_verified_followers(user_id)
        else:
            logger.error(f"Unknown API name: {api_name}")
            return False
        client.get_user_tweets()
        time.sleep(random.randrange(1, 3))
        account_manager.db.increment_api_usage(account_id, api_name)
        
        # Process first page
        for item in data:
            item_data = prepare_item_for_json(item)
            if api_name == 'get_user_followers':
                account_manager.db.add_follower(user_id, item.id)
                account_manager.db.add_person(
                    person_id=item.id,
                    source_url=item.url,
                    data=json.dumps(item_data),
                    status_code=200,
                    slug=item.screen_name
                )
            elif api_name == 'get_user_following':
                account_manager.db.add_following(user_id, item.id)
                account_manager.db.add_person(
                    person_id=item.id,
                    source_url=item.url,
                    data=json.dumps(item_data),
                    status_code=200,
                    slug=item.screen_name
                )
            elif api_name == 'get_user_verified_followers':
                account_manager.db.add_verified_follower(user_id, item.id)
                account_manager.db.add_person(
                    person_id=item.id,
                    source_url=item.url,
                    data=json.dumps(item_data),
                    status_code=200,
                    slug=item.screen_name
                )
            else:
                logger.error(f"Unknown API name: {api_name}")
                return False
            logger.info(f"{api_name} ID: {item.id}")

        # Process remaining pages
        while True:
            try:
                if not data.next_cursor:
                    break
                if not account_manager.db.check_valid_api_usage(account_id, api_name):
                    logger.info("Rate limit reached")
                    break
                data = await data.next()
                time.sleep(random.randrange(1, 3))
                account_manager.db.increment_api_usage(account_id, api_name)
                if data:
                    for item in data:
                        item_data = prepare_item_for_json(item)
                        if api_name == 'get_user_followers':
                            account_manager.db.add_follower(user_id, item.id)
                            account_manager.db.add_person(
                                person_id=item.id,
                                source_url=item.url,
                                data=json.dumps(item_data),
                                status_code=200,
                                slug=item.screen_name
                            )
                        elif api_name == 'get_user_following':
                            account_manager.db.add_following(user_id, item.id)
                            account_manager.db.add_person(
                                person_id=item.id,
                                source_url=item.url,
                                data=json.dumps(item_data),
                                status_code=200,
                                slug=item.screen_name
                            )
                        elif api_name == 'get_user_verified_followers':
                            account_manager.db.add_verified_follower(user_id, item.id)
                            account_manager.db.add_person(
                                person_id=item.id,
                                source_url=item.url,
                                data=json.dumps(item_data),
                                status_code=200,
                                slug=item.screen_name
                            )
                        else:
                            logger.error(f"Unknown API name: {api_name}")
                            return False
                        logger.info(f"{api_name} ID: {item.id}")
                else:
                    logger.info(f"No more {api_name} pages")
                    break
            except Exception as e:
                logger.error(f"Error getting next page: {e}")
                break

        # Mark this user as having their follow relationships crawled
        account_manager.db.set_crawled(user_id)
        return True
    except Exception as e:
        logger.error(f"Error getting user data: {str(traceback.format_exc())}")
        return str(e)

async def main():
    try:
        # Initialize the account manager
        account_manager = AccountManager()
        account_manager.initialize()

        # Reset API usage
        await account_manager.db.reset_all_api_usage()

        # Get a valid account
        # for api_name in ['get_user_following', 'get_user_followers']:
        for api_name in ['get_user_verified_followers']:
            account = await account_manager.get_valid_account(api_name)
            if not account:
                logger.error("No valid account found")
                return

            # Initialize client
            client = Client(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0",
            )
            
            client.set_cookies(cookies=dict(
                ct0=account['ct0'],
                auth_token=account['auth_token']
            ))

            logger.info(f"Started processing with account {account['id']}")

            # Get uncrawled users
            uncrawled_users = account_manager.db.get_uncrawled_users()
            if not uncrawled_users:
                logger.error("No uncrawled users found")
                return

            logger.info(f"Found {len(uncrawled_users)} uncrawled users")

            # Process each user
            for user_id in uncrawled_users:
                logger.info(f"Processing user {user_id}")
                
                # Process the user
                result = await get_user_data(
                    client=client, 
                    account_manager=account_manager, 
                    account=account, 
                    user_id=user_id,
                    api_name=api_name)
                if isinstance(result, str):
                    account_manager.handle_account_error(account['id'], result)
                    logger.error(f"Failed to process user {user_id}: {result}")
                else:
                    account_manager.db.set_crawled(user_id)
                    logger.info(f"Successfully processed user {user_id}")

    except Exception as e:
        logger.error(f"Main execution error: {str(e)}")
        # sys.exit(1)
    finally:
        # Clean up
        account_manager.close()

if __name__ == "__main__":
    asyncio.run(main())