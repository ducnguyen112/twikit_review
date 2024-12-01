from twikit import Client
import logging
import sys
import asyncio
from db.account_manager import AccountManager
from db.db_manager import DBManager
from typing import List, Dict
import json
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def get_user_data(client: Client, account_manager: AccountManager, user_id: str, account_id: int):
    """Get user's followers, and following"""
    try:
        for api_name in ['get_user_followers', 'get_user_following']:
            if api_name == 'get_user_followers':
                # Get user follower with pagination
                data = await client.get_user_followers(user_id, count=56)
            else:
                # Get user following with pagination
                data = await client.get_user_following(user_id, count=56)
            
            request_count = 1
            
            # Process first page
            for item in data:
                if api_name == 'get_user_followers':
                    account_manager.db.add_follower(user_id, item.id)
                    account_manager.db.add_person(id=item.id, source_url=item.url, data=json.dumps(item.__dict__), status_code=200, slug=item.url.split('/')[-1])
                else:
                    account_manager.db.add_following(user_id, item.id)
                    account_manager.db.add_person(id=item.id, source_url=item.url, data=json.dumps(item.__dict__), status_code=200, slug=item.url.split('/')[-1])
                logger.info(f"{api_name} ID: {item.id}")

            # Process remaining pages
            while True:
                try:
                    if not data.next_cursor:
                        break
                    data = await data.next()
                    request_count += 1
                    if data:
                        for item in data:
                            if api_name == 'get_user_followers':
                                account_manager.db.add_follower(user_id, item.id)
                            else:
                                account_manager.db.add_following(user_id, item.id)
                            logger.info(f"{api_name} ID: {item.id}")
                    else:
                        logger.info(f"No more {api_name} pages")
                        break
                except Exception as e:
                    logger.error(f"Error getting next page: {e}")
                    break

            # Update API usage count
            account_manager.db.increment_api_usage(account_id, api_name, request_count)

        return True
    except Exception as e:
        account_manager.handle_api_error(account_id, api_name, str(e))
        logger.error(f"Error getting user data: {str(e)}")
        return str(e)

async def process_users_with_account(account: Dict, account_manager: AccountManager, user_queue: asyncio.Queue):
    """Process users with a single account"""
    client = Client(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0",
    )
    
    client.set_cookies(cookies=dict(
        ct0=account['ct0'],
        auth_token=account['auth_token']
    ))

    logger.info(f"Started processing with account {account['id']}")

    while True:
        try:
            # Get next user from queue
            user_id = await user_queue.get()
            logger.info(f"Processing user {user_id} with account {account['id']}")
            
            # Process the user
            result = await get_user_data(client, account_manager, user_id, account['id'])
            if isinstance(result, str):
                logger.error(f"Failed to process user {user_id} with account {account['id']}: {result}")
            else:
                logger.info(f"Successfully processed user {user_id} with account {account['id']}")
            
            # Mark task as done
            user_queue.task_done()
            
        except asyncio.CancelledError:
            logger.info(f"Task cancelled for account {account['id']}")
            break
        except Exception as e:
            logger.error(f"Error processing user with account {account['id']}: {str(e)}")
            user_queue.task_done()

async def main():
    try:
        # Initialize the account manager
        account_manager = AccountManager()
        account_manager.initialize()

        # Get all valid accounts
        valid_accounts = []
        for api_name in ['get_user_followers', 'get_user_following']:
            account = await account_manager.get_valid_account(api_name)
            if account and account not in valid_accounts:
                valid_accounts.append(account)

        if not valid_accounts:
            logger.error("No valid accounts found")
            return

        logger.info(f"Found {len(valid_accounts)} valid accounts")

        # Create a queue for users
        user_queue = asyncio.Queue()

        # Get uncrawled users and add them to the queue
        uncrawled_users = account_manager.db.get_uncrawled_users()
        if not uncrawled_users:
            logger.error("No uncrawled users found")
            return

        logger.info(f"Found {len(uncrawled_users)} uncrawled users")

        # Add users to queue
        for user_id in uncrawled_users:
            await user_queue.put(user_id)

        # Create tasks for each account
        tasks = []
        for account in valid_accounts:
            task = asyncio.create_task(
                process_users_with_account(account, account_manager, user_queue)
            )
            tasks.append(task)

        logger.info(f"Started {len(tasks)} processing tasks")

        # Wait for all users to be processed
        await user_queue.join()

        # Cancel all tasks
        for task in tasks:
            task.cancel()
        
        # Wait for all tasks to be cancelled
        await asyncio.gather(*tasks, return_exceptions=True)

        logger.info("All tasks completed")

    except Exception as e:
        logger.error(f"Main execution error: {str(e)}")
        sys.exit(1)
    finally:
        # Clean up
        account_manager.close()

if __name__ == "__main__":
    asyncio.run(main())
