from db_manager import DBManager
import argparse
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

def add_account(table_name: str, email: str, password: str, totp_secret: str):
    """Add a new Twitter account to the database"""
    db = DBManager()
    try:
        db.connect()
        account_id = db.add_account(table_name, email, password, totp_secret)
        print(f"Successfully added account {email} with ID: {account_id}")
    except Exception as e:
        print(f"Error adding account: {str(e)}")
    finally:
        db.close()

def list_accounts():
    """List all accounts in the database"""
    db = DBManager()
    try:
        db.connect()
        with db.conn.cursor() as cur:
            cur.execute("""
                SELECT a.id, a.email, a.is_active, a.is_banned,
                       COALESCE(c.is_valid, false) as has_valid_cookies,
                       COALESCE(u.request_count, 0) as api_calls
                FROM accounts a
                LEFT JOIN cookies c ON a.id = c.account_id AND c.is_valid = true
                LEFT JOIN api_usage u ON a.id = u.account_id
                ORDER BY a.id
            """)
            accounts = cur.fetchall()
            
            print("\nAccounts in database:")
            print("=" * 80)
            for acc in accounts:
                status = "Active" if acc[2] else "Inactive"
                status += " (BANNED)" if acc[3] else ""
                print(f"ID: {acc[0]}")
                print(f"Email: {acc[1]}")
                print(f"Status: {status}")
                print(f"Has Valid Cookies: {acc[4]}")
                print(f"API Calls: {acc[5]}")
                print("-" * 40)
    finally:
        db.close()

def main():
    parser = argparse.ArgumentParser(description='Manage Twitter accounts in the database')
    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Add account command
    add_parser = subparsers.add_parser('add', help='Add a new account')
    add_parser.add_argument('--table-name', required=True, help='Table name')
    add_parser.add_argument('--email', required=True, help='Account email')
    add_parser.add_argument('--password', required=True, help='Account password')
    add_parser.add_argument('--totp-secret', required=True, help='TOTP secret key')

    # List accounts command
    subparsers.add_parser('list', help='List all accounts')

    args = parser.parse_args()

    if args.command == 'add':
        add_account(args.table_name, args.email, args.password, args.totp_secret)
    elif args.command == 'list':
        list_accounts()
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
