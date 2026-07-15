"""
Setup script for additional Xbox accounts

This script helps you authenticate additional Xbox accounts to increase processing speed.

Usage:
    python -m src.auth.setup_account 2    # Setup account 2
    python -m src.auth.setup_account 3    # Setup account 3
    python -m src.auth.setup_account 4    # Setup account 4
    python -m src.auth.setup_account 5    # Setup account 5
"""

import asyncio
import os
import sys
import json
from pathlib import Path

# Add project root to path for imports when running directly
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import get_token_cache_path, TOKEN_CACHE_FILE


async def setup_account(account_num: int):
    """Setup authentication for a specific account number (2-5)"""
    
    if account_num < 2 or account_num > 5:
        print("ERROR: Account number must be between 2 and 5")
        print("Account 1 is set up using: python -m src.auth.tokens")
        return
    
    print("=" * 60)
    print(f"ACCOUNT {account_num} SETUP")
    print("=" * 60)
    print()
    print(f"This will authenticate Xbox account #{account_num}.")
    print()
    print("IMPORTANT:")
    print("   - Use a DIFFERENT Microsoft account than your other accounts")
    print("   - The account needs Xbox Live access")
    print()
    
    input("Press Enter to continue...")
    print()
    
    # Load credentials from environment
    from dotenv import load_dotenv
    load_dotenv()
    
    client_id = os.getenv('client_id')
    client_secret = os.getenv('client_secret')
    
    if not client_id or not client_secret:
        print("ERROR: Missing credentials in .env file")
        print("Please ensure client_id and client_secret are set in .env")
        return
    
    # Get token file paths
    account1_cache_file = str(TOKEN_CACHE_FILE)
    account_n_cache_file = str(get_token_cache_path(account_num))
    
    # Backup existing account 1 tokens
    account1_tokens = None
    if os.path.exists(account1_cache_file):
        print("Backing up Account 1 tokens...")
        with open(account1_cache_file, 'r') as f:
            account1_tokens = json.load(f)
    else:
        print(f"Warning: No Account 1 tokens found ({account1_cache_file})")
        print("Please set up Account 1 first: python -m src.auth.tokens")
        return
    
    # Import auth modules
    from src.auth.tokens import AuthenticationManager, OAuthFlow
    
    try:
        # Show current Account 1 details
        print()
        print(f"Starting authentication for Account {account_num}...")
        print("=" * 60)
        print("🔍 Current Account 1 Details:")
        xuid_1 = account1_tokens.get('xsts', {}).get('xuid', 'Unknown')
        print(f"   XUID: {xuid_1}")
        print("=" * 60)
        print()
        print("⚠️  YOU MUST USE A DIFFERENT ACCOUNT!")
        print(f"   Do NOT sign in with XUID {xuid_1} again!")
        print()
        
        print("⚠️  IMPORTANT: Watch for the browser window!")
        print("   It will open automatically - make sure to:")
        print("   1. Look for the browser window (it might be behind other windows)")
        print("   2. Click 'Use another account' or 'Sign out' if it shows your current account")
        print("   3. Sign in with a DIFFERENT Microsoft account")
        print()
        input("Press Enter when ready to open browser...")
        print()
        
        # Create auth manager
        manager = AuthenticationManager(client_id, client_secret)
        
        # Create OAuth flow with forced account selection
        print("Opening browser for authentication...")
        print()
        print("⏳ Waiting for you to complete sign-in...")
        print("   After signing in, you should see 'OK. Close this window.'")
        print()
        
        oauth_flow = OAuthFlow(client_id, client_secret, port=8080)
        
        try:
            auth_code = oauth_flow.get_authorization_code(
                force_account_selection=True,
                browser_name='chrome',
                incognito=True
            )
        except KeyboardInterrupt:
            print()
            print("Authentication cancelled by user")
            return
        except Exception as e:
            print()
            print(f"ERROR during OAuth flow: {e}")
            return
        
        if not auth_code:
            print()
            print("ERROR: No authorization code received")
            print("   Make sure you completed the sign-in process")
            return
        
        print()
        print("Authorization code received! Getting tokens...")
        
        # Exchange for tokens and get clearance
        oauth_tokens = oauth_flow.exchange_tokens(code=auth_code)
        
        # Clear the cache completely before setting new tokens
        print("Clearing cache to force fresh token generation...")
        manager.cache.cache = {}
        
        manager.cache.set("oauth", oauth_tokens)
        
        # Get clearance token (includes XSTS and Spartan)
        success = await manager.get_clearance_token()
        
        # Move the new tokens to account N file
        if success and os.path.exists(account1_cache_file):
            with open(account1_cache_file, 'r') as f:
                account_n_tokens = json.load(f)
            
            # Get XUIDs
            xuid_1 = account1_tokens.get('xsts', {}).get('xuid', 'Unknown')
            xuid_n = account_n_tokens.get('xsts', {}).get('xuid', 'Unknown')
            
            # Check if it's the same account
            account1_xsts = account1_tokens.get('xsts', {}).get('token', '')
            account_n_xsts = account_n_tokens.get('xsts', {}).get('token', '')
            
            if account1_xsts and account1_xsts == account_n_xsts:
                print()
                print("=" * 60)
                print("⚠️  ERROR: SAME ACCOUNT DETECTED!")
                print("=" * 60)
                print()
                print(f"You signed in with XUID: {xuid_n}")
                print(f"This is the SAME as Account 1 (XUID: {xuid_1})")
                print()
                print(f"The tokens have NOT been saved as account {account_num}.")
                print()
                
                # Restore account 1 tokens
                with open(account1_cache_file, 'w') as f:
                    json.dump(account1_tokens, f, indent=2)
                
                return
            
            # Different account - success!
            print()
            print("=" * 60)
            print("✅ SUCCESS! Different account detected!")
            print("=" * 60)
            print(f"Account 1 XUID: {xuid_1}")
            print(f"Account {account_num} XUID: {xuid_n}")
            print("=" * 60)
            
            # Save as account N
            with open(account_n_cache_file, 'w') as f:
                json.dump(account_n_tokens, f, indent=2)
            
            print()
            print(f"Account {account_num} tokens saved to: {account_n_cache_file}")
            
            # Restore account 1 tokens
            if account1_tokens:
                with open(account1_cache_file, 'w') as f:
                    json.dump(account1_tokens, f, indent=2)
                print(f"Account 1 tokens restored to: {account1_cache_file}")
            
            print()
            print("=" * 60)
            print(f"SUCCESS! Account {account_num} is now configured")
            print("=" * 60)
            print()
            print("Your bot will automatically use all configured accounts.")
            print()
            
        else:
            print()
            print("ERROR: Authentication failed - no tokens generated")
            
    except Exception as e:
        print()
        print(f"ERROR during authentication: {e}")
        print()
        
        # Restore account 1 tokens on error
        if account1_tokens:
            with open(account1_cache_file, 'w') as f:
                json.dump(account1_tokens, f, indent=2)
            print("Account 1 tokens restored")
        
        import traceback
        traceback.print_exc()


async def main():
    """Main entry point - parse command line args"""
    if len(sys.argv) < 2:
        print("Usage: python -m src.auth.setup_account <account_number>")
        print()
        print("Examples:")
        print("  python -m src.auth.setup_account 2   # Setup account 2")
        print("  python -m src.auth.setup_account 3   # Setup account 3")
        print("  python -m src.auth.setup_account 4   # Setup account 4")
        print("  python -m src.auth.setup_account 5   # Setup account 5")
        return
    
    try:
        account_num = int(sys.argv[1])
    except ValueError:
        print(f"ERROR: '{sys.argv[1]}' is not a valid account number")
        return
    
    await setup_account(account_num)


if __name__ == "__main__":
    asyncio.run(main())
