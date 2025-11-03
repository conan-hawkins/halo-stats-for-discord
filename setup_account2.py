"""
Setup script for second Xbox account

This script helps you authenticate a second Xbox account to double your XUID resolution speed.

Usage:
    python setup_account2.py
"""

import asyncio
import os
import json
from get_auth_tokens import run_auth_flow

async def main():
    print("=" * 60)
    print("SECOND ACCOUNT SETUP")
    print("=" * 60)
    print()
    print("This will authenticate a second Xbox account to double your")
    print("XUID resolution speed (from 30 req/min to 60 req/min).")
    print()
    print("IMPORTANT:")
    print("   - Use a DIFFERENT Microsoft account than your first one")
    print("   - The account needs Xbox Live access")
    print("   - Both accounts will share the same rate limits (5 req/10s each)")
    print()
    
    input("Press Enter to continue...")
    print()
    
    # Client credentials
    client_id = "9e2d25cc-669b-4977-95dd-0b13a063b898"
    client_secret = "Al~8Q~9Rs6fPB7e1pTllyfsgRkXJSSFx8YM_Zab-"
    
    # Backup existing account 1 tokens
    if os.path.exists("token_cache.json"):
        print("Backing up Account 1 tokens...")
        with open("token_cache.json", 'r') as f:
            account1_tokens = json.load(f)
    else:
        print("Warning: No Account 1 tokens found (token_cache.json)")
        account1_tokens = None
    
    # Run auth flow for account 2
    print()
    print("Starting authentication for Account 2...")
    print()
    print("CRITICAL STEP:")
    print("   1. A browser will open with your FIRST account still logged in")
    print("   2. You MUST click 'Sign out' or 'Use another account'")
    print("   3. Then sign in with your SECOND Microsoft account")
    print("   4. Make sure you see a DIFFERENT email address when signing in")
    print()
    
    input("Press Enter when ready to open browser...")
    print()
    
    try:
        # Run authentication with forced account selection
        from get_auth_tokens import AuthenticationManager
        manager = AuthenticationManager(client_id, client_secret)
        
        # Clear all cached tokens to force fresh login
        manager.cache.cache = {}
        manager.cache.save()#
        
        
        # Override to force account selection
        original_get_code = manager.oauth.get_authorization_code
        manager.oauth.get_authorization_code = lambda force_account_selection=True: original_get_code(force_account_selection=force_account_selection)
        
        await manager.get_clearance_token()
        
        # Move the new tokens to account2 file
        if os.path.exists("token_cache.json"):
            with open("token_cache.json", 'r') as f:
                account2_tokens = json.load(f)
            
            # Save as account2
            with open("token_cache_account2.json", 'w') as f:
                json.dump(account2_tokens, f, indent=2)
            
            print()
            print("Account 2 tokens saved to: token_cache_account2.json")
            
            # Restore account 1 tokens
            if account1_tokens:
                with open("token_cache.json", 'w') as f:
                    json.dump(account1_tokens, f, indent=2)
                print("Account 1 tokens restored to: token_cache.json")
            
            print()
            print("=" * 60)
            print("SUCCESS! You now have 2 accounts configured")
            print("=" * 60)
            print()
            print("Speed improvement:")
            print("   • Before: ~30 requests/minute (1 account)")
            print("   • After:  ~60 requests/minute (2 accounts)")
            print()
            print("Your bot will automatically use both accounts when resolving XUIDs.")
            print()
            
        else:
            print()
            print("ERROR: Authentication failed - no tokens generated")
            print("   Please try running the script again")
            
    except Exception as e:
        print()
        print(f"ERROR during authentication: {e}")
        print()
        
        # Restore account 1 tokens on error
        if account1_tokens:
            with open("token_cache.json", 'w') as f:
                json.dump(account1_tokens, f, indent=2)
            print("Account 1 tokens restored")
        
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
