"""
Setup script for fifth Xbox account

This script helps you authenticate a fifth Xbox account to further increase processing speed.

Usage:
    python setup_account5.py
"""

import asyncio
import os
import json

async def main():
    print("=" * 60)
    print("FIFTH ACCOUNT SETUP")
    print("=" * 60)
    print()
    print("This will authenticate a fifth Xbox account.")
    print()
    print("IMPORTANT:")
    print("   - Use a DIFFERENT Microsoft account than accounts 1, 2, 3, and 4")
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
    
    # Backup existing account 1 tokens
    if os.path.exists("token_cache.json"):
        print("Backing up Account 1 tokens...")
        with open("token_cache.json", 'r') as f:
            account1_tokens = json.load(f)
    else:
        print("Warning: No Account 1 tokens found (token_cache.json)")
        account1_tokens = None
    
    # Use the same authentication flow as get_auth_tokens.py but with account selection
    from .get_auth_tokens import AuthenticationManager, OAuthFlow
    
    try:
        # Show current Account 1 details
        print()
        print("Starting authentication for Account 5...")
        print("="*60)
        print("🔍 Current Account 1 Details:")
        xuid_1 = account1_tokens.get('xsts', {}).get('xuid', 'Unknown')
        print(f"   XUID: {xuid_1}")
        print("="*60)
        print()
        print("⚠️  YOU MUST USE A DIFFERENT ACCOUNT!")
        print(f"   Do NOT sign in with XUID {xuid_1} again!")
        print()
        
        print("⚠️  IMPORTANT: Watch for the browser window!")
        print("   It will open automatically - make sure to:")
        print("   1. Look for the browser window (it might be behind other windows)")
        print("   2. Click 'Use another account' or 'Sign out' if it shows your current account")
        print("   3. Sign in with a DIFFERENT Microsoft account")
        print(f"      (Different XUID than {xuid_1})")
        print()
        input("Press Enter when ready to open browser...")
        print()
        
        # Create auth manager and force account selection
        manager = AuthenticationManager(client_id, client_secret)
        
        # Manually trigger OAuth flow with account selection forced
        print("Opening Chrome for authentication...")
        print("   (Using Chrome because it's not signed in)")
        print()
        print("⏳ Waiting for you to complete sign-in...")
        print("   (Chrome should open automatically)")
        print("   After signing in, you should see 'OK. Close this window.'")
        print()
        
        # Create OAuth flow with forced account selection
        oauth_flow = OAuthFlow(client_id, client_secret, port=8080)
        
        try:
            # Try to use Chrome specifically
            auth_code = oauth_flow.get_authorization_code(
                force_account_selection=True,
                browser_name='chrome'
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
        
        # CRITICAL: Clear the cache completely before setting new tokens
        # This prevents reusing Account 1's cached XSTS/Spartan tokens
        print("Clearing cache to force fresh token generation...")
        manager.cache.cache = {}  # Clear everything
        
        manager.cache.set("oauth", oauth_tokens)
        
        # Get clearance token (includes XSTS and Spartan) - will be forced to generate new ones
        success = await manager.get_clearance_token()
        
        # Move the new tokens to account5 file
        if success and os.path.exists("token_cache.json"):
            with open("token_cache.json", 'r') as f:
                account5_tokens = json.load(f)
            
            # Get XUIDs
            xuid_1 = account1_tokens.get('xsts', {}).get('xuid', 'Unknown')
            xuid_5 = account5_tokens.get('xsts', {}).get('xuid', 'Unknown')
            
            # Check if it's the same account
            account1_xsts = account1_tokens.get('xsts', {}).get('token', '')
            account5_xsts = account5_tokens.get('xsts', {}).get('token', '')
            
            if account1_xsts and account1_xsts == account5_xsts:
                print()
                print("=" * 60)
                print("⚠️  ERROR: SAME ACCOUNT DETECTED!")
                print("=" * 60)
                print()
                print(f"You signed in with XUID: {xuid_5}")
                print(f"This is the SAME as Account 1 (XUID: {xuid_1})")
                print()
                print("The tokens have NOT been saved as account 5.")
                print()
                
                # Restore account 1 tokens
                with open("token_cache.json", 'w') as f:
                    json.dump(account1_tokens, f, indent=2)
                
                return
            
            # Different account - success!
            print()
            print("=" * 60)
            print("✅ SUCCESS! Different account detected!")
            print("=" * 60)
            print(f"Account 1 XUID: {xuid_1}")
            print(f"Account 5 XUID: {xuid_5}")
            print("=" * 60)
            
            # Save as account5
            with open("token_cache_account5.json", 'w') as f:
                json.dump(account5_tokens, f, indent=2)
            
            print()
            print("Account 5 tokens saved to: token_cache_account5.json")
            
            # Restore account 1 tokens
            if account1_tokens:
                with open("token_cache.json", 'w') as f:
                    json.dump(account1_tokens, f, indent=2)
                print("Account 1 tokens restored to: token_cache.json")
            
            print()
            print("=" * 60)
            print("SUCCESS! You now have 5 accounts configured")
            print("=" * 60)
            
        else:
            print()
            print("ERROR: Authentication failed - no tokens generated")
            
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
