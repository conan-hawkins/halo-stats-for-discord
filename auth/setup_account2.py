"""
Setup script for second Xbox account ISS BOBCAT

This script helps you authenticate a second Xbox account to double your XUID resolution speed.

Usage:
    python setup_account2.py
"""

import asyncio
import os
import json

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
    
    # Run auth flow for account 2
    print()
    print("Starting authentication for Account 2...")
    print()
    print("⚠️ CRITICAL STEPS TO AVOID USING THE SAME ACCOUNT:")
    print()
    print("   The browser will open with a Microsoft login page.")
    print()
    print("   IF IT AUTO-LOGS YOU IN TO YOUR CURRENT ACCOUNT:")
    print("   1. Click your profile picture/name in the top right")
    print("   2. Click 'Sign out' or 'Use a different account'")
    print("   3. Sign in with your SECOND Microsoft account")
    print()
    print("   OR BETTER: Use InPrivate/Incognito mode:")
    print("   1. When browser opens, press Ctrl+Shift+N (Chrome) or Ctrl+Shift+P (Edge)")
    print("   2. Copy the URL from the first window and paste into InPrivate window")
    print("   3. Sign in with your SECOND account")
    print()
    print("   ✓ VERIFY: Make sure you see a DIFFERENT email address!")
    print("   ⚠️  DO NOT use your main account to avoid potential bans!")
    print()
    
    # Use the same authentication flow as get_auth_tokens.py but with account selection
    from .get_auth_tokens import AuthenticationManager
    
    try:
        # Show current Account 1 details
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
        
        from .get_auth_tokens import OAuthFlow
        
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
        
        # Debug: Check what user info we got
        print("DEBUG: OAuth token exchange complete")
        print(f"DEBUG: OAuth tokens keys: {list(oauth_tokens.keys())}")
        
        # CRITICAL: Clear the cache completely before setting new tokens
        # This prevents reusing Account 1's cached XSTS/Spartan tokens
        print("DEBUG: Clearing cache to force fresh token generation...")
        manager.cache.cache = {}  # Clear everything
        
        manager.cache.set("oauth", oauth_tokens)
        
        # Get clearance token (includes XSTS and Spartan) - will be forced to generate new ones
        success = await manager.get_clearance_token()
        
        print(f"DEBUG: get_clearance_token success: {success}")
        
        # Move the new tokens to account2 file
        if success and os.path.exists("token_cache.json"):
            with open("token_cache.json", 'r') as f:
                account2_tokens = json.load(f)
            
            # Debug: Show both XUIDs
            xuid_1 = account1_tokens.get('xsts', {}).get('xuid', 'Unknown')
            xuid_2 = account2_tokens.get('xsts', {}).get('xuid', 'Unknown')
            
            print(f"DEBUG: Account 1 XUID: {xuid_1}")
            print(f"DEBUG: Account 2 XUID: {xuid_2}")
            
            # Check if it's the same account
            account1_xsts = account1_tokens.get('xsts', {}).get('token', '')
            account2_xsts = account2_tokens.get('xsts', {}).get('token', '')
            
            print(f"DEBUG: XSTS tokens are same: {account1_xsts == account2_xsts}")
            print(f"DEBUG: Account 1 XSTS token length: {len(account1_xsts)}")
            print(f"DEBUG: Account 2 XSTS token length: {len(account2_xsts)}")
            print(f"DEBUG: Account 1 XSTS first 50 chars: {account1_xsts[:50]}")
            print(f"DEBUG: Account 2 XSTS first 50 chars: {account2_xsts[:50]}")
            
            if account1_xsts and account1_xsts == account2_xsts:
                print()
                print("=" * 60)
                print("⚠️  ERROR: SAME ACCOUNT DETECTED!")
                print("=" * 60)
                print()
                print(f"You signed in with XUID: {xuid_2}")
                print(f"This is the SAME as Account 1 (XUID: {xuid_1})")
                print()
                print("This won't give you any speed benefits and could risk your account.")
                print()
                print("The tokens have NOT been saved as account 2.")
                print()
                print("To try again:")
                print("   1. Run this script again")
                print("   2. When browser opens, click 'Use another account'")
                print("   3. Sign in with a DIFFERENT Microsoft account")
                print(f"   4. Make sure it has a different XUID than {xuid_1}!")
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
            print(f"Account 2 XUID: {xuid_2}")
            print("=" * 60)
            
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
