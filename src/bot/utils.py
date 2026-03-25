"""
Discord utility functions for Xbox account integration.
"""


async def get_xbox_gamertag_from_discord(member, bot):
    """
    Try to get Xbox gamertag from Discord user's linked accounts using REST API
    
    Args:
        member: Discord member object
        bot: Discord bot instance (needed for auth token)
        
    Returns:
        Xbox gamertag string if found, None otherwise
    """
    try:
        print(f"   Checking Xbox linked account for {member.name}...")
        
        url = f"https://discord.com/api/v10/users/{member.id}/profile"
        headers = {"Authorization": f"Bot {bot.http.token}"}
        
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'connected_accounts' in data:
                        print(f"   Found {len(data['connected_accounts'])} connected account(s)")
                        for account in data['connected_accounts']:
                            account_type = account.get('type', '')
                            account_name = account.get('name', '')
                            print(f"      - {account_type}: {account_name}")
                            if account_type == 'xbox':
                                print(f"   Found linked Xbox account: {account_name}")
                                return account_name
                        print(f"   No Xbox account in connected accounts")
                    else:
                        print(f"   No connected_accounts in API response")
                elif response.status == 403:
                    print(f"   User has connections set to private or bot lacks permissions")
                elif response.status == 404:
                    print(f"   User not found")
                else:
                    print(f"   API returned status {response.status}")
    except Exception as e:
        print(f"   Error checking Xbox connection: {e}")
    
    return None


__all__ = [
    "get_xbox_gamertag_from_discord",
]
