"""
Discord utility functions for Xbox account integration and gamertag resolution
"""

import discord


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


async def get_gamertag_for_member(member, bot):
    """
    Get the best gamertag match for a Discord member
    Priority: Xbox linked account > Discord names
    
    Args:
        member: Discord member object
        bot: Discord bot instance
        
    Returns:
        List of gamertag strings to try, ordered by priority
    """
    # First priority: Check for linked Xbox account
    xbox_gt = await get_xbox_gamertag_from_discord(member, bot)
    if xbox_gt:
        print(f"   Using linked Xbox account: {xbox_gt}")
        return [xbox_gt]
    
    # Fallback to Discord names
    print(f"   No linked Xbox account, trying Discord names...")
    gamertag_attempts = [
        member.name,
        member.display_name,
        member.global_name
    ]
    
    # Remove None values and duplicates
    seen = set()
    unique_attempts = []
    for gt in gamertag_attempts:
        if gt and gt not in seen:
            seen.add(gt)
            unique_attempts.append(gt)
    
    return unique_attempts
