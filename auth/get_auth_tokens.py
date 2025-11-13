"""
Object-Oriented Authentication Manager for Halo Infinite
Refactored from get_auth_tokens.py to use proper OOP principles
"""

import json
import os
import time
from urllib.parse import urlencode, urlparse, parse_qs
import webbrowser
import http.server
import socketserver
import requests
import aiohttp
import asyncio
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional, Dict, Any


class TokenCache:
    """Manages token cache persistence and validation"""
    
    def __init__(self, cache_file: str = "token_cache.json"):
        self.cache_file = cache_file
        self.cache = self._load()
    
    def _load(self) -> Dict[str, Any]:
        """Load the token cache from file"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r") as f:
                    content = f.read().strip()
                    if not content:
                        return {}
                    return json.loads(content)
            except json.JSONDecodeError:
                print("Cache file is empty or corrupted. Starting fresh.")
                return {}
        return {}
    
    def save(self):
        """Save the current cache to file"""
        with open(self.cache_file, "w") as f:
            json.dump(self.cache, f, indent=2)
    
    def is_valid(self, token_info: Optional[Dict[str, Any]]) -> bool:
        """Check if a token is valid (not expired)"""
        return token_info and token_info.get("expires_at", 0) > time.time()
    
    def get(self, key: str) -> Optional[Dict[str, Any]]:
        """Get a token from cache"""
        return self.cache.get(key)
    
    def set(self, key: str, value: Dict[str, Any]):
        """Set a token in cache and save"""
        self.cache[key] = value
        self.save()
    
    def update(self, data: Dict[str, Any]):
        """Update multiple cache entries at once"""
        self.cache.update(data)
        self.save()


class OAuthFlow:
    """Handles Microsoft OAuth authentication flow"""
    
    def __init__(self, client_id: str, client_secret: str, port: int = 8080):
        self.client_id = client_id
        self.client_secret = client_secret
        self.port = port
        self.redirect_uri = f"http://localhost:{port}"
    
    def get_authorization_code(self, force_account_selection: bool = False, browser_name: Optional[str] = None, incognito: bool = False) -> Optional[str]:
        """Start browser-based OAuth and capture authorization code"""
        auth_params = {
            "client_id": self.client_id,
            "response_type": "code",
            "redirect_uri": self.redirect_uri,
            "scope": "Xboxlive.signin Xboxlive.offline_access"
        }
        
        # Force account selection dialog if requested
        if force_account_selection:
            auth_params["prompt"] = "select_account"
        
        url = f"https://login.live.com/oauth20_authorize.srf?{urlencode(auth_params)}"
        
        # Use specified browser if provided
        if browser_name:
            try:
                # For Chrome on Windows, try common paths
                if browser_name.lower() == 'chrome':
                    import os
                    import subprocess
                    chrome_paths = [
                        'C:/Program Files/Google/Chrome/Application/chrome.exe',
                        'C:/Program Files (x86)/Google/Chrome/Application/chrome.exe',
                        os.path.expanduser('~/AppData/Local/Google/Chrome/Application/chrome.exe')
                    ]
                    for chrome_path in chrome_paths:
                        if os.path.exists(chrome_path):
                            if incognito:
                                # Open Chrome in incognito mode
                                subprocess.Popen([chrome_path, '--incognito', url])
                            else:
                                webbrowser.register('chrome', None, 
                                                  webbrowser.BackgroundBrowser(chrome_path))
                                browser = webbrowser.get('chrome')
                                browser.open(url)
                            break
                    else:
                        print(f"Chrome not found, using default browser")
                        webbrowser.open(url)
                else:
                    browser = webbrowser.get(browser_name)
                    browser.open(url)
            except (webbrowser.Error, Exception) as e:
                print(f"Could not open {browser_name}: {e}, using default browser")
                webbrowser.open(url)
        else:
            webbrowser.open(url)

        class OAuthHandler(http.server.SimpleHTTPRequestHandler):
            def do_GET(handler_self):
                nonlocal auth_code
                if "code=" in handler_self.path:
                    auth_code = parse_qs(urlparse(handler_self.path).query)["code"][0]
                    handler_self.send_response(200)
                    handler_self.end_headers()
                    handler_self.wfile.write(b"OK. Close this window.")
                else:
                    handler_self.send_error(404)
            
            def log_message(self, format, *args):
                pass  # Suppress server logs

        auth_code = None
        with socketserver.TCPServer(("localhost", self.port), OAuthHandler) as httpd:
            httpd.handle_request()
        return auth_code
    
    def exchange_tokens(self, code: Optional[str] = None, refresh_token: Optional[str] = None) -> Dict[str, Any]:
        """Exchange authorization code or refresh token for access tokens"""
        url = "https://login.live.com/oauth20_token.srf"
        if code:
            payload = {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": self.redirect_uri,
                "client_id": self.client_id,
                "client_secret": self.client_secret
            }
        else:
            payload = {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret
            }
        r = requests.post(url, data=payload)
        r.raise_for_status()
        data = r.json()
        return {
            "access_token": data["access_token"],
            "refresh_token": data["refresh_token"],
            "expires_at": time.time() + data.get("expires_in", 3600)
        }


class XboxAuth:
    """Handles Xbox Live authentication"""
    
    @staticmethod
    def request_user_token(access_token: str) -> Dict[str, Any]:
        """Request Xbox user token using Microsoft access token"""
        url = "https://user.auth.xboxlive.com/user/authenticate"
        headers = {
            "x-xbl-contract-version": "1",
            "Content-Type": "application/json"
        }
        payload = {
            "RelyingParty": "http://auth.xboxlive.com",
            "TokenType": "JWT",
            "Properties": {
                "AuthMethod": "RPS",
                "SiteName": "user.auth.xboxlive.com",
                "RpsTicket": f"d={access_token}"
            }
        }
        r = requests.post(url, json=payload, headers=headers)
        r.raise_for_status()
        data = r.json()
        return {
            "token": data["Token"],
            "expires_at": time.time() + (data.get("NotAfterSeconds", 86400))
        }
    
    @staticmethod
    def request_xsts_token(user_token: str, relying_party: str = "http://xboxlive.com") -> Optional[Dict[str, Any]]:
        """Request XSTS token for specific relying party"""
        url = "https://xsts.auth.xboxlive.com/xsts/authorize"
        headers = {
            "x-xbl-contract-version": "1",
            "Content-Type": "application/json"
        }
        payload = {
            "Properties": {
                "SandboxId": "RETAIL",
                "UserTokens": [user_token]
            },
            "RelyingParty": relying_party,
            "TokenType": "JWT"
        }
        
        try:
            r = requests.post(url, json=payload, headers=headers)
            
            if r.status_code not in [200, 201]:
                print(f"XSTS request failed ({r.status_code}) for RP: {relying_party}")
                return None
            
            r.raise_for_status()
            data = r.json()
            
            # Parse expiry time
            expires_at = time.time() + 86400  # Default 24h
            if "NotAfter" in data:
                try:
                    not_after = datetime.fromisoformat(data["NotAfter"].replace('Z', '+00:00'))
                    expires_at = not_after.timestamp()
                except:
                    pass
            
            # Extract XUID and UHS
            xuid = None
            uhs = None
            try:
                xui = data["DisplayClaims"]["xui"][0]
                xuid = xui.get("xid")
                uhs = xui.get("uhs")
            except:
                pass
            
            return {
                "token": data["Token"],
                "expires_at": expires_at,
                "xuid": xuid,
                "uhs": uhs
            }
        except Exception as e:
            print(f"XSTS error for {relying_party}: {e}")
            return None
    
    @staticmethod
    def get_dual_xsts_tokens(user_token: str) -> Optional[Dict[str, Any]]:
        """Get both Xbox Live and Halo XSTS tokens with XUID"""
        # Get Xbox Live XSTS (for profile API and XUID)
        xbox_result = XboxAuth.request_xsts_token(user_token, "http://xboxlive.com")
        if not xbox_result or not xbox_result.get("xuid"):
            print("Could not get XUID from Xbox Live XSTS")
            return None
        
        # Get Halo XSTS (for Spartan token)
        halo_result = XboxAuth.request_xsts_token(user_token, "https://prod.xsts.halowaypoint.com/")
        if not halo_result:
            print("Could not get Halo XSTS token")
            return None
        
        return {
            "token": halo_result["token"],  # Halo XSTS for Spartan
            "expires_at": halo_result["expires_at"],
            "xuid": xbox_result["xuid"],  # XUID from Xbox Live XSTS
            "uhs": xbox_result.get("uhs"),
            "xbox_token": xbox_result["token"],  # Xbox Live XSTS for profile API
            "xbox_expires_at": xbox_result["expires_at"]
        }


class HaloAuth:
    """Handles Halo-specific authentication"""
    
    USER_AGENT = "HaloWaypoint/6.1.0.0 (Windows10; Xbox; Production)"
    
    @staticmethod
    async def request_spartan_token(xsts_token: str) -> Optional[Dict[str, Any]]:
        """Request Spartan token using Halo XSTS token"""
        url = "https://settings.svc.halowaypoint.com/spartan-token"
        token_request = {
            "Audience": "urn:343:s3:services",
            "MinVersion": "4",
            "Proof": [
                {
                    "Token": xsts_token,
                    "TokenType": "Xbox_XSTSv3"
                }
            ]
        }
        headers = {
            "User-Agent": HaloAuth.USER_AGENT,
            "Content-Type": "application/json"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=token_request, headers=headers) as response:
                text = await response.text()
                
                if response.status == 201:
                    try:
                        root = ET.fromstring(text)
                        ns = {"ns": "http://schemas.datacontract.org/2004/07/Microsoft.Halo.RegisterClient.Bond"}
                        token_elem = root.find("ns:SpartanToken", ns)
                        expires_elem = root.find("ns:ExpiresUtc", ns)
                        
                        if token_elem is not None:
                            spartan_token = token_elem.text
                            if expires_elem is not None and expires_elem.text:
                                expires_at = datetime.strptime(
                                    expires_elem.text, "%Y-%m-%dT%H:%M:%S.%fZ"
                                ).timestamp()
                            else:
                                expires_at = time.time() + 86400
                            
                            return {
                                "token": spartan_token,
                                "expires_at": expires_at
                            }
                    except:
                        pass
                
                print(f"Spartan token request failed ({response.status})")
                return None
    
    @staticmethod
    async def request_clearance(spartan_token: str, xuid: str) -> Optional[Dict[str, Any]]:
        """Request Clearance token (optional for some endpoints)"""
        if not xuid:
            print("Cannot request clearance: XUID missing")
            return None
        
        url = (
            "https://settings.svc.halowaypoint.com"
            f"/oban/flight-configurations/titles/hi"
            f"/audiences/RETAIL/players/xuid({xuid})/active"
        )
        headers = {
            "User-Agent": HaloAuth.USER_AGENT,
            "x-343-authorization-spartan": spartan_token,
            "Accept": "application/json",
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    try:
                        data = await resp.json()
                        data["expires_at"] = time.time() + 86400
                        if "FlightConfigurationId" in data and "token" not in data:
                            data["token"] = data["FlightConfigurationId"]
                        return data
                    except:
                        text = await resp.text()
                        if len(text) > 10:
                            return {
                                "token": text.strip(),
                                "FlightConfigurationId": text.strip(),
                                "expires_at": time.time() + 86400
                            }
        
        # Clearance is optional - return placeholder
        print("Clearance token unavailable (not critical)")
        return {
            "FlightConfigurationId": "skip",
            "token": "skip",
            "expires_at": time.time() + 86400
        }


class AuthenticationManager:
    """Main authentication manager orchestrating the full auth flow"""
    
    def __init__(self, client_id: str, client_secret: str, cache_file: str = "token_cache.json"):
        self.cache = TokenCache(cache_file)
        self.oauth = OAuthFlow(client_id, client_secret)
    
    async def get_clearance_token(self) -> Optional[str]:
        """Get clearance token, handling full auth flow if needed"""
        
        # Check cached clearance
        if self.cache.is_valid(self.cache.get("clearance")):
            print("Valid clearance token found in cache")
            return self.cache.get("clearance")["token"]
        
        # Check if we can get clearance from cached Spartan
        if self.cache.is_valid(self.cache.get("spartan")) and self.cache.get("xsts", {}).get("xuid"):
            clearance = await HaloAuth.request_clearance(
                self.cache.get("spartan")["token"],
                self.cache.get("xsts")["xuid"]
            )
            if clearance:
                self.cache.set("clearance", clearance)
                return clearance.get("FlightConfigurationId") or clearance.get("token")
        
        # Check if we can get Spartan from cached XSTS
        if self.cache.is_valid(self.cache.get("xsts")) and self.cache.get("xsts", {}).get("xuid"):
            spartan = await HaloAuth.request_spartan_token(self.cache.get("xsts")["token"])
            if spartan:
                self.cache.set("spartan", spartan)
                clearance = await HaloAuth.request_clearance(spartan["token"], self.cache.get("xsts")["xuid"])
                if clearance:
                    self.cache.set("clearance", clearance)
                    return clearance.get("FlightConfigurationId") or clearance.get("token")
        
        # Check if we can get XSTS from cached user token
        if self.cache.is_valid(self.cache.get("user")):
            xsts = XboxAuth.get_dual_xsts_tokens(self.cache.get("user")["token"])
            if xsts and xsts.get("xuid"):
                self.cache.update({
                    "xsts": xsts,
                    "xsts_xbox": {
                        "token": xsts["xbox_token"],
                        "expires_at": xsts["xbox_expires_at"],
                        "uhs": xsts["uhs"]
                    }
                })
                
                spartan = await HaloAuth.request_spartan_token(xsts["token"])
                if spartan:
                    self.cache.set("spartan", spartan)
                    clearance = await HaloAuth.request_clearance(spartan["token"], xsts["xuid"])
                    if clearance:
                        self.cache.set("clearance", clearance)
                        return clearance.get("FlightConfigurationId") or clearance.get("token")
        
        # Need OAuth tokens
        oauth = await self._get_oauth_tokens()
        if not oauth:
            print("OAuth authentication failed")
            return None
        
        # Get Xbox user token
        print("Requesting Xbox Live user token...")
        user = XboxAuth.request_user_token(oauth["access_token"])
        self.cache.set("user", user)
        
        # Get XUID and XSTS tokens
        print("Requesting XUID and XSTS tokens...")
        xsts = XboxAuth.get_dual_xsts_tokens(user["token"])
        if not xsts or not xsts.get("xuid"):
            print("Could not obtain XUID - account may not have played Halo Infinite")
            return None
        
        self.cache.update({
            "xsts": xsts,
            "xsts_xbox": {
                "token": xsts["xbox_token"],
                "expires_at": xsts["xbox_expires_at"],
                "uhs": xsts["uhs"]
            }
        })
        
        # Get Spartan token
        print("Requesting Spartan token...")
        spartan = await HaloAuth.request_spartan_token(xsts["token"])
        if not spartan:
            print("Could not obtain Spartan token")
            return None
        self.cache.set("spartan", spartan)
        
        # Get Clearance
        print("Requesting Clearance token...")
        clearance = await HaloAuth.request_clearance(spartan["token"], xsts["xuid"])
        if clearance:
            self.cache.set("clearance", clearance)
            return clearance.get("FlightConfigurationId") or clearance.get("token")
        
        return None
    
    async def _get_oauth_tokens(self) -> Optional[Dict[str, Any]]:
        """Get OAuth tokens through refresh or new authorization"""
        oauth_cache = self.cache.get("oauth")
        
        # Try cached OAuth
        if self.cache.is_valid(oauth_cache):
            return oauth_cache
        
        # Try refresh
        if oauth_cache and oauth_cache.get("refresh_token"):
            print("Refreshing OAuth tokens...")
            try:
                oauth = self.oauth.exchange_tokens(refresh_token=oauth_cache["refresh_token"])
                self.cache.set("oauth", oauth)
                return oauth
            except:
                print("Refresh failed, starting new OAuth flow...")
        
        # New OAuth flow
        print("Starting browser-based OAuth login...")
        print("Opening Chrome in Incognito mode for fresh account selection...")
        print("Please log in with your Microsoft account in the browser...")
        print("TIP: If auto-logged in, click 'Use another account' to switch accounts")
        code = self.oauth.get_authorization_code(force_account_selection=True, browser_name='chrome', incognito=True)
        if not code:
            return None
        
        print("Authorization code received!")
        oauth = self.oauth.exchange_tokens(code=code)
        self.cache.set("oauth", oauth)
        return oauth


# Legacy function for backward compatibility
async def run_auth_flow(client_id: str, client_secret: str, use_halo: bool = True, force_account_selection: bool = False) -> Optional[str]:
    """Legacy function wrapper for backward compatibility"""
    manager = AuthenticationManager(client_id, client_secret)
    
    # If forcing account selection, clear OAuth cache to trigger new flow
    if force_account_selection:
        oauth_cache = manager.cache.get("oauth")
        if oauth_cache:
            # Clear the cached tokens to force re-authentication
            manager.cache.cache.pop("oauth", None)
            manager.cache.cache.pop("user", None)
            manager.cache.cache.pop("xsts", None)
            manager.cache.cache.pop("xsts_xbox", None)
            manager.cache.cache.pop("spartan", None)
            manager.cache.cache.pop("clearance", None)
            manager.cache.save()
        # Modify the OAuth flow to force account selection
        manager.oauth.get_authorization_code = lambda: manager.oauth.get_authorization_code.__class__(
            manager.oauth.get_authorization_code.__func__.__code__,
            manager.oauth.get_authorization_code.__globals__
        )(manager.oauth, force_account_selection=True)
    
    return await manager.get_clearance_token()


if __name__ == "__main__":
    # OAuth credentials (environment variables)
    client_id = os.getenv('client_id')
    client_secret = os.getenv('client_secret')
    
    print("=" * 60)
    print("Halo Infinite Authentication Flow (OOP Version)")
    print("=" * 60)
    print()
    
    async def main():
        manager = AuthenticationManager(client_id, client_secret)
        result = await manager.get_clearance_token()
        
        if result:
            print()
            print("=" * 60)
            print("Authentication successful!")
            print(f"Clearance Token: {result}")
            print("=" * 60)
            print()
            print("You can now use the Discord bot to fetch stats.")
        else:
            print()
            print("=" * 60)
            print("Authentication failed!")
            print("=" * 60)
    
    asyncio.run(main())
