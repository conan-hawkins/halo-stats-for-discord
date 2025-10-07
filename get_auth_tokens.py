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

CACHE_FILE = "token_cache.json"
PORT = 8080

# Load the token cache from a file. Handle empty or corrupted files gracefully.
def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r") as f:
                content = f.read().strip()
                if not content:
                    return {}
                return json.loads(content)
        except json.JSONDecodeError:
            print("Cache file is empty or corrupted. Ignoring and starting fresh.")
            return {}
    return {}
 
# Save the token cache to a file
def save_cache(cache):
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)

# Check if a token is valid (not expired)
def is_valid(token_info):
    # token_info: {"token": "...", "expires_at": 1234567890}
    return token_info and token_info.get("expires_at", 0) > time.time()

# Start a local server to capture the authorization code
def get_authorization_code(client_id, redirect_uri):
    auth_params = {
        "client_id": client_id,
        "response_type": "code",
        "redirect_uri": redirect_uri,
        "scope": "Xboxlive.signin Xboxlive.offline_access"
    }
    url = f"https://login.live.com/oauth20_authorize.srf?{urlencode(auth_params)}"
    webbrowser.open(url)

    class OAuthHandler(http.server.SimpleHTTPRequestHandler):
        def do_GET(self):
            nonlocal auth_code
            if "code=" in self.path:
                auth_code = parse_qs(urlparse(self.path).query)["code"][0]
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"OK. Close this window.")
            else:
                self.send_error(404)

    auth_code = None
    with socketserver.TCPServer(("localhost", PORT), OAuthHandler) as httpd:
        httpd.handle_request()
    return auth_code

# Exchange authorization code or refresh token for access and refresh tokens
def exchange_code_for_tokens(client_id, client_secret, redirect_uri, code=None, refresh_token=None):
    url = "https://login.live.com/oauth20_token.srf"
    if code:
        payload = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": client_id,
            "client_secret": client_secret
        }
    else:
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret
        }
    r = requests.post(url, data=payload)
    r.raise_for_status()
    data = r.json()
    return {
        "access_token": data["access_token"],
        "refresh_token": data["refresh_token"],
        "expires_at": time.time() + data.get("expires_in", 3600)
    }

# Request Xbox user token using the access token
def request_user_token(access_token):
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
    # XSTS token expiry is typically 24h
    return {
        "token": data["Token"],
        "expires_at": time.time() + (data.get("NotAfterSeconds", 86400))
    }

# Request XSTS token using the Xbox user token
def request_xsts_token(user_token, use_halo=True):
    url = "https://xsts.auth.xboxlive.com/xsts/authorize"
    
    if use_halo:
        # Halo-specific RP - XSTSv3HaloAudience
        rp = "https://prod.xsts.halowaypoint.com/"
    else:
        # Standard Xbox Live RP - XSTSv3XboxAudience
        rp = "http://xboxlive.com"
    
    headers = {
        "x-xbl-contract-version": "1",
        "Content-Type": "application/json"
    }
    
    # Fix the payload structure to match the documentation exactly
    payload = {
        "Properties": {
            "SandboxId": "RETAIL",
            "UserTokens": [user_token]  # Changed from "User" to "UserTokens"
        },
        "RelyingParty": rp,
        "TokenType": "JWT"
    }
    
    print(f"🔍 XSTS request for RP: {rp}")
    print(f"Payload: {json.dumps(payload, indent=2)}")
    
    try:
        r = requests.post(url, json=payload, headers=headers)
        
        print(f"Response status: {r.status_code}")
        print(f"Response text: {r.text}")
        
        if r.status_code == 400:
            print(f"❌ Bad Request (400) for RelyingParty: {rp}")
            return None
        elif r.status_code == 401:
            print(f"❌ Unauthorized (401) for RelyingParty: {rp}")
            return None
        
        r.raise_for_status()
        data = r.json()
        print(f"✅ XSTS response (RP: {rp}):", json.dumps(data, indent=2))
        
        # Extract XUID/UHS from response
        xuid = None
        uhs = None
        try:
            xui = data["DisplayClaims"]["xui"][0]
            # Look for both xid (XUID) and uhs (User Hash)
            xuid = xui.get("xid")  # Numeric XUID
            uhs = xui.get("uhs")   # User Hash
            
            if xuid:
                print(f"✅ Found XUID: {xuid}")
            elif uhs:
                print(f"✅ Found UHS: {uhs}")
                print("ℹ️ Note: Got UHS instead of XUID. This may work for some endpoints.")
            else:
                print("❌ Neither XUID nor UHS found in XSTS response.")
                
        except Exception as e:
            print("❌ Could not extract XUID/UHS from XSTS response:", e)
        
        return {
            "token": data["Token"],
            "expires_at": time.time() + 86400,  # Default to 24h, will be overridden by NotAfter if present
            "xuid": xuid,
            "uhs": uhs
        }
        
    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP Error for RelyingParty {rp}: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error for RelyingParty {rp}: {e}")
        return None

# Add function to try different XSTS approaches
def try_get_xsts_with_xuid(user_token):
    """Try different XSTS approaches to get XUID"""
    print("🔄 Trying to get XSTS token with XUID...")
    
    # Method 1: Try Xbox Live RP first (more likely to return XUID)
    print("\n🔍 Method 1: Xbox Live RelyingParty")
    result = request_xsts_token(user_token, use_halo=False)
    if result and result.get("xuid"):
        print("✅ Got XUID with Xbox Live RP!")
        return result
    
    # Method 2: Try Halo RP
    print("\n🔍 Method 2: Halo RelyingParty")  
    result = request_xsts_token(user_token, use_halo=True)
    if result and result.get("xuid"):
        print("✅ Got XUID with Halo RP!")
        return result
    elif result and result.get("uhs"):
        print("⚠️ Only got UHS with Halo RP")
        # Keep this result as fallback, but continue trying for XUID
        fallback_result = result
    
    # Method 3: Try without SandboxId
    print("\n🔍 Method 3: Xbox Live RP without SandboxId")
    url = "https://xsts.auth.xboxlive.com/xsts/authorize"
    headers = {
        "x-xbl-contract-version": "1",
        "Content-Type": "application/json"
    }
    payload = {
        "Properties": {
            "UserTokens": [user_token]
            # No SandboxId
        },
        "RelyingParty": "http://xboxlive.com",
        "TokenType": "JWT"
    }
    
    try:
        r = requests.post(url, json=payload, headers=headers)
        if r.status_code == 200:
            data = r.json()
            print("✅ Method 3 response:", json.dumps(data, indent=2))
            
            xuid = data["DisplayClaims"]["xui"][0].get("xid")
            if xuid:
                print("✅ Got XUID without SandboxId!")
                return {
                    "token": data["Token"],
                    "expires_at": time.time() + 86400,
                    "xuid": xuid,
                    "uhs": data["DisplayClaims"]["xui"][0].get("uhs")
                }
        else:
            print(f"Method 3 failed: {r.status_code} - {r.text}")
    except Exception as e:
        print(f"Method 3 exception: {e}")
    
    # If we have a UHS result as fallback, return it
    if 'fallback_result' in locals():
        print("⚠️ Returning UHS result as fallback")
        return fallback_result
    
    print("❌ All XSTS methods failed")
    return None

# Request Spartan token using the XSTS token
async def request_spartan_token(xsts_token):
    HALO_WAYPOINT_USER_AGENT = "HaloWaypoint/6.1.0.0 (Windows10; Xbox; Production)"
    SPARTAN_TOKEN_V4_ENDPOINT = "https://settings.svc.halowaypoint.com/spartan-token"

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
        "User-Agent": HALO_WAYPOINT_USER_AGENT,
        "Content-Type": "application/json"
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            SPARTAN_TOKEN_V4_ENDPOINT,
            json=token_request,
            headers=headers
        ) as response:

            text = await response.text()

            if response.status == 201:
                root = ET.fromstring(text)
                ns = {"ns": "http://schemas.datacontract.org/2004/07/Microsoft.Halo.RegisterClient.Bond"}
                token_elem = root.find("ns:SpartanToken", ns)
                # Try to get expiry from XML, fallback to 24h
                expires_elem = root.find("ns:ExpiresUtc", ns)
                if token_elem is not None:
                    spartan_token = token_elem.text
                    if expires_elem is not None and expires_elem.text:
                        # Parse UTC expiry to timestamp
                        import datetime
                        expires_at = datetime.datetime.strptime(
                            expires_elem.text, "%Y-%m-%dT%H:%M:%S.%fZ"
                        ).timestamp()
                    else:
                        expires_at = time.time() + 86400  # fallback: 24h
                    result = {
                        "token": spartan_token,
                        "expires_at": expires_at
                    }
                    print("✅ Spartan token retrieved (JSON):")
                    print(json.dumps(result, indent=2))
                    print(f"Spartan token expires at: {expires_at} ({time.ctime(expires_at)})")
                    return result
                else:
                    print("❌ Spartan token not found in XML.")
                    return None
            else:
                print(f"❌ Spartan token request failed ({response.status}):")
                print(text)
                return None
            
# Request Clearance token using the Spartan token and XUID
async def request_clearance(spartan_token, xuid):
    print("Requesting Clearance token...")
    print("Using XUID:", xuid)
    if not xuid:
        print("❌ Cannot request clearance: XUID is missing. Make sure your account has played Halo Infinite and is a full Xbox Live account.")
        return None
    
    USER_AGENT = "HaloWaypoint/6.1.0.0 (Windows10; Xbox; Production)"
    ENDPOINT = (
        "https://settings.svc.halowaypoint.com"
        f"/oban/flight-configurations/titles/hi"
        f"/audiences/RETAIL/players/xuid({xuid})"
        "/active"
        "?sandbox=UNUSED&build=210921.22.01.10.1706-0"
    )

    headers = {
        "User-Agent": USER_AGENT,
        "x-343-authorization-spartan": spartan_token,  # Fixed header name
        "Accept": "application/json",
    }

    print(f"🔍 Clearance request to: {ENDPOINT}")
    print(f"Headers: {headers}")

    async with aiohttp.ClientSession() as session:
        async with session.get(ENDPOINT, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                print("✅ Clearance token retrieved:")
                print(json.dumps(data, indent=2))
                return data
            else:
                text = await resp.text()
                print(f"❌ Clearance request failed ({resp.status}):")
                print(f"Response: {text}")
                print(f"Response headers: {dict(resp.headers)}")
                return None
            
# Add function to get both XUID and Halo XSTS token
def get_xuid_and_halo_xsts(user_token):
    """Get XUID from Xbox Live RP and XSTS token from Halo RP"""
    print("🔄 Getting XUID and Halo XSTS token...")
    
    # Step 1: Get XUID from Xbox Live RP
    print("\n🔍 Step 1: Getting XUID from Xbox Live RelyingParty")
    xbox_result = request_xsts_token(user_token, use_halo=False)
    if not xbox_result or not xbox_result.get("xuid"):
        print("❌ Could not get XUID from Xbox Live RP")
        return None
    
    xuid = xbox_result["xuid"]
    print(f"✅ Got XUID: {xuid}")
    
    # Step 2: Get XSTS token from Halo RP (needed for Spartan token)
    print("\n🔍 Step 2: Getting XSTS token from Halo RelyingParty")
    halo_result = request_xsts_token(user_token, use_halo=True)
    if not halo_result:
        print("❌ Could not get XSTS token from Halo RP")
        return None
    
    print(f"✅ Got Halo XSTS token")
    
    # Combine the results: XUID from Xbox RP, token from Halo RP
    return {
        "token": halo_result["token"],  # Halo XSTS token for Spartan request
        "expires_at": halo_result["expires_at"],
        "xuid": xuid,  # XUID from Xbox Live RP for clearance request
        "uhs": halo_result.get("uhs")
    }

# Update the main authentication flow
async def run_auth_flow(client_id, client_secret, use_halo=True):
    redirect_uri = f"http://localhost:{PORT}"
    cache = load_cache()

    # -. Clearance valid?
    if is_valid(cache.get("clearance")):
        return cache["clearance"]["token"]

    # 0. Spartan token valid?
    if is_valid(cache.get("spartan")) and cache.get("xsts", {}).get("xuid"):
        clearance = await request_clearance(cache["spartan"]["token"], cache["xsts"]["xuid"])
        if clearance is not None:
            cache["clearance"] = clearance
            save_cache(cache)
            return clearance.get("FlightConfigurationId") or clearance.get("token")

    # 1. XSTS token valid?
    if is_valid(cache.get("xsts")) and cache.get("xsts", {}).get("xuid"):
        spartan = await request_spartan_token(cache["xsts"]["token"])
        if spartan is not None:
            cache["spartan"] = spartan
            save_cache(cache)
            clearance = await request_clearance(spartan["token"], cache["xsts"]["xuid"])
            if clearance is not None:
                cache["clearance"] = clearance
                save_cache(cache)
                return clearance.get("FlightConfigurationId") or clearance.get("token")

    # 2. Xbox user token valid?
    if is_valid(cache.get("user")):
        xt = get_xuid_and_halo_xsts(cache["user"]["token"])
        
        if xt and xt.get("xuid"):
            cache["xsts"] = xt
            save_cache(cache)
            spartan = await request_spartan_token(xt["token"])
            if spartan is not None:
                cache["spartan"] = spartan
                save_cache(cache)
                clearance = await request_clearance(spartan["token"], xt["xuid"])
                if clearance is not None:
                    cache["clearance"] = clearance
                    save_cache(cache)
                    return clearance.get("FlightConfigurationId") or clearance.get("token")

    # 3. Access token valid?
    if is_valid(cache.get("oauth")):
        oauth = cache["oauth"]
    else:
        # 4. Refresh or full OAuth
        if cache.get("oauth") and cache["oauth"].get("refresh_token"):
            oauth = exchange_code_for_tokens(
                client_id, client_secret, redirect_uri,
                refresh_token=cache["oauth"]["refresh_token"]
            )
        else:
            code = get_authorization_code(client_id, redirect_uri)
            oauth = exchange_code_for_tokens(
                client_id, client_secret, redirect_uri, code=code
            )
        cache["oauth"] = oauth
        save_cache(cache)

    # 5. Get new Xbox user token
    user = request_user_token(oauth["access_token"])
    cache["user"] = user
    save_cache(cache)

    # 6. Get XUID and Halo XSTS token
    xt = get_xuid_and_halo_xsts(user["token"])
    
    if not xt or not xt.get("xuid"):
        print("❌ Could not obtain XUID and Halo XSTS token.")
        print("This usually means:")
        print("  1. Your Microsoft account hasn't played Halo Infinite")
        print("  2. Your account doesn't have proper Xbox Live permissions")
        print("  3. Your account has privacy restrictions")
        print("  4. You need to log into Halo Infinite at least once")
        return None
    
    cache["xsts"] = xt
    save_cache(cache)

    # 7. Get Spartan token (using Halo XSTS token)
    spartan = await request_spartan_token(xt["token"])
    if spartan is None:
        print("❌ Could not obtain Spartan token.")
        return None
    cache["spartan"] = spartan
    save_cache(cache)

    # 8. Get Clearance (using XUID)
    clearance = await request_clearance(spartan["token"], xt["xuid"])
    if clearance is None:
        print("❌ Clearance token request failed. Aborting authentication flow.")
        return None
    cache["clearance"] = clearance
    save_cache(cache)
    return clearance.get("FlightConfigurationId") or clearance.get("token")

if __name__ == "__main__":
    token = asyncio.run(run_auth_flow(
        client_id="9e2d25cc-669b-4977-95dd-0b13a063b898",
        client_secret="Al~8Q~9Rs6fPB7e1pTllyfsgRkXJSSFx8YM_Zab-",
        use_halo=True
    ))