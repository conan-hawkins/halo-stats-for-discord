#!/usr/bin/env python3
"""
Setup script for Halo Discord Bot - API Version
"""

import os
import sys
import subprocess

def check_python_version():
    """Check if Python version is compatible"""
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 9):
        print("Python 3.9+ is required")
        print(f"Current version: {version.major}.{version.minor}.{version.micro}")
        return False
    print(f"Python version {version.major}.{version.minor}.{version.micro} is compatible")
    return True

def install_dependencies():
    """Install required Python packages"""
    print("Installing dependencies...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError:
        print("Failed to install dependencies")
        return False

def create_env_template():
    """Create a template .env file if it doesn't exist"""
    env_file = ".env"
    if not os.path.exists(env_file):
        print("Creating .env template...")
        with open(env_file, "w") as f:
            f.write("# Discord Bot Configuration\n")
            f.write("DISCORD_TOKEN=your_discord_bot_token_here\n")
            f.write("\n")
            f.write("# Optional: Uncomment to override OAuth credentials\n")
            f.write("# HALO_CLIENT_ID=your_client_id\n")
            f.write("# HALO_CLIENT_SECRET=your_client_secret\n")
        print(f"Created {env_file} template")
        print("Please edit .env and add your Discord bot token")
        return False
    else:
        print(f"{env_file} already exists")
        return True

def main():
    print("Halo Discord Bot Setup")
    print("=" * 40)
    
    # Check Python version
    if not check_python_version():
        return 1
    
    # Install dependencies
    if not install_dependencies():
        return 1
    
    # Create .env template
    env_ready = create_env_template()
    
    print("\n" + "=" * 40)
    print("Setup completed!")
    
    if not env_ready:
        print("\nNext steps:")
        print("1. Edit .env file and add your Discord bot token")
        print("2. Start the bot: python bot.py")
    else:
        print("\nReady to go!")
        print("Start the bot: python bot.py")
    
    return 0

if __name__ == "__main__":
    exit(main())