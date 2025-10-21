#!/usr/bin/env python3
"""
Configuration setup script for SearchSystem
Sets up the required LLM configuration parameters
"""

import os
from typing import Optional

def setup_environment_variables():
    """Set up environment variables for LLM configuration"""
    print("🔧 Setting up LLM configuration...")
    
    # Check if OPENAI_API_KEY is already set
    if os.getenv("OPENAI_API_KEY"):
        print("✅ OPENAI_API_KEY is already configured")
        return True
    
    # Prompt for API key
    print("\n📝 Please provide your OpenAI API configuration:")
    api_key = input("Enter your OpenAI API Key: ").strip()
    
    if not api_key:
        print("❌ No API key provided. Exiting...")
        return False
    
    # Set environment variable for current session
    os.environ["OPENAI_API_KEY"] = api_key
    
    # Optional: Set other parameters
    base_url = input("Enter OpenAI Base URL (press Enter for default 'https://api.openai.com/v1'): ").strip()
    if not base_url:
        base_url = "https://api.openai.com/v1"
    os.environ["OPENAI_BASE_URL"] = base_url
    
    model = input("Enter LLM Model (press Enter for default 'gpt-4o'): ").strip()
    if not model:
        model = "gpt-4o"
    os.environ["LLM_MODEL"] = model
    
    print("\n✅ Configuration set for current session:")
    print(f"   OPENAI_API_KEY: {'*' * (len(api_key) - 4) + api_key[-4:] if len(api_key) > 4 else '***'}")
    print(f"   OPENAI_BASE_URL: {base_url}")
    print(f"   LLM_MODEL: {model}")
    
    return True

def create_env_file():
    """Create a .env file for persistent configuration"""
    print("\n💾 Creating .env file for persistent configuration...")
    
    if os.path.exists(".env"):
        overwrite = input(".env file already exists. Overwrite? (y/N): ").strip().lower()
        if overwrite != 'y':
            print("Skipping .env file creation.")
            return
    
    with open(".env", "w") as f:
        f.write("# SearchSystem Configuration\n")
        f.write("# Copy this file and fill in your actual values\n\n")
        f.write("OPENAI_API_KEY=your_api_key_here\n")
        f.write("OPENAI_BASE_URL=https://api.openai.com/v1\n")
        f.write("LLM_MODEL=gpt-4o\n")
        f.write("\n# Optional: Enable debug logging\n")
        f.write("PIPE_DEBUG=0\n")
    
    print("✅ Created .env file template")
    print("📝 Please edit .env file and add your actual API key")

def load_env_file():
    """Load environment variables from .env file"""
    if not os.path.exists(".env"):
        return False
    
    print("📂 Loading configuration from .env file...")
    
    with open(".env", "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ[key] = value
    
    print("✅ Loaded configuration from .env file")
    return True

def test_configuration():
    """Test if the configuration is working"""
    print("\n🧪 Testing configuration...")
    
    api_key = os.getenv("OPENAI_API_KEY")
    base_url = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
    model = os.getenv("LLM_MODEL", "gpt-4o")
    
    if not api_key:
        print("❌ OPENAI_API_KEY not found")
        return False
    
    print(f"✅ Configuration looks good:")
    print(f"   API Key: {'*' * (len(api_key) - 4) + api_key[-4:] if len(api_key) > 4 else '***'}")
    print(f"   Base URL: {base_url}")
    print(f"   Model: {model}")
    
    return True

def main():
    """Main configuration setup"""
    print("🚀 SearchSystem Configuration Setup")
    print("=" * 50)
    
    # Try to load from .env file first
    if load_env_file():
        if test_configuration():
            print("\n✅ Configuration is ready!")
            return
    
    # If .env doesn't work, set up interactively
    if setup_environment_variables():
        # Ask if they want to save to .env file
        save_env = input("\n💾 Save configuration to .env file? (Y/n): ").strip().lower()
        if save_env != 'n':
            create_env_file()
    
    # Test the configuration
    if test_configuration():
        print("\n🎉 Setup complete! You can now use the SearchSystem.")
    else:
        print("\n❌ Setup incomplete. Please check your configuration.")

if __name__ == "__main__":
    main()
