#!/usr/bin/env python3
"""
Quick Start Script for Agricultural Data System

This script helps users quickly set up and run the agricultural data system.

Author: Agricultural Data Team
Date: 2024
"""

import os
import sys
import subprocess
import argparse
from pathlib import Path


def check_python_version():
    """Check if Python version is compatible."""
    if sys.version_info < (3, 8):
        print("ERROR: Python 3.8 or higher is required")
        print(f"Current version: {sys.version}")
        return False
    print(f"SUCCESS: Python version: {sys.version.split()[0]}")
    return True


def install_dependencies():
    """Install required dependencies."""
    print("Installing dependencies...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("SUCCESS: Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Failed to install dependencies: {e}")
        return False


def create_directories():
    """Create necessary directories."""
    print("Creating directories...")
    directories = ["logs", "data", "tests"]
    
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        print(f"SUCCESS: Created directory: {directory}")


def check_config():
    """Check if configuration file exists."""
    config_file = Path("config.yaml")
    if config_file.exists():
        print("SUCCESS: Configuration file found")
        return True
    else:
        print("ERROR: Configuration file not found")
        print("Please ensure config.yaml exists in the project root")
        return False


def run_tests():
    """Run the test suite."""
    print("Running tests...")
    try:
        subprocess.check_call([sys.executable, "-m", "pytest", "tests/", "-v"])
        print("SUCCESS: All tests passed")
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Tests failed: {e}")
        return False


def run_scraper():
    """Run the scraper."""
    print("Running scraper...")
    try:
        subprocess.check_call([sys.executable, "main.py", "--sources", "usda"])
        print("SUCCESS: Scraper completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Scraper failed: {e}")
        return False


def start_api():
    """Start the API server."""
    print("Starting API server...")
    print("API will be available at: http://localhost:8000")
    print("API documentation: http://localhost:8000/docs")
    print("Press Ctrl+C to stop the server")
    
    try:
        subprocess.check_call([sys.executable, "main.py", "--api"])
    except KeyboardInterrupt:
        print("\nSUCCESS: API server stopped")
    except subprocess.CalledProcessError as e:
        print(f"ERROR: API server failed: {e}")
        return False


def show_status():
    """Show system status."""
    print("System Status:")
    try:
        subprocess.check_call([sys.executable, "main.py", "--status"])
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Failed to get status: {e}")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Agricultural Data System Quick Start")
    parser.add_argument('--setup', action='store_true', help='Setup the system')
    parser.add_argument('--test', action='store_true', help='Run tests')
    parser.add_argument('--scrape', action='store_true', help='Run scraper')
    parser.add_argument('--api', action='store_true', help='Start API server')
    parser.add_argument('--status', action='store_true', help='Show system status')
    parser.add_argument('--all', action='store_true', help='Run setup, test, and scrape')
    
    args = parser.parse_args()
    
    print("Agricultural Data System Quick Start")
    print("=" * 50)
    
    # Check Python version
    if not check_python_version():
        sys.exit(1)
    
    # Setup
    if args.setup or args.all:
        print("\nSetting up system...")
        
        if not install_dependencies():
            sys.exit(1)
        
        create_directories()
        
        if not check_config():
            sys.exit(1)
        
        print("SUCCESS: System setup completed")
    
    # Run tests
    if args.test or args.all:
        print("\nRunning tests...")
        if not run_tests():
            sys.exit(1)
    
    # Run scraper
    if args.scrape or args.all:
        print("\nRunning scraper...")
        if not run_scraper():
            sys.exit(1)
    
    # Show status
    if args.status:
        print("\nSystem Status:")
        show_status()
    
    # Start API
    if args.api:
        print("\nStarting API server...")
        start_api()
    
    # Default: show help
    if not any([args.setup, args.test, args.scrape, args.api, args.status, args.all]):
        print("\nUsage Examples:")
        print("  python quick_start.py --setup    # Setup the system")
        print("  python quick_start.py --test     # Run tests")
        print("  python quick_start.py --scrape   # Run scraper")
        print("  python quick_start.py --api       # Start API server")
        print("  python quick_start.py --status    # Show system status")
        print("  python quick_start.py --all       # Run setup, test, and scrape")
        print("\nFor more information, see README_v2.md")


if __name__ == "__main__":
    main()