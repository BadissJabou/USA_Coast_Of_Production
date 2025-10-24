#!/usr/bin/env python3
"""
Example Usage of Agricultural Data System

This script demonstrates how to use the modernized agricultural data system.

Author: Agricultural Data Team
Date: 2024
"""

import asyncio
import pandas as pd
from scripts.usda_modernized import USDAScraper
from scripts.data_validator import DataValidator
from scripts.database_manager import AgriculturalDatabase
from main import AgriculturalDataPipeline


def example_individual_scraper():
    """Example of using an individual scraper."""
    print("🔍 Example: Individual Scraper Usage")
    print("-" * 40)
    
    # Initialize scraper
    scraper = USDAScraper("config.yaml")
    
    # Run scraper
    data = scraper.run()
    
    if data is not None:
        print(f"✅ Scraped {len(data)} records")
        print(f"Commodities: {data['Commodity'].unique()}")
        print(f"Years: {sorted(data['Year'].unique())}")
        print(f"Sample data:")
        print(data.head())
    else:
        print("❌ Scraping failed")


def example_data_validation():
    """Example of data validation."""
    print("\n🔍 Example: Data Validation")
    print("-" * 40)
    
    # Create sample data
    sample_data = pd.DataFrame({
        'Item': ['Fertilizer', 'Seed', 'Yield', 'Invalid Item'],
        'Value': [150.0, 80.0, 180.0, 'not_numeric'],
        'Unit': ['$/acre', '$/acre', 'bu/acre', 'invalid_unit'],
        'Commodity': ['Corn', 'Corn', 'Corn', 'InvalidCrop'],
        'Location': ['Iowa', 'Iowa', 'Iowa', 'Iowa'],
        'Year': ['2023/2024', '2023/2024', '2023/2024', '2023'],
        'Source': ['USDA', 'USDA', 'USDA', 'USDA']
    })
    
    # Validate data
    validator = DataValidator("config.yaml")
    result = validator.validate_data(sample_data)
    
    print(f"✅ Validation completed")
    print(f"Valid: {result.is_valid}")
    print(f"Errors: {result.errors}")
    print(f"Warnings: {result.warnings}")
    print(f"Quality Score: {validator.get_data_quality_score(sample_data):.1f}/100")
    
    if result.cleaned_data is not None:
        print(f"Cleaned data: {len(result.cleaned_data)} records")


def example_database_operations():
    """Example of database operations."""
    print("\n🔍 Example: Database Operations")
    print("-" * 40)
    
    # Initialize database
    db = AgriculturalDatabase("data/example.db")
    
    # Create sample data
    sample_data = pd.DataFrame({
        'Item': ['Fertilizer', 'Seed', 'Yield'],
        'Value': [150.0, 80.0, 180.0],
        'Unit': ['$/acre', '$/acre', 'bu/acre'],
        'Commodity': ['Corn', 'Corn', 'Corn'],
        'Location': ['Iowa', 'Iowa', 'Iowa'],
        'Year': ['2023/2024', '2023/2024', '2023/2024'],
        'Source': ['USDA', 'USDA', 'USDA']
    })
    
    # Insert data
    success = db.insert_raw_data(sample_data, 'USDA')
    print(f"✅ Data insertion: {'Success' if success else 'Failed'}")
    
    # Query data
    results = db.query_data(commodity='Corn', location='Iowa')
    print(f"✅ Query results: {len(results)} records")
    
    # Get summary
    summary = db.get_data_summary()
    print(f"✅ Database summary: {summary['total_raw_records']} total records")


def example_pipeline_usage():
    """Example of pipeline usage."""
    print("\n🔍 Example: Pipeline Usage")
    print("-" * 40)
    
    # Initialize pipeline
    pipeline = AgriculturalDataPipeline("config.yaml")
    
    # Get status
    status = pipeline.get_pipeline_status()
    print(f"✅ Pipeline status: {status['status']}")
    print(f"Available scrapers: {status['scrapers_available']}")
    
    # Run scraping (commented out to avoid actual scraping in example)
    # results = await pipeline.run_scraping_pipeline(['usda'])
    # print(f"✅ Pipeline results: {results['success_count']} sources processed")


def example_api_usage():
    """Example of API usage (simulated)."""
    print("\n🔍 Example: API Usage")
    print("-" * 40)
    
    print("API endpoints available:")
    print("  GET /health - Health check")
    print("  GET /data - Get agricultural data")
    print("  GET /summary - Get database summary")
    print("  GET /export - Export data")
    print("  POST /data/validate - Validate data")
    
    print("\nExample API calls:")
    print("  curl 'http://localhost:8000/data?commodity=Corn&location=Iowa'")
    print("  curl 'http://localhost:8000/summary'")
    print("  curl 'http://localhost:8000/export?format=excel' -o data.xlsx")


def example_configuration():
    """Example of configuration usage."""
    print("\n🔍 Example: Configuration")
    print("-" * 40)
    
    import yaml
    
    # Load configuration
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    
    print("Configuration sections:")
    for section in config.keys():
        print(f"  - {section}")
    
    print(f"\nUSDA scraper enabled: {config['states']['usda']['enabled']}")
    print(f"Logging level: {config['logging']['level']}")
    print(f"Request timeout: {config['requests']['timeout']} seconds")


async def main():
    """Main example function."""
    print("🌾 Agricultural Data System - Usage Examples")
    print("=" * 60)
    
    try:
        # Run examples
        example_individual_scraper()
        example_data_validation()
        example_database_operations()
        example_pipeline_usage()
        example_api_usage()
        example_configuration()
        
        print("\n✅ All examples completed successfully!")
        print("\n📚 For more information:")
        print("  - README_v2.md - Complete documentation")
        print("  - http://localhost:8000/docs - API documentation")
        print("  - python quick_start.py --help - Quick start guide")
        
    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        print("Make sure you have:")
        print("  1. Installed dependencies: pip install -r requirements.txt")
        print("  2. Created config.yaml file")
        print("  3. Set up the system: python quick_start.py --setup")


if __name__ == "__main__":
    asyncio.run(main())
