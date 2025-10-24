#!/usr/bin/env python3
"""
Main Orchestration Script for Agricultural Data Pipeline

This script orchestrates the entire agricultural data collection pipeline,
including scraping, validation, database storage, and API serving.

Author: Agricultural Data Team
Date: 2024
"""

import asyncio
import logging
import argparse
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
import yaml
from datetime import datetime

# Import our modules
from scripts.base_scraper import BaseScraper
from scripts.usda_modernized import USDAScraper
from scripts.data_validator import DataValidator
from scripts.database_manager import AgriculturalDatabase


class AgriculturalDataPipeline:
    """
    Main pipeline orchestrator for agricultural data collection.
    
    Coordinates:
    - Data scraping from multiple sources
    - Data validation and cleaning
    - Database storage
    - API serving
    """
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize the pipeline.
        
        Args:
            config_path (str): Path to configuration file
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.setup_logging()
        
        # Initialize components
        self.logger = logging.getLogger(__name__)
        self.validator = DataValidator(config_path)
        self.database = AgriculturalDatabase(config_path=config_path)
        self.scrapers = self._initialize_scrapers()
        
        self.logger.info("Agricultural Data Pipeline initialized")
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            print(f"Config file {self.config_path} not found")
            sys.exit(1)
    
    def setup_logging(self):
        """Setup logging configuration."""
        log_config = self.config.get('logging', {})
        
        # Ensure logs directory exists
        log_file = log_config.get('file', 'logs/pipeline.log')
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            level=getattr(logging, log_config.get('level', 'INFO')),
            format=log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
    
    def _initialize_scrapers(self) -> Dict[str, BaseScraper]:
        """Initialize all configured scrapers."""
        scrapers = {}
        
        # Initialize USDA scraper
        if self.config.get('states', {}).get('usda', {}).get('enabled', True):
            scrapers['usda'] = USDAScraper(self.config_path)
        
        # TODO: Initialize other scrapers as they are modernized
        # if self.config.get('states', {}).get('iowa', {}).get('enabled', True):
        #     scrapers['iowa'] = IowaScraper(self.config_path)
        
        self.logger.info(f"Initialized {len(scrapers)} scrapers: {list(scrapers.keys())}")
        return scrapers
    
    async def run_scraping_pipeline(self, sources: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run the complete scraping pipeline.
        
        Args:
            sources (Optional[List[str]]): Specific sources to scrape (None for all)
            
        Returns:
            Dict[str, Any]: Pipeline results
        """
        self.logger.info("Starting scraping pipeline")
        
        results = {
            'start_time': datetime.now(),
            'sources_processed': [],
            'total_records': 0,
            'errors': [],
            'success_count': 0
        }
        
        # Determine which sources to process
        sources_to_process = sources or list(self.scrapers.keys())
        
        for source_name in sources_to_process:
            if source_name not in self.scrapers:
                self.logger.warning(f"Scraper {source_name} not available")
                continue
            
            try:
                self.logger.info(f"Processing {source_name}")
                
                # Run scraper
                scraper = self.scrapers[source_name]
                data = scraper.run()
                
                if data is None or data.empty:
                    self.logger.warning(f"No data scraped from {source_name}")
                    continue
                
                # Validate data
                validation_result = self.validator.validate_data(data)
                
                if not validation_result.is_valid:
                    self.logger.error(f"Data validation failed for {source_name}: {validation_result.errors}")
                    results['errors'].append({
                        'source': source_name,
                        'type': 'validation',
                        'errors': validation_result.errors
                    })
                    continue
                
                # Store in database
                metadata = {
                    'data_quality_score': self.validator.get_data_quality_score(data),
                    'validation_errors': validation_result.errors,
                    'validation_warnings': validation_result.warnings,
                    'processing_status': 'completed'
                }
                
                success = self.database.insert_raw_data(data, source_name, metadata)
                
                if success:
                    results['sources_processed'].append(source_name)
                    results['total_records'] += len(data)
                    results['success_count'] += 1
                    self.logger.info(f"Successfully processed {source_name}: {len(data)} records")
                else:
                    self.logger.error(f"Failed to store data from {source_name}")
                    results['errors'].append({
                        'source': source_name,
                        'type': 'storage',
                        'error': 'Database insertion failed'
                    })
                
            except Exception as e:
                self.logger.error(f"Error processing {source_name}: {e}", exc_info=True)
                results['errors'].append({
                    'source': source_name,
                    'type': 'scraping',
                    'error': str(e)
                })
        
        results['end_time'] = datetime.now()
        results['duration'] = (results['end_time'] - results['start_time']).total_seconds()
        
        self.logger.info(f"Pipeline completed: {results['success_count']} sources processed, "
                        f"{results['total_records']} total records, {len(results['errors'])} errors")
        
        return results
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline status."""
        try:
            db_summary = self.database.get_data_summary()
            
            return {
                'status': 'running',
                'database_connected': True,
                'scrapers_available': list(self.scrapers.keys()),
                'database_summary': db_summary,
                'last_update': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Failed to get status: {e}")
            return {
                'status': 'error',
                'database_connected': False,
                'error': str(e),
                'last_update': datetime.now().isoformat()
            }
    
    def cleanup_old_data(self, days_to_keep: int = 30) -> int:
        """Clean up old data."""
        return self.database.cleanup_old_data(days_to_keep)
    
    def export_data(self, output_path: str, format: str = 'excel', 
                   table: str = 'processed_data') -> bool:
        """Export data to file."""
        return self.database.export_data(output_path, table, format)


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description="Agricultural Data Pipeline")
    parser.add_argument('--config', default='config.yaml', help='Configuration file path')
    parser.add_argument('--sources', nargs='+', help='Specific sources to scrape')
    parser.add_argument('--status', action='store_true', help='Show pipeline status')
    parser.add_argument('--cleanup', type=int, help='Clean up data older than N days')
    parser.add_argument('--export', help='Export data to file')
    parser.add_argument('--format', default='excel', choices=['excel', 'csv', 'json'], 
                       help='Export format')
    parser.add_argument('--api', action='store_true', help='Start API server')
    
    args = parser.parse_args()
    
    # Initialize pipeline
    pipeline = AgriculturalDataPipeline(args.config)
    
    if args.status:
        # Show status
        status = pipeline.get_pipeline_status()
        print("Pipeline Status:")
        print(f"  Status: {status['status']}")
        print(f"  Database Connected: {status['database_connected']}")
        print(f"  Available Scrapers: {', '.join(status['scrapers_available'])}")
        
        if 'database_summary' in status:
            summary = status['database_summary']
            print(f"  Total Records: {summary.get('total_raw_records', 0)}")
            print(f"  Unique Sources: {', '.join(summary.get('unique_sources', []))}")
            print(f"  Unique Commodities: {', '.join(summary.get('unique_commodities', []))}")
    
    elif args.cleanup:
        # Cleanup old data
        deleted_count = pipeline.cleanup_old_data(args.cleanup)
        print(f"Cleaned up {deleted_count} old records")
    
    elif args.export:
        # Export data
        success = pipeline.export_data(args.export, args.format)
        if success:
            print(f"Data exported to {args.export}")
        else:
            print("Export failed")
            sys.exit(1)
    
    elif args.api:
        # Start API server
        import uvicorn
        from api.app import app
        print("Starting API server...")
        uvicorn.run(app, host="0.0.0.0", port=8000)
    
    else:
        # Run scraping pipeline
        print("Starting agricultural data pipeline...")
        results = asyncio.run(pipeline.run_scraping_pipeline(args.sources))
        
        print(f"\nPipeline Results:")
        print(f"  Sources Processed: {len(results['sources_processed'])}")
        print(f"  Total Records: {results['total_records']}")
        print(f"  Duration: {results['duration']:.2f} seconds")
        print(f"  Errors: {len(results['errors'])}")
        
        if results['errors']:
            print(f"\nErrors:")
            for error in results['errors']:
                print(f"  {error['source']}: {error['type']} - {error.get('error', error.get('errors', []))}")
        
        if results['sources_processed']:
            print(f"\nSuccessfully processed: {', '.join(results['sources_processed'])}")


if __name__ == "__main__":
    main()
