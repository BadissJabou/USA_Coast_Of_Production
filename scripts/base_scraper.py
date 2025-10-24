#!/usr/bin/env python3
"""
Base Scraper Class for Agricultural Data Collection

This module provides a standardized base class for all agricultural data scrapers,
ensuring consistent logging, error handling, and data processing across all scrapers.

Author: Agricultural Data Team
Date: 2024
"""

import logging
import requests
import pandas as pd
from typing import Optional, Dict, Any, List
from abc import ABC, abstractmethod
import time
import random
from pathlib import Path
import yaml


class BaseScraper(ABC):
    """
    Abstract base class for agricultural data scrapers.
    
    Provides common functionality for:
    - Logging configuration
    - HTTP requests with retry logic
    - Data validation
    - Error handling
    - Rate limiting
    """
    
    def __init__(self, state_name: str, config_path: str = "config.yaml"):
        """
        Initialize the base scraper.
        
        Args:
            state_name (str): Name of the state being scraped
            config_path (str): Path to configuration file
        """
        self.state_name = state_name
        self.config_path = config_path
        self.config = self._load_config()
        self.setup_logging()
        self.session = self._create_session()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            logging.warning(f"Config file {self.config_path} not found, using defaults")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration."""
        return {
            'logging': {
                'level': 'INFO',
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                'file': f'scraper_{self.state_name.lower()}.log'
            },
            'requests': {
                'timeout': 15,
                'retries': 3,
                'delay_min': 1,
                'delay_max': 3,
                'user_agents': [
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                ]
            },
            'data': {
                'output_dir': 'data',
                'validate_data': True,
                'required_columns': ['Item', 'Value', 'Unit', 'Commodity', 'Location', 'Year', 'Source']
            }
        }
    
    def setup_logging(self):
        """Setup logging configuration."""
        log_config = self.config.get('logging', {})
        
        logging.basicConfig(
            level=getattr(logging, log_config.get('level', 'INFO')),
            format=log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
            handlers=[
                logging.FileHandler(log_config.get('file', f'scraper_{self.state_name.lower()}.log')),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(f"{self.__class__.__name__}_{self.state_name}")
        self.logger.info(f"Initialized {self.state_name} scraper")
    
    def _create_session(self) -> requests.Session:
        """Create a requests session with proper headers."""
        session = requests.Session()
        session.headers.update({
            'User-Agent': random.choice(self.config['requests']['user_agents']),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })
        return session
    
    def make_request(self, url: str, **kwargs) -> Optional[requests.Response]:
        """
        Make HTTP request with retry logic and rate limiting.
        
        Args:
            url (str): URL to request
            **kwargs: Additional arguments for requests.get
            
        Returns:
            Optional[requests.Response]: Response object or None if failed
        """
        retries = self.config['requests']['retries']
        timeout = self.config['requests']['timeout']
        
        for attempt in range(retries):
            try:
                # Rate limiting
                if attempt > 0:
                    delay = random.uniform(
                        self.config['requests']['delay_min'],
                        self.config['requests']['delay_max']
                    )
                    self.logger.info(f"Waiting {delay:.2f}s before retry {attempt}")
                    time.sleep(delay)
                
                response = self.session.get(url, timeout=timeout, **kwargs)
                response.raise_for_status()
                
                self.logger.info(f"Successfully fetched {url}")
                return response
                
            except requests.RequestException as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt == retries - 1:
                    self.logger.error(f"All {retries} attempts failed for {url}")
                    return None
        
        return None
    
    def validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate scraped data according to schema.
        
        Args:
            df (pd.DataFrame): Data to validate
            
        Returns:
            pd.DataFrame: Validated and cleaned data
        """
        if not self.config['data']['validate_data']:
            return df
            
        required_columns = self.config['data']['required_columns']
        
        # Check required columns
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            self.logger.error(f"Missing required columns: {missing_columns}")
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Remove rows with missing critical data
        initial_rows = len(df)
        df = df.dropna(subset=['Item', 'Value', 'Commodity', 'Year'])
        
        if len(df) < initial_rows:
            self.logger.warning(f"Removed {initial_rows - len(df)} rows with missing critical data")
        
        # Validate data types
        if 'Value' in df.columns:
            df['Value'] = pd.to_numeric(df['Value'], errors='coerce')
            invalid_values = df['Value'].isna().sum()
            if invalid_values > 0:
                self.logger.warning(f"Found {invalid_values} invalid numeric values")
        
        # Validate year format
        if 'Year' in df.columns:
            df = self._validate_year_format(df)
        
        self.logger.info(f"Data validation complete: {len(df)} valid rows")
        return df
    
    def _validate_year_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and standardize year format."""
        def transform_year(year):
            try:
                if isinstance(year, str) and '/' in year:
                    return year  # Already in correct format
                year_int = int(year)
                return f"{year_int}/{year_int + 1}"
            except (ValueError, TypeError):
                self.logger.warning(f"Invalid year format: {year}")
                return None
        
        df['Year'] = df['Year'].apply(transform_year)
        df = df.dropna(subset=['Year'])
        return df
    
    def save_data(self, df: pd.DataFrame, filename: str = None) -> str:
        """
        Save data to file with proper formatting.
        
        Args:
            df (pd.DataFrame): Data to save
            filename (str): Output filename (optional)
            
        Returns:
            str: Path to saved file
        """
        if filename is None:
            filename = f"{self.state_name.lower()}_data.xlsx"
        
        # Ensure output directory exists
        output_dir = Path(self.config['data']['output_dir'])
        output_dir.mkdir(exist_ok=True)
        
        filepath = output_dir / filename
        
        # Save based on file extension
        if filename.endswith('.csv'):
            df.to_csv(filepath, index=False)
        else:
            df.to_excel(filepath, index=False)
        
        self.logger.info(f"Data saved to {filepath}")
        return str(filepath)
    
    def transform_year(self, year) -> str:
        """
        Transform year format from YYYY to YYYY/YYYY+1.
        
        Args:
            year: Year value to transform
            
        Returns:
            str: Transformed year string
        """
        try:
            if isinstance(year, str) and '/' in year:
                return year
            year_int = int(year)
            return f"{year_int}/{year_int + 1}"
        except (ValueError, TypeError):
            self.logger.warning(f"Invalid year format: {year}")
            return str(year)
    
    @abstractmethod
    def scrape_data(self) -> pd.DataFrame:
        """
        Abstract method to scrape data. Must be implemented by subclasses.
        
        Returns:
            pd.DataFrame: Scraped and processed data
        """
        pass
    
    def run(self) -> Optional[pd.DataFrame]:
        """
        Main execution method.
        
        Returns:
            Optional[pd.DataFrame]: Scraped data or None if failed
        """
        try:
            self.logger.info(f"Starting {self.state_name} data scraping")
            
            # Scrape data
            data = self.scrape_data()
            
            if data is None or data.empty:
                self.logger.error("No data scraped")
                return None
            
            # Validate data
            data = self.validate_data(data)
            
            # Save data
            self.save_data(data)
            
            self.logger.info(f"Successfully completed {self.state_name} scraping: {len(data)} rows")
            return data
            
        except Exception as e:
            self.logger.error(f"Scraping failed: {e}", exc_info=True)
            return None
        finally:
            self.session.close()


class DataSchema:
    """Data schema definitions for agricultural data."""
    
    REQUIRED_COLUMNS = [
        'Item',      # Cost/revenue item name
        'Value',     # Numerical value
        'Unit',      # Measurement unit ($/acre, bu/acre, etc.)
        'Commodity', # Crop type (Corn, Soybeans, etc.)
        'Location',  # State/region
        'Year',      # Crop year (YYYY/YYYY+1 format)
        'Source'     # Data source (USDA, University, etc.)
    ]
    
    OPTIONAL_COLUMNS = [
        'Soil_Type',    # Soil quality (Low, Medium, High)
        'Rotation',      # Crop rotation type
        'Tillage',       # Tillage practice
        'Category',      # Cost category (Variable, Fixed, etc.)
        'Notes'          # Additional notes
    ]
    
    DATA_TYPES = {
        'Item': 'string',
        'Value': 'float64',
        'Unit': 'string',
        'Commodity': 'string',
        'Location': 'string',
        'Year': 'string',
        'Source': 'string'
    }
    
    VALIDATION_RULES = {
        'Year': r'^\d{4}/\d{4}$',  # Must be YYYY/YYYY format
        'Commodity': ['Corn', 'Soybeans', 'Wheat', 'Cotton'],  # Valid commodities
        'Unit': [r'\$/acre', r'bu/acre', r'\$/bu', r'\$/ton']  # Valid units
    }
