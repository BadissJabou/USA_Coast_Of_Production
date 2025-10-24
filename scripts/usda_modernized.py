#!/usr/bin/env python3
"""
USDA Agricultural Data Scraper

This script scrapes agricultural cost and returns data from the USDA Economic Research Service (ERS).
It extracts recent, historical, and forecast data for corn and soybeans.

Author: Agricultural Data Team
Date: 2024
"""

import pandas as pd
from bs4 import BeautifulSoup
from typing import List, Optional, Dict, Any
import logging
from scripts.base_scraper import BaseScraper, DataSchema


class USDAScraper(BaseScraper):
    """
    USDA agricultural data scraper.
    
    Scrapes data from USDA Economic Research Service including:
    - Recent corn and soybeans data
    - Historical data (1975-1996)
    - Forecast data
    """
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize USDA scraper."""
        super().__init__("USDA", config_path)
        self.base_url = "https://www.ers.usda.gov"
        self.main_url = "https://www.ers.usda.gov/data-products/commodity-costs-and-returns/"
        
    def scrape_data(self) -> Optional[pd.DataFrame]:
        """
        Main scraping method.
        
        Returns:
            Optional[pd.DataFrame]: Combined scraped data
        """
        try:
            self.logger.info("Starting USDA data scraping")
            
            # Scrape different data types
            recent_data = self._scrape_recent_data()
            historical_data = self._scrape_historical_data()
            forecast_data = self._scrape_forecast_data()
            
            # Combine all data
            all_data = []
            if recent_data is not None and not recent_data.empty:
                all_data.append(recent_data)
            if historical_data is not None and not historical_data.empty:
                all_data.append(historical_data)
            if forecast_data is not None and not forecast_data.empty:
                all_data.append(forecast_data)
            
            if not all_data:
                self.logger.error("No data scraped from any source")
                return None
            
            # Combine and standardize
            combined_data = pd.concat(all_data, ignore_index=True)
            combined_data = self._standardize_data(combined_data)
            
            self.logger.info(f"Successfully scraped {len(combined_data)} records")
            return combined_data
            
        except Exception as e:
            self.logger.error(f"Scraping failed: {e}", exc_info=True)
            return None
    
    def _scrape_recent_data(self) -> Optional[pd.DataFrame]:
        """Scrape recent corn and soybeans data."""
        try:
            self.logger.info("Scraping recent data")
            
            # Get page content
            response = self.make_request(self.main_url)
            if response is None:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find download links
            links = self._find_download_links(soup, ['corn', 'soybeans'])
            if len(links) < 2:
                self.logger.error("Could not find corn and soybeans download links")
                return None
            
            # Download and process corn data
            corn_data = self._download_and_process_file(
                self.base_url + links[0], 
                'corn', 
                'recent'
            )
            
            # Download and process soybeans data
            soybeans_data = self._download_and_process_file(
                self.base_url + links[1], 
                'soybeans', 
                'recent'
            )
            
            if corn_data is None or soybeans_data is None:
                return None
            
            return pd.concat([corn_data, soybeans_data], ignore_index=True)
            
        except Exception as e:
            self.logger.error(f"Failed to scrape recent data: {e}")
            return None
    
    def _scrape_historical_data(self) -> Optional[pd.DataFrame]:
        """Scrape historical data (1975-1996)."""
        try:
            self.logger.info("Scraping historical data")
            
            # Get page content
            response = self.make_request(self.main_url)
            if response is None:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find historical download links
            links = self._find_download_links(soup, ['us-1975-95', 'us-1975-96'])
            if len(links) < 2:
                self.logger.error("Could not find historical download links")
                return None
            
            # Download and process historical corn
            hist_corn_data = self._download_and_process_file(
                self.base_url + links[0], 
                'corn', 
                'historical'
            )
            
            # Download and process historical soybeans
            hist_soy_data = self._download_and_process_file(
                self.base_url + links[1], 
                'soybeans', 
                'historical'
            )
            
            if hist_corn_data is None or hist_soy_data is None:
                return None
            
            return pd.concat([hist_corn_data, hist_soy_data], ignore_index=True)
            
        except Exception as e:
            self.logger.error(f"Failed to scrape historical data: {e}")
            return None
    
    def _scrape_forecast_data(self) -> Optional[pd.DataFrame]:
        """Scrape forecast data."""
        try:
            self.logger.info("Scraping forecast data")
            
            # Get page content
            response = self.make_request(self.main_url)
            if response is None:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find forecast download links
            links = self._find_download_links(soup, ['cost-of-production-forecasts-for-major-us-field-crops'])
            if not links:
                self.logger.error("Could not find forecast download links")
                return None
            
            # Download and process forecast data
            forecast_data = self._download_and_process_file(
                self.base_url + links[0], 
                'forecast', 
                'forecast'
            )
            
            return forecast_data
            
        except Exception as e:
            self.logger.error(f"Failed to scrape forecast data: {e}")
            return None
    
    def _find_download_links(self, soup: BeautifulSoup, keywords: List[str]) -> List[str]:
        """
        Find download links based on keywords.
        
        Args:
            soup (BeautifulSoup): Parsed HTML content
            keywords (List[str]): Keywords to search for
            
        Returns:
            List[str]: List of download links
        """
        links = soup.find_all('a', href=True)
        result = []
        
        for keyword in keywords:
            for link in links:
                href = link['href'].lower()
                if keyword.lower() in href and ('xlsx' in href or 'xls' in href):
                    result.append(link['href'])
                    break
        
        return result
    
    def _download_and_process_file(self, url: str, commodity: str, data_type: str) -> Optional[pd.DataFrame]:
        """
        Download and process a single file.
        
        Args:
            url (str): URL to download
            commodity (str): Commodity type
            data_type (str): Type of data (recent, historical, forecast)
            
        Returns:
            Optional[pd.DataFrame]: Processed data
        """
        try:
            # Download file
            response = self.make_request(url)
            if response is None:
                return None
            
            # Determine file extension
            file_ext = '.xlsx' if url.endswith('.xlsx') else '.xls'
            temp_filename = f"temp_{commodity}_{data_type}{file_ext}"
            
            # Save temporary file
            with open(temp_filename, 'wb') as f:
                f.write(response.content)
            
            # Process based on data type
            if data_type == 'recent':
                data = self._process_recent_file(temp_filename, commodity)
            elif data_type == 'historical':
                data = self._process_historical_file(temp_filename, commodity)
            elif data_type == 'forecast':
                data = self._process_forecast_file(temp_filename, commodity)
            else:
                self.logger.error(f"Unknown data type: {data_type}")
                return None
            
            # Clean up temporary file
            import os
            try:
                os.remove(temp_filename)
            except OSError:
                pass
            
            return data
            
        except Exception as e:
            self.logger.error(f"Failed to download and process {url}: {e}")
            return None
    
    def _process_recent_file(self, filename: str, commodity: str) -> Optional[pd.DataFrame]:
        """Process recent data file."""
        try:
            df = pd.read_excel(filename, skiprows=4, header=1)
            df = df.dropna(how='all')
            df = df.iloc[1:]
            
            # Set up columns
            df.columns = ['Item'] + [str(int(x)) for x in df.iloc[0, 1:].fillna(0)]
            df = df.iloc[1:]
            df = df.set_index('Item')
            df = df.T
            df['Year'] = df.index
            df = df.reset_index(drop=True)
            
            # Select relevant columns based on commodity
            if commodity.lower() == 'corn':
                columns = ['Year', 'Primary product, grain', 'Secondary product, silage', 
                          'Total, gross value of production', 'Total, operating costs', 
                          'Total, allocated overhead', 'Total, costs listed', 'Net value']
            else:  # soybeans
                columns = ['Year', 'Primary product, soybeans', 'Total, gross value of production', 
                          'Total, operating costs', 'Total, allocated overhead', 
                          'Total, costs listed', 'Net value']
            
            # Filter available columns
            available_columns = [col for col in columns if col in df.columns]
            df = df[available_columns]
            
            # Melt the dataframe
            df = df.melt(id_vars=['Year'], var_name='Item', value_name='Value')
            df['Commodity'] = commodity.title()
            df['Source'] = 'USDA'
            df['Location'] = 'National'
            df['Unit'] = self._assign_units(df['Item'])
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to process recent file {filename}: {e}")
            return None
    
    def _process_historical_file(self, filename: str, commodity: str) -> Optional[pd.DataFrame]:
        """Process historical data file."""
        try:
            df = pd.read_excel(filename, skiprows=4, header=1)
            df = df.dropna(how='all')
            df = df.iloc[1:]
            
            # Set up columns
            df.columns = ['Item'] + [str(int(x)) for x in df.iloc[0, 1:].fillna(0)]
            df = df.iloc[1:]
            df = df.set_index('Item')
            df = df.T
            df['Year'] = df.index
            df = df.reset_index(drop=True)
            
            # Select relevant columns based on commodity
            if commodity.lower() == 'corn':
                columns = ['Year', '  Corn grain', '  Corn silage', 
                          '    Total, gross value of production', '    Total, cash expenses', 
                          '    Subtotal', '  Residual returns to risk and management  ']
            else:  # soybeans
                columns = ['Year', '  Soybeans', '    Total, gross value of production', 
                          '      Total, cash expenses', '    Total, economic costs', 
                          '  Residual returns to management and risk']
            
            # Filter available columns
            available_columns = [col for col in columns if col in df.columns]
            df = df[available_columns]
            
            # Melt the dataframe
            df = df.melt(id_vars=['Year'], var_name='Item', value_name='Value')
            df['Commodity'] = commodity.title()
            df['Source'] = 'USDA'
            df['Location'] = 'National'
            df['Unit'] = self._assign_units(df['Item'])
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to process historical file {filename}: {e}")
            return None
    
    def _process_forecast_file(self, filename: str, commodity: str) -> Optional[pd.DataFrame]:
        """Process forecast data file."""
        try:
            df = pd.read_excel(filename, skiprows=2, header=1)
            df = df.dropna(how='all')
            df = df.iloc[1:]
            
            # Set up columns
            df.columns = ['Item'] + [str(int(x)) for x in df.iloc[0, 1:].fillna(0)]
            df = df.iloc[1:]
            df = df.set_index('Item')
            df = df.T
            df['Year'] = df.index
            df = df.reset_index(drop=True)
            
            # Select relevant columns
            columns = ['Year', '      Total, operating costs', '      Total, allocated costs', 
                      '      Total, costs listed']
            available_columns = [col for col in columns if col in df.columns]
            df = df[available_columns]
            
            # Melt the dataframe
            df = df.melt(id_vars=['Year'], var_name='Item', value_name='Value')
            df['Commodity'] = 'Forecast'
            df['Source'] = 'USDA'
            df['Location'] = 'National'
            df['Unit'] = self._assign_units(df['Item'])
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to process forecast file {filename}: {e}")
            return None
    
    def _assign_units(self, items: pd.Series) -> List[str]:
        """Assign appropriate units based on item names."""
        units = []
        for item in items:
            item_str = str(item).lower()
            if 'yield' in item_str or 'grain' in item_str or 'silage' in item_str:
                units.append('bu/acre')
            elif 'price' in item_str:
                units.append('$/bu')
            else:
                units.append('$/acre')
        return units
    
    def _standardize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize data format."""
        # Transform year format
        df['Year'] = df['Year'].apply(self.transform_year)
        
        # Clean item names
        df['Item'] = df['Item'].astype(str).str.strip()
        
        # Ensure numeric values
        df['Value'] = pd.to_numeric(df['Value'], errors='coerce')
        
        # Remove rows with invalid values
        df = df.dropna(subset=['Value'])
        
        return df


def main():
    """Main execution function."""
    scraper = USDAScraper()
    result = scraper.run()
    
    if result is not None:
        print(f"USDA data scraping completed successfully!")
        print(f"Total records: {len(result)}")
        print(f"Commodities: {result['Commodity'].unique()}")
        print(f"Years: {sorted(result['Year'].unique())}")
    else:
        print("USDA data scraping failed. Check logs for details.")


if __name__ == "__main__":
    main()
