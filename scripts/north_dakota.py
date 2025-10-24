#!/usr/bin/env python3
"""
North Dakota Crop Budget Data Scraper

This script extracts crop budget data from the NDSU website,
processes it into a structured format, and exports to Excel.

Usage:
    python north_dakota_scraper.py

Requirements:
    - pandas
    - requests
    - beautifulsoup4
    - openpyxl (for Excel export)
"""

import pandas as pd
from bs4 import BeautifulSoup
import requests
import logging
from typing import List, Tuple, Optional
import sys
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('scraper.log', mode='w')
    ]
)
logger = logging.getLogger(__name__)


def fetch_page(url: str) -> Optional[str]:
    """
    Fetch the HTML content of a webpage with proper headers.

    Args:
        url (str): The URL to fetch.

    Returns:
        Optional[str]: The HTML content if successful, None otherwise.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        logger.info(f"Successfully fetched {url}")
        return response.text
    except requests.RequestException as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return None


def extract_links(soup: BeautifulSoup) -> List[Tuple[str, str]]:
    """
    Extract all href and text pairs from anchor tags within span elements.

    Args:
        soup (BeautifulSoup): Parsed HTML content.

    Returns:
        List[Tuple[str, str]]: List of (href, text) tuples.
    """
    links = []
    for span in soup.find_all("span"):
        for a in span.find_all("a", href=True):
            links.append((a["href"], a.text.strip()))
    logger.info(f"Extracted {len(links)} links from page")
    return links


def filter_links(links: List[Tuple[str, str]], min_year: int = 2023) -> List[Tuple[str, str, str]]:
    """
    Filter links to only XLS files from specified year onwards and extract metadata.

    Args:
        links (List[Tuple[str, str]]): List of (href, text) tuples.
        min_year (int): Minimum year to include.

    Returns:
        List[Tuple[str, str, str]]: List of (full_url, year, location) tuples.
    """
    filtered = []
    for href, text in links:
        if href.lower().endswith('.xls'):
            try:
                year = int(text[:4])
                if year >= min_year:
                    location = text.split('ND')[0][4:].strip() if 'ND' in text else text
                    full_url = f"https://www.ndsu.edu{href}"
                    filtered.append((full_url, str(year), location))
            except (ValueError, IndexError):
                logger.warning(f"Skipping invalid link text: {text}")
                continue
    logger.info(f"Filtered to {len(filtered)} valid XLS links for {min_year}+")
    return filtered


def assign_units(items: pd.Series) -> List[str]:
    """
    Assign appropriate units based on item names.

    Args:
        items (pd.Series): Series of item names.

    Returns:
        List[str]: List of corresponding units.
    """
    units = []
    unit_map = {
        "Market Yield": "bu/acre",
        "Market Price": "$/bu",
        "Market Price + LDP:": "$/bu"
    }
    for item in items:
        item_str = str(item).strip()
        units.append(unit_map.get(item_str, "$/acre"))
    return units


def process_excel_sheet(url: str, sheet: str, location: str, year: str) -> Optional[pd.DataFrame]:
    """
    Process a single Excel sheet and return cleaned DataFrame.

    Args:
        url (str): URL of the Excel file.
        sheet (str): Sheet name to process.
        location (str): Location metadata.
        year (str): Year metadata.

    Returns:
        Optional[pd.DataFrame]: Cleaned DataFrame or None if failed.
    """
    try:
        df = pd.read_excel(url, sheet_name=sheet, usecols="A:B", nrows=29)
        if df.empty:
            logger.warning(f"Empty sheet {sheet} in {url}")
            return None
        df = df.rename(columns={df.columns[0]: "Item", df.columns[1]: "Value"})
        df = df.dropna()
        df["Item"] = df["Item"].astype(str).str.replace(r"-", "", regex=True)
        df = df.assign(
            Location=f"ND {location}",
            Source="NDSU",
            Commodity=sheet,
            Year=year
        )
        df["Unit"] = assign_units(df["Item"])
        logger.info(f"Processed {sheet} from {url}: {len(df)} rows")
        return df
    except Exception as e:
        logger.warning(f"Failed to process {sheet} from {url}: {e}")
        return None


def extract_north_dakota_data(url: str, min_year: int = 2006) -> pd.DataFrame:
    """
    Main function to extract North Dakota crop budget data.

    Args:
        url (str): Base URL for scraping.
        min_year (int): Minimum year to include.

    Returns:
        pd.DataFrame: Combined DataFrame of all extracted data.
    """
    logger.info("Starting data extraction...")

    page_content = fetch_page(url)
    if not page_content:
        logger.error("Failed to fetch main page")
        return pd.DataFrame()

    soup = BeautifulSoup(page_content, "html.parser")
    links = extract_links(soup)
    filtered_links = filter_links(links, min_year)

    if not filtered_links:
        logger.warning("No valid links found")
        return pd.DataFrame()

    commodities = ["Corn", "Soy", "Soybean"]  # Corrected commodities
    all_data = []

    for link_url, year, location in filtered_links:
        for crop in commodities:
            df = process_excel_sheet(link_url, crop, location, year)
            if df is not None:
                all_data.append(df)

    if all_data:
        result = pd.concat(all_data, ignore_index=True)
        logger.info(f"Extracted {len(result)} total rows of data")
        return result
    else:
        logger.warning("No data extracted")
        return pd.DataFrame()


def transform_year(year):
    """
    Transform year format from YYYY to YYYY/YYYY+1.

    Args:
        year: Year value to transform.

    Returns:
        str: Transformed year string.
    """
    try:
        y = int(year)
        return f"{y}/{y+1}"
    except ValueError:
        return year


def main():
    """Main execution function."""
    url = "https://www.ndsu.edu/agriculture/ag-hub/ag-topics/farm-management/crop-economics/projected-crop-budgets"
    output_path = "North_Dakota.xlsx"

    logger.info("North Dakota Crop Budget Scraper Started")

    data = extract_north_dakota_data(url, 2006)

    if not data.empty:
        # Transform year format
        data["Year"] = data["Year"].apply(transform_year)

        # Save to Excel
        data.to_excel(output_path, index=False)
        logger.info(f"Data saved to {output_path}")

        # Print summary
        print(f"Extraction completed successfully!")
        print(f"Total rows: {len(data)}")
        print(f"Unique locations: {sorted(data['Location'].unique())}")
        print(f"Years covered: {sorted(data['Year'].unique())}")
        print(f"Output saved to: {os.path.abspath(output_path)}")
    else:
        logger.error("No data extracted. Check logs for details.")
        sys.exit(1)

if __name__ == "__main__":
    main()
