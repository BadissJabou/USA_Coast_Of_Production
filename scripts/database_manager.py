#!/usr/bin/env python3
"""
Database Integration Module for Agricultural Data

This module provides SQLite database integration for storing and managing
agricultural crop budget data.

Author: Agricultural Data Team
Date: 2024
"""

import sqlite3
import pandas as pd
import logging
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
import yaml
from datetime import datetime
import hashlib


class AgriculturalDatabase:
    """
    SQLite database manager for agricultural data.
    
    Provides functionality for:
    - Database initialization
    - Data insertion and retrieval
    - Data versioning and history
    - Query optimization
    - Data integrity checks
    """
    
    def __init__(self, db_path: str = "data/agricultural_data.db", config_path: str = "config.yaml"):
        """
        Initialize the database manager.
        
        Args:
            db_path (str): Path to SQLite database file
            config_path (str): Path to configuration file
        """
        self.db_path = db_path
        self.config = self._load_config(config_path)
        self.logger = logging.getLogger(__name__)
        
        # Ensure database directory exists
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize database
        self._init_database()
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            self.logger.warning(f"Config file {config_path} not found, using defaults")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration."""
        return {
            'database': {
                'tables': {
                    'raw_data': 'raw_scraped_data',
                    'processed_data': 'processed_data',
                    'metadata': 'scraping_metadata'
                }
            }
        }
    
    def _init_database(self):
        """Initialize database tables."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create raw data table
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.config['database']['tables']['raw_data']} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        item TEXT NOT NULL,
                        value REAL NOT NULL,
                        unit TEXT NOT NULL,
                        commodity TEXT NOT NULL,
                        location TEXT NOT NULL,
                        year TEXT NOT NULL,
                        source TEXT NOT NULL,
                        soil_type TEXT,
                        rotation TEXT,
                        tillage TEXT,
                        category TEXT,
                        notes TEXT,
                        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        data_hash TEXT,
                        UNIQUE(item, value, unit, commodity, location, year, source, scraped_at)
                    )
                """)
                
                # Create processed data table
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.config['database']['tables']['processed_data']} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        item TEXT NOT NULL,
                        value REAL NOT NULL,
                        unit TEXT NOT NULL,
                        commodity TEXT NOT NULL,
                        location TEXT NOT NULL,
                        year TEXT NOT NULL,
                        source TEXT NOT NULL,
                        soil_type TEXT,
                        rotation TEXT,
                        tillage TEXT,
                        category TEXT,
                        notes TEXT,
                        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        processing_version TEXT,
                        data_hash TEXT,
                        UNIQUE(item, value, unit, commodity, location, year, source, processed_at)
                    )
                """)
                
                # Create metadata table
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.config['database']['tables']['metadata']} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        source TEXT NOT NULL,
                        location TEXT NOT NULL,
                        commodity TEXT NOT NULL,
                        year TEXT NOT NULL,
                        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        records_count INTEGER,
                        data_quality_score REAL,
                        validation_errors TEXT,
                        processing_status TEXT DEFAULT 'pending',
                        file_path TEXT,
                        checksum TEXT,
                        UNIQUE(source, location, commodity, year, scraped_at)
                    )
                """)
                
                # Create indexes for better performance
                self._create_indexes(cursor)
                
                conn.commit()
                self.logger.info("Database initialized successfully")
                
        except sqlite3.Error as e:
            self.logger.error(f"Database initialization failed: {e}")
            raise
    
    def _create_indexes(self, cursor):
        """Create database indexes for better performance."""
        indexes = [
            f"CREATE INDEX IF NOT EXISTS idx_raw_commodity_year ON {self.config['database']['tables']['raw_data']} (commodity, year)",
            f"CREATE INDEX IF NOT EXISTS idx_raw_location ON {self.config['database']['tables']['raw_data']} (location)",
            f"CREATE INDEX IF NOT EXISTS idx_raw_source ON {self.config['database']['tables']['raw_data']} (source)",
            f"CREATE INDEX IF NOT EXISTS idx_raw_scraped_at ON {self.config['database']['tables']['raw_data']} (scraped_at)",
            f"CREATE INDEX IF NOT EXISTS idx_processed_commodity_year ON {self.config['database']['tables']['processed_data']} (commodity, year)",
            f"CREATE INDEX IF NOT EXISTS idx_processed_location ON {self.config['database']['tables']['processed_data']} (location)",
            f"CREATE INDEX IF NOT EXISTS idx_processed_source ON {self.config['database']['tables']['processed_data']} (source)",
            f"CREATE INDEX IF NOT EXISTS idx_metadata_source ON {self.config['database']['tables']['metadata']} (source)",
            f"CREATE INDEX IF NOT EXISTS idx_metadata_scraped_at ON {self.config['database']['tables']['metadata']} (scraped_at)"
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
    
    def insert_raw_data(self, df: pd.DataFrame, source: str, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Insert raw scraped data into database.
        
        Args:
            df (pd.DataFrame): Data to insert
            source (str): Data source identifier
            metadata (Optional[Dict[str, Any]]): Additional metadata
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Prepare data for insertion
                records = []
                for _, row in df.iterrows():
                    # Calculate data hash for deduplication
                    data_hash = self._calculate_data_hash(row)
                    
                    record = (
                        row.get('Item', ''),
                        row.get('Value', 0.0),
                        row.get('Unit', ''),
                        row.get('Commodity', ''),
                        row.get('Location', ''),
                        row.get('Year', ''),
                        source,
                        row.get('Soil_Type'),
                        row.get('Rotation'),
                        row.get('Tillage'),
                        row.get('Category'),
                        row.get('Notes'),
                        data_hash
                    )
                    records.append(record)
                
                # Insert data
                insert_sql = f"""
                    INSERT OR REPLACE INTO {self.config['database']['tables']['raw_data']}
                    (item, value, unit, commodity, location, year, source, soil_type, 
                     rotation, tillage, category, notes, data_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                cursor.executemany(insert_sql, records)
                
                # Insert metadata
                if metadata:
                    self._insert_metadata(cursor, source, df, metadata)
                
                conn.commit()
                self.logger.info(f"Inserted {len(records)} records from {source}")
                return True
                
        except sqlite3.Error as e:
            self.logger.error(f"Failed to insert raw data: {e}")
            return False
    
    def insert_processed_data(self, df: pd.DataFrame, source: str, processing_version: str = "1.0") -> bool:
        """
        Insert processed data into database.
        
        Args:
            df (pd.DataFrame): Processed data to insert
            source (str): Data source identifier
            processing_version (str): Version of processing applied
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Prepare data for insertion
                records = []
                for _, row in df.iterrows():
                    data_hash = self._calculate_data_hash(row)
                    
                    record = (
                        row.get('Item', ''),
                        row.get('Value', 0.0),
                        row.get('Unit', ''),
                        row.get('Commodity', ''),
                        row.get('Location', ''),
                        row.get('Year', ''),
                        source,
                        row.get('Soil_Type'),
                        row.get('Rotation'),
                        row.get('Tillage'),
                        row.get('Category'),
                        row.get('Notes'),
                        processing_version,
                        data_hash
                    )
                    records.append(record)
                
                # Insert data
                insert_sql = f"""
                    INSERT OR REPLACE INTO {self.config['database']['tables']['processed_data']}
                    (item, value, unit, commodity, location, year, source, soil_type, 
                     rotation, tillage, category, notes, processing_version, data_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                cursor.executemany(insert_sql, records)
                conn.commit()
                self.logger.info(f"Inserted {len(records)} processed records from {source}")
                return True
                
        except sqlite3.Error as e:
            self.logger.error(f"Failed to insert processed data: {e}")
            return False
    
    def _insert_metadata(self, cursor, source: str, df: pd.DataFrame, metadata: Dict[str, Any]):
        """Insert scraping metadata."""
        try:
            # Calculate checksum of the data
            checksum = hashlib.md5(df.to_string().encode()).hexdigest()
            
            # Get unique values for metadata
            unique_locations = df['Location'].unique() if 'Location' in df.columns else ['']
            unique_commodities = df['Commodity'].unique() if 'Commodity' in df.columns else ['']
            unique_years = df['Year'].unique() if 'Year' in df.columns else ['']
            
            for location in unique_locations:
                for commodity in unique_commodities:
                    for year in unique_years:
                        # Count records for this combination
                        count = len(df[
                            (df['Location'] == location) & 
                            (df['Commodity'] == commodity) & 
                            (df['Year'] == year)
                        ])
                        
                        metadata_record = (
                            source,
                            location,
                            commodity,
                            year,
                            count,
                            metadata.get('data_quality_score', 0.0),
                            metadata.get('validation_errors', ''),
                            metadata.get('processing_status', 'completed'),
                            metadata.get('file_path', ''),
                            checksum
                        )
                        
                        insert_sql = f"""
                            INSERT OR REPLACE INTO {self.config['database']['tables']['metadata']}
                            (source, location, commodity, year, records_count, data_quality_score,
                             validation_errors, processing_status, file_path, checksum)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                        
                        cursor.execute(insert_sql, metadata_record)
                        
        except Exception as e:
            self.logger.error(f"Failed to insert metadata: {e}")
    
    def _calculate_data_hash(self, row: pd.Series) -> str:
        """Calculate hash for data deduplication."""
        data_string = f"{row.get('Item', '')}{row.get('Value', '')}{row.get('Unit', '')}{row.get('Commodity', '')}{row.get('Location', '')}{row.get('Year', '')}"
        return hashlib.md5(data_string.encode()).hexdigest()
    
    def query_data(self, 
                   table: str = 'raw_data',
                   commodity: Optional[str] = None,
                   location: Optional[str] = None,
                   year: Optional[str] = None,
                   source: Optional[str] = None,
                   limit: Optional[int] = None) -> pd.DataFrame:
        """
        Query data from database.
        
        Args:
            table (str): Table to query ('raw_data' or 'processed_data')
            commodity (Optional[str]): Filter by commodity
            location (Optional[str]): Filter by location
            year (Optional[str]): Filter by year
            source (Optional[str]): Filter by source
            limit (Optional[int]): Limit number of results
            
        Returns:
            pd.DataFrame: Query results
        """
        try:
            table_name = self.config['database']['tables'][table]
            
            # Build query
            conditions = []
            params = []
            
            if commodity:
                conditions.append("commodity = ?")
                params.append(commodity)
            
            if location:
                conditions.append("location = ?")
                params.append(location)
            
            if year:
                conditions.append("year = ?")
                params.append(year)
            
            if source:
                conditions.append("source = ?")
                params.append(source)
            
            where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
            limit_clause = f"LIMIT {limit}" if limit else ""
            
            query = f"""
                SELECT * FROM {table_name}
                {where_clause}
                ORDER BY scraped_at DESC
                {limit_clause}
            """
            
            with sqlite3.connect(self.db_path) as conn:
                df = pd.read_sql_query(query, conn, params=params)
                self.logger.info(f"Retrieved {len(df)} records from {table}")
                return df
                
        except sqlite3.Error as e:
            self.logger.error(f"Query failed: {e}")
            return pd.DataFrame()
    
    def get_data_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics of stored data.
        
        Returns:
            Dict[str, Any]: Summary statistics
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get counts by table
                raw_count = cursor.execute(f"SELECT COUNT(*) FROM {self.config['database']['tables']['raw_data']}").fetchone()[0]
                processed_count = cursor.execute(f"SELECT COUNT(*) FROM {self.config['database']['tables']['processed_data']}").fetchone()[0]
                
                # Get unique values
                unique_sources = cursor.execute(f"SELECT DISTINCT source FROM {self.config['database']['tables']['raw_data']}").fetchall()
                unique_commodities = cursor.execute(f"SELECT DISTINCT commodity FROM {self.config['database']['tables']['raw_data']}").fetchall()
                unique_locations = cursor.execute(f"SELECT DISTINCT location FROM {self.config['database']['tables']['raw_data']}").fetchall()
                
                # Get date range
                date_range = cursor.execute(f"""
                    SELECT MIN(scraped_at), MAX(scraped_at) 
                    FROM {self.config['database']['tables']['raw_data']}
                """).fetchone()
                
                summary = {
                    'total_raw_records': raw_count,
                    'total_processed_records': processed_count,
                    'unique_sources': [row[0] for row in unique_sources],
                    'unique_commodities': [row[0] for row in unique_commodities],
                    'unique_locations': [row[0] for row in unique_locations],
                    'date_range': {
                        'earliest': date_range[0],
                        'latest': date_range[1]
                    },
                    'database_size': Path(self.db_path).stat().st_size if Path(self.db_path).exists() else 0
                }
                
                return summary
                
        except sqlite3.Error as e:
            self.logger.error(f"Failed to get summary: {e}")
            return {}
    
    def cleanup_old_data(self, days_to_keep: int = 30) -> int:
        """
        Clean up old data beyond specified days.
        
        Args:
            days_to_keep (int): Number of days to keep data
            
        Returns:
            int: Number of records deleted
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Delete old raw data
                cursor.execute(f"""
                    DELETE FROM {self.config['database']['tables']['raw_data']}
                    WHERE scraped_at < datetime('now', '-{days_to_keep} days')
                """)
                raw_deleted = cursor.rowcount
                
                # Delete old processed data
                cursor.execute(f"""
                    DELETE FROM {self.config['database']['tables']['processed_data']}
                    WHERE processed_at < datetime('now', '-{days_to_keep} days')
                """)
                processed_deleted = cursor.rowcount
                
                # Delete old metadata
                cursor.execute(f"""
                    DELETE FROM {self.config['database']['tables']['metadata']}
                    WHERE scraped_at < datetime('now', '-{days_to_keep} days')
                """)
                metadata_deleted = cursor.rowcount
                
                conn.commit()
                
                total_deleted = raw_deleted + processed_deleted + metadata_deleted
                self.logger.info(f"Cleaned up {total_deleted} old records")
                return total_deleted
                
        except sqlite3.Error as e:
            self.logger.error(f"Cleanup failed: {e}")
            return 0
    
    def export_data(self, output_path: str, table: str = 'processed_data', format: str = 'excel') -> bool:
        """
        Export data to file.
        
        Args:
            output_path (str): Output file path
            table (str): Table to export
            format (str): Export format ('excel', 'csv', 'json')
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Query all data
            df = self.query_data(table=table)
            
            if df.empty:
                self.logger.warning("No data to export")
                return False
            
            # Export based on format
            if format.lower() == 'excel':
                df.to_excel(output_path, index=False)
            elif format.lower() == 'csv':
                df.to_csv(output_path, index=False)
            elif format.lower() == 'json':
                df.to_json(output_path, orient='records', indent=2)
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            self.logger.info(f"Exported {len(df)} records to {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Export failed: {e}")
            return False


def create_database_manager(db_path: str = "data/agricultural_data.db") -> AgriculturalDatabase:
    """
    Convenience function to create database manager.
    
    Args:
        db_path (str): Path to database file
        
    Returns:
        AgriculturalDatabase: Database manager instance
    """
    return AgriculturalDatabase(db_path)


if __name__ == "__main__":
    # Example usage
    db = AgriculturalDatabase()
    
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
    print(f"Insert successful: {success}")
    
    # Query data
    results = db.query_data(commodity='Corn', location='Iowa')
    print(f"Query results: {len(results)} records")
    
    # Get summary
    summary = db.get_data_summary()
    print(f"Database summary: {summary}")
