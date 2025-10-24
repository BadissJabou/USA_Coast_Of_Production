#!/usr/bin/env python3
"""
Data Validation Module for Agricultural Data

This module provides comprehensive data validation and quality checks
for agricultural crop budget data.

Author: Agricultural Data Team
Date: 2024
"""

import pandas as pd
import numpy as np
import re
import logging
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
import yaml


@dataclass
class ValidationResult:
    """Container for validation results."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    cleaned_data: Optional[pd.DataFrame] = None
    stats: Optional[Dict[str, Any]] = None


class DataValidator:
    """
    Comprehensive data validator for agricultural data.
    
    Provides validation for:
    - Data schema compliance
    - Data type validation
    - Range validation
    - Format validation
    - Completeness checks
    """
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize the data validator.
        
        Args:
            config_path (str): Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.logger = logging.getLogger(__name__)
        
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
            'validation': {
                'year_pattern': r'^\d{4}/\d{4}$',
                'valid_commodities': ['Corn', 'Soybeans', 'Wheat', 'Cotton'],
                'valid_units': [r'\$/acre', r'bu/acre', r'\$/bu', r'\$/ton', r'\$/lb'],
                'min_year': 1975,
                'max_year': 2030,
                'value_ranges': {
                    'yield_min': 0,
                    'yield_max': 500,
                    'price_min': 0,
                    'price_max': 50,
                    'cost_min': 0,
                    'cost_max': 2000
                }
            },
            'data': {
                'required_columns': ['Item', 'Value', 'Unit', 'Commodity', 'Location', 'Year', 'Source']
            }
        }
    
    def validate_data(self, df: pd.DataFrame) -> ValidationResult:
        """
        Perform comprehensive data validation.
        
        Args:
            df (pd.DataFrame): Data to validate
            
        Returns:
            ValidationResult: Validation results
        """
        errors = []
        warnings = []
        
        # Check if DataFrame is empty
        if df.empty:
            errors.append("DataFrame is empty")
            return ValidationResult(False, errors, warnings)
        
        # Schema validation
        schema_result = self._validate_schema(df)
        errors.extend(schema_result['errors'])
        warnings.extend(schema_result['warnings'])
        
        # Data type validation
        type_result = self._validate_data_types(df)
        errors.extend(type_result['errors'])
        warnings.extend(type_result['warnings'])
        
        # Range validation
        range_result = self._validate_ranges(df)
        errors.extend(range_result['errors'])
        warnings.extend(range_result['warnings'])
        
        # Format validation
        format_result = self._validate_formats(df)
        errors.extend(format_result['errors'])
        warnings.extend(format_result['warnings'])
        
        # Completeness validation
        completeness_result = self._validate_completeness(df)
        errors.extend(completeness_result['errors'])
        warnings.extend(completeness_result['warnings'])
        
        # Clean data if validation passes
        cleaned_data = None
        if not errors:
            cleaned_data = self._clean_data(df)
        
        # Generate statistics
        stats = self._generate_stats(df)
        
        is_valid = len(errors) == 0
        
        return ValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            cleaned_data=cleaned_data,
            stats=stats
        )
    
    def _validate_schema(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """Validate data schema compliance."""
        errors = []
        warnings = []
        
        required_columns = self.config['data']['required_columns']
        
        # Check for required columns
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")
        
        # Check for unexpected columns
        unexpected_columns = set(df.columns) - set(required_columns)
        if unexpected_columns:
            warnings.append(f"Unexpected columns found: {unexpected_columns}")
        
        return {'errors': errors, 'warnings': warnings}
    
    def _validate_data_types(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """Validate data types."""
        errors = []
        warnings = []
        
        # Check Value column is numeric
        if 'Value' in df.columns:
            non_numeric = pd.to_numeric(df['Value'], errors='coerce').isna().sum()
            if non_numeric > 0:
                errors.append(f"Found {non_numeric} non-numeric values in 'Value' column")
        
        # Check string columns
        string_columns = ['Item', 'Unit', 'Commodity', 'Location', 'Year', 'Source']
        for col in string_columns:
            if col in df.columns:
                null_count = df[col].isna().sum()
                if null_count > 0:
                    warnings.append(f"Found {null_count} null values in '{col}' column")
        
        return {'errors': errors, 'warnings': warnings}
    
    def _validate_ranges(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """Validate value ranges."""
        errors = []
        warnings = []
        
        if 'Value' not in df.columns:
            return {'errors': errors, 'warnings': warnings}
        
        value_ranges = self.config['validation']['value_ranges']
        
        # Convert Value to numeric
        numeric_values = pd.to_numeric(df['Value'], errors='coerce')
        
        # Check for negative values
        negative_count = (numeric_values < 0).sum()
        if negative_count > 0:
            warnings.append(f"Found {negative_count} negative values")
        
        # Check for extremely high values (potential outliers)
        high_threshold = value_ranges['cost_max'] * 2
        high_count = (numeric_values > high_threshold).sum()
        if high_count > 0:
            warnings.append(f"Found {high_count} values exceeding {high_threshold}")
        
        return {'errors': errors, 'warnings': warnings}
    
    def _validate_formats(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """Validate data formats."""
        errors = []
        warnings = []
        
        # Validate year format
        if 'Year' in df.columns:
            year_pattern = self.config['validation']['year_pattern']
            invalid_years = df[~df['Year'].astype(str).str.match(year_pattern)]
            if len(invalid_years) > 0:
                errors.append(f"Found {len(invalid_years)} invalid year formats")
        
        # Validate commodity names
        if 'Commodity' in df.columns:
            valid_commodities = self.config['validation']['valid_commodities']
            invalid_commodities = df[~df['Commodity'].isin(valid_commodities)]
            if len(invalid_commodities) > 0:
                warnings.append(f"Found {len(invalid_commodities)} invalid commodity names")
        
        # Validate units
        if 'Unit' in df.columns:
            valid_units = self.config['validation']['valid_units']
            unit_pattern = '|'.join(valid_units)
            invalid_units = df[~df['Unit'].astype(str).str.match(unit_pattern)]
            if len(invalid_units) > 0:
                warnings.append(f"Found {len(invalid_units)} invalid unit formats")
        
        return {'errors': errors, 'warnings': warnings}
    
    def _validate_completeness(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """Validate data completeness."""
        errors = []
        warnings = []
        
        # Check for completely empty rows
        empty_rows = df.isnull().all(axis=1).sum()
        if empty_rows > 0:
            errors.append(f"Found {empty_rows} completely empty rows")
        
        # Check for rows missing critical data
        critical_columns = ['Item', 'Value', 'Commodity', 'Year']
        critical_missing = df[critical_columns].isnull().any(axis=1).sum()
        if critical_missing > 0:
            errors.append(f"Found {critical_missing} rows missing critical data")
        
        # Check for duplicate rows
        duplicates = df.duplicated().sum()
        if duplicates > 0:
            warnings.append(f"Found {duplicates} duplicate rows")
        
        return {'errors': errors, 'warnings': warnings}
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize data."""
        cleaned_df = df.copy()
        
        # Remove completely empty rows
        cleaned_df = cleaned_df.dropna(how='all')
        
        # Remove rows missing critical data
        critical_columns = ['Item', 'Value', 'Commodity', 'Year']
        cleaned_df = cleaned_df.dropna(subset=critical_columns)
        
        # Convert Value to numeric
        if 'Value' in cleaned_df.columns:
            cleaned_df['Value'] = pd.to_numeric(cleaned_df['Value'], errors='coerce')
            cleaned_df = cleaned_df.dropna(subset=['Value'])
        
        # Standardize string columns
        string_columns = ['Item', 'Unit', 'Commodity', 'Location', 'Year', 'Source']
        for col in string_columns:
            if col in cleaned_df.columns:
                cleaned_df[col] = cleaned_df[col].astype(str).str.strip()
        
        # Remove duplicates
        cleaned_df = cleaned_df.drop_duplicates()
        
        return cleaned_df
    
    def _generate_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate data statistics."""
        stats = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'memory_usage': df.memory_usage(deep=True).sum(),
            'data_types': df.dtypes.to_dict(),
            'null_counts': df.isnull().sum().to_dict()
        }
        
        if 'Value' in df.columns:
            numeric_values = pd.to_numeric(df['Value'], errors='coerce')
            stats['value_stats'] = {
                'mean': numeric_values.mean(),
                'median': numeric_values.median(),
                'std': numeric_values.std(),
                'min': numeric_values.min(),
                'max': numeric_values.max()
            }
        
        if 'Commodity' in df.columns:
            stats['commodity_counts'] = df['Commodity'].value_counts().to_dict()
        
        if 'Year' in df.columns:
            stats['year_range'] = {
                'min': df['Year'].min(),
                'max': df['Year'].max(),
                'unique_years': df['Year'].nunique()
            }
        
        return stats
    
    def validate_single_record(self, record: Dict[str, Any]) -> ValidationResult:
        """
        Validate a single data record.
        
        Args:
            record (Dict[str, Any]): Single record to validate
            
        Returns:
            ValidationResult: Validation results
        """
        df = pd.DataFrame([record])
        return self.validate_data(df)
    
    def get_data_quality_score(self, df: pd.DataFrame) -> float:
        """
        Calculate a data quality score (0-100).
        
        Args:
            df (pd.DataFrame): Data to score
            
        Returns:
            float: Quality score (0-100)
        """
        if df.empty:
            return 0.0
        
        score = 100.0
        
        # Deduct points for missing required columns
        required_columns = self.config['data']['required_columns']
        missing_columns = set(required_columns) - set(df.columns)
        score -= len(missing_columns) * 10
        
        # Deduct points for null values
        null_percentage = df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100
        score -= null_percentage * 0.5
        
        # Deduct points for invalid data types
        if 'Value' in df.columns:
            non_numeric = pd.to_numeric(df['Value'], errors='coerce').isna().sum()
            score -= (non_numeric / len(df)) * 20
        
        # Deduct points for duplicates
        duplicates = df.duplicated().sum()
        score -= (duplicates / len(df)) * 15
        
        return max(0.0, score)


def validate_agricultural_data(df: pd.DataFrame, config_path: str = "config.yaml") -> ValidationResult:
    """
    Convenience function to validate agricultural data.
    
    Args:
        df (pd.DataFrame): Data to validate
        config_path (str): Path to configuration file
        
    Returns:
        ValidationResult: Validation results
    """
    validator = DataValidator(config_path)
    return validator.validate_data(df)
