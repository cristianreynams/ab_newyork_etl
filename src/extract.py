"""
Extraction module for NYC Airbnb data
"""
import pandas as pd
import zipfile
import os
from pathlib import Path
from typing import Optional, Union, Dict, Any
import logging
from loguru import logger

class DataExtractor:
    """Handles data extraction from various sources"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.raw_path = Path(config['paths']['raw_data'])
        self.raw_path.mkdir(parents=True, exist_ok=True)
    
    def extract_from_zip(self, zip_path: Union[str, Path]) -> pd.DataFrame:
        """
        Extract data from ZIP file
        
        Args:
            zip_path: Path to ZIP file
            
        Returns:
            Extracted DataFrame
        """
        logger.info(f"Extracting data from {zip_path}")
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # List files in ZIP
                file_list = zip_ref.namelist()
                logger.info(f"Files in ZIP: {file_list}")
                
                # Find CSV files
                csv_files = [f for f in file_list if f.endswith('.csv')]
                if not csv_files:
                    raise ValueError("No CSV files found in ZIP archive")
                
                # Extract first CSV file
                csv_file = csv_files[0]
                logger.info(f"Extracting {csv_file}")
                
                # Extract to raw data directory
                zip_ref.extract(csv_file, self.raw_path)
                
                # Load the CSV
                csv_path = self.raw_path / csv_file
                df = pd.read_csv(
                    csv_path,
                    encoding=self.config['extraction']['encoding'],
                    compression=self.config['extraction']['compression']
                )
                
                logger.info(f"Successfully extracted data. Shape: {df.shape}")
                return df
                
        except Exception as e:
            logger.error(f"Failed to extract data: {str(e)}")
            raise
    
    def extract_from_csv(self, csv_path: Union[str, Path]) -> pd.DataFrame:
        """
        Extract data from CSV file
        
        Args:
            csv_path: Path to CSV file
            
        Returns:
            Extracted DataFrame
        """
        logger.info(f"Loading data from {csv_path}")
        
        try:
            df = pd.read_csv(
                csv_path,
                encoding=self.config['extraction']['encoding']
            )
            logger.info(f"Successfully loaded data. Shape: {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to load CSV: {str(e)}")
            raise
    
    def extract(self, source_path: Union[str, Path], 
                source_type: Optional[str] = None) -> pd.DataFrame:
        """
        Main extraction method
        
        Args:
            source_path: Path to data source
            source_type: Type of source (zip, csv, auto)
            
        Returns:
            Extracted DataFrame
        """
        source_path = Path(source_path)
        
        if source_type is None:
            # Auto-detect source type
            if source_path.suffix.lower() == '.zip':
                source_type = 'zip'
            elif source_path.suffix.lower() == '.csv':
                source_type = 'csv'
            else:
                raise ValueError(f"Unsupported file type: {source_path.suffix}")
        
        if source_type == 'zip':
            return self.extract_from_zip(source_path)
        elif source_type == 'csv':
            return self.extract_from_csv(source_path)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
