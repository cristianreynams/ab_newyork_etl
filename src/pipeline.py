"""
Main ETL pipeline orchestration
"""
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
from loguru import logger

from .extract import DataExtractor
from .transform import DataTransformer
from .load import DataLoader
from .utils import setup_logging, load_config

class ETLPipeline:
    """Main ETL pipeline orchestrator"""
    
    def __init__(self, config_path: Optional[str] = None):
        # Load configuration
        self.config = load_config(config_path)
        
        # Setup logging
        setup_logging(self.config)
        
        # Initialize components
        self.extractor = DataExtractor(self.config)
        self.transformer = DataTransformer(self.config)
        self.loader = DataLoader(self.config)
        
        logger.info("ETL Pipeline initialized")
    
    def run(self, source_path: str, source_type: Optional[str] = None) -> pd.DataFrame:
        """
        Run the complete ETL pipeline
        
        Args:
            source_path: Path to data source
            source_type: Type of source (zip, csv, auto)
            
        Returns:
            Processed DataFrame
        """
        logger.info("=" * 50)
        logger.info("Starting ETL Pipeline")
        logger.info(f"Source: {source_path}")
        logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 50)
        
        try:
            # 1. EXTRACT
            logger.info("Phase 1: EXTRACTION")
            raw_data = self.extractor.extract(source_path, source_type)
            logger.info(f"Extracted {len(raw_data)} rows")
            
            # 2. TRANSFORM
            logger.info("Phase 2: TRANSFORMATION")
            transformed_data = self.transformer.transform(raw_data)
            logger.info(f"Transformed {len(transformed_data)} rows")
            
            # 3. LOAD
            logger.info("Phase 3: LOADING")
            self.loader.load(transformed_data, "nyc_airbnb_processed")
            
            # Generate summary
            self._generate_summary(raw_data, transformed_data)
            
            logger.info("=" * 50)
            logger.info("ETL Pipeline completed successfully!")
            logger.info("=" * 50)
            
            return transformed_data
            
        except Exception as e:
            logger.error(f"ETL Pipeline failed: {str(e)}")
            raise
    
    def _generate_summary(self, raw_df: pd.DataFrame, processed_df: pd.DataFrame) -> None:
        """Generate and log pipeline summary"""
        summary = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'raw_rows': len(raw_df),
            'raw_columns': len(raw_df.columns),
            'processed_rows': len(processed_df),
            'processed_columns': len(processed_df.columns),
            'rows_dropped': len(raw_df) - len(processed_df),
            'columns_dropped': len(raw_df.columns) - len(processed_df.columns),
            'completion_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        logger.info("Pipeline Summary:")
        for key, value in summary.items():
            logger.info(f"  {key}: {value}")
        
        # Save summary to file
        summary_path = Path(self.config['paths']['processed_data']) / "pipeline_summary.txt"
        with open(summary_path, 'w') as f:
            for key, value in summary.items():
                f.write(f"{key}: {value}\n")
