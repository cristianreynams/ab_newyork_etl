"""
Loading module for NYC Airbnb data
"""
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List
import sqlalchemy as sa
from loguru import logger

class DataLoader:
    """Handles data loading to various destinations"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.processed_path = Path(config['paths']['processed_data'])
        self.processed_path.mkdir(parents=True, exist_ok=True)
    
    def save_to_disk(self, df: pd.DataFrame, name: str) -> None:
        """
        Save DataFrame to disk in multiple formats
        
        Args:
            df: DataFrame to save
            name: Base name for files
        """
        logger.info(f"Saving data to disk: {name}")
        
        output_formats = self.config['loading']['output_formats']
        
        for fmt in output_formats:
            file_path = self.processed_path / f"{name}.{fmt}"
            
            try:
                if fmt == 'csv':
                    df.to_csv(file_path, index=False)
                elif fmt == 'parquet':
                    df.to_parquet(file_path, index=False)
                elif fmt == 'json':
                    df.to_json(file_path, orient='records', indent=2)
                elif fmt == 'excel':
                    df.to_excel(file_path, index=False)
                else:
                    logger.warning(f"Unsupported format: {fmt}")
                    continue
                
                logger.info(f"Saved {file_path} ({file_path.stat().st_size / 1024:.2f} KB)")
                
            except Exception as e:
                logger.error(f"Failed to save {fmt}: {str(e)}")
    
    def save_to_database(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Save DataFrame to database
        
        Args:
            df: DataFrame to save
            table_name: Name of table in database
        """
        db_config = self.config['loading']['database']
        
        if not db_config.get('enabled', False):
            logger.info("Database loading disabled in config")
            return
        
        logger.info(f"Saving data to database table: {table_name}")
        
        try:
            engine = sa.create_engine(db_config['connection_string'])
            
            # Save to database
            df.to_sql(
                table_name,
                engine,
                if_exists='replace',
                index=False,
                method='multi'
            )
            
            logger.info(f"Successfully saved to database: {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to save to database: {str(e)}")
    
    def run_quality_checks(self, df: pd.DataFrame) -> bool:
        """
        Run data quality checks
        
        Args:
            df: DataFrame to check
            
        Returns:
            True if checks pass, False otherwise
        """
        if not self.config['quality_checks']['enabled']:
            return True
        
        logger.info("Running quality checks")
        checks_passed = True
        
        # Check 1: No null IDs
        if 'id' in df.columns:
            null_ids = df['id'].isnull().sum()
            if null_ids > 0:
                logger.error(f"Quality check failed: {null_ids} null IDs found")
                checks_passed = False
        
        # Check 2: Price positive
        if 'price' in df.columns:
            negative_prices = (df['price'] < 0).sum()
            if negative_prices > 0:
                logger.error(f"Quality check failed: {negative_prices} negative prices found")
                checks_passed = False
        
        # Check 3: Dates consistency
        date_cols = [col for col in df.columns if 'date' in col or 'review' in col]
        for col in date_cols:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                future_dates = (df[col] > pd.Timestamp.now()).sum()
                if future_dates > 0:
                    logger.warning(f"Found {future_dates} future dates in {col}")
        
        if checks_passed:
            logger.info("All quality checks passed")
        
        return checks_passed
    
    def load(self, df: pd.DataFrame, name: str = "airbnb_processed") -> None:
        """
        Main loading method
        
        Args:
            df: Transformed DataFrame
            name: Base name for outputs
        """
        logger.info("Starting data loading")
        
        # Run quality checks
        if not self.run_quality_checks(df):
            logger.warning("Quality checks failed, but continuing with loading")
        
        # Save to disk
        self.save_to_disk(df, name)
        
        # Save to database
        self.save_to_database(df, f"{name}_table")
        
        logger.info("Data loading complete")
