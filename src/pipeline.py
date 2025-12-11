"""
ETL Pipeline for NYC Airbnb Data
"""

import pandas as pd
import zipfile
import os
from pathlib import Path
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('etl_pipeline.log')
    ]
)
logger = logging.getLogger(__name__)


class NYC_Airbnb_ETL:
    """
    Main ETL pipeline class for NYC Airbnb data
    """
    
    def __init__(self):
        """Initialize the pipeline"""
        self.data = None
        self.processed_data = None
        logger.info("ETL Pipeline initialized")
    
    def extract(self, zip_path):
        """
        Extract data from ZIP file
        
        Args:
            zip_path: Path to ZIP file
            
        Returns:
            DataFrame with extracted data
        """
        logger.info(f"Extracting data from: {zip_path}")
        
        try:
            # Check if file exists
            if not os.path.exists(zip_path):
                raise FileNotFoundError(f"File not found: {zip_path}")
            
            # Open ZIP file
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # List files in ZIP
                file_list = zip_ref.namelist()
                logger.info(f"Files in ZIP: {file_list}")
                
                # Look for CSV files
                csv_files = [f for f in file_list if f.lower().endswith('.csv')]
                
                if not csv_files:
                    raise ValueError("No CSV files found in ZIP")
                
                # Take first CSV file
                csv_file = csv_files[0]
                logger.info(f"Processing file: {csv_file}")
                
                # Extract and read CSV
                with zip_ref.open(csv_file) as f:
                    df = pd.read_csv(f)
                
                logger.info(f"Data extracted successfully - Rows: {df.shape[0]}, Columns: {df.shape[1]}")
                self.data = df
                return df
                
        except Exception as e:
            logger.error(f"Extraction error: {str(e)}")
            raise
    
    def transform(self, df):
        """
        Transform and clean data
        
        Args:
            df: DataFrame with raw data
            
        Returns:
            DataFrame with transformed data
        """
        logger.info("Starting data transformation...")
        
        # Make a copy to avoid modifying original
        df_clean = df.copy()
        
        # 1. Clean column names
        df_clean.columns = [
            col.strip().lower().replace(' ', '_').replace('-', '_')
            for col in df_clean.columns
        ]
        logger.info(f"Columns after cleaning: {list(df_clean.columns)}")
        
        # 2. Remove completely empty rows
        initial_rows = len(df_clean)
        df_clean = df_clean.dropna(how='all')
        logger.info(f"Rows removed (completely empty): {initial_rows - len(df_clean)}")
        
        # 3. Remove duplicates
        df_clean = df_clean.drop_duplicates()
        logger.info(f"Rows after removing duplicates: {len(df_clean)}")
        
        # 4. Handle missing values
        missing_percent = (df_clean.isnull().sum() / len(df_clean)) * 100
        logger.info("Percentage of missing values per column:")
        for col, percent in missing_percent.items():
            if percent > 0:
                logger.info(f"  {col}: {percent:.2f}%")
        
        # 5. Clean price column
        if 'price' in df_clean.columns:
            logger.info("Cleaning 'price' column...")
            # Convert to string, remove symbols, convert to float
            df_clean['price'] = (
                df_clean['price']
                .astype(str)
                .str.replace(r'[\$,]', '', regex=True)
                .astype(float, errors='coerce')
            )
            
            # Filter valid prices
            valid_prices = df_clean['price'].between(0, 10000)
            df_clean = df_clean[valid_prices]
            logger.info(f"Valid prices (0-10000): {valid_prices.sum()} rows")
        
        # 6. Convert dates
        date_columns = ['last_review', 'host_since']
        for col in date_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
                logger.info(f"Converted date column: {col}")
        
        # 7. Create new features
        if 'last_review' in df_clean.columns:
            # Days since last review
            df_clean['days_since_last_review'] = (
                datetime.now() - df_clean['last_review']
            ).dt.days
            df_clean['days_since_last_review'] = df_clean['days_since_last_review'].fillna(-1)
        
        if 'price' in df_clean.columns and 'minimum_nights' in df_clean.columns:
            # Price per night
            df_clean['price_per_night'] = df_clean['price'] / df_clean['minimum_nights'].clip(lower=1)
        
        if 'availability_365' in df_clean.columns:
            # Availability boolean
            df_clean['is_available'] = df_clean['availability_365'] > 0
        
        logger.info(f"Transformation completed - Rows: {df_clean.shape[0]}, Columns: {df_clean.shape[1]}")
        self.processed_data = df_clean
        return df_clean
    
    def load(self, df, output_dir="data/processed"):
        """
        Save processed data
        
        Args:
            df: DataFrame with processed data
            output_dir: Output directory
            
        Returns:
            Tuple with paths to saved files
        """
        logger.info(f"Saving processed data to: {output_dir}")
        
        # Create directory if it doesn't exist
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Generate timestamp for unique names
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save as CSV
        csv_filename = f"nyc_airbnb_processed_{timestamp}.csv"
        csv_path = os.path.join(output_dir, csv_filename)
        df.to_csv(csv_path, index=False)
        logger.info(f"CSV saved: {csv_path}")
        
        # Save as Parquet (more efficient)
        parquet_filename = f"nyc_airbnb_processed_{timestamp}.parquet"
        parquet_path = os.path.join(output_dir, parquet_filename)
        df.to_parquet(parquet_path, index=False)
        logger.info(f"Parquet saved: {parquet_path}")
        
        # Also save a file without timestamp for reference
        latest_csv = os.path.join(output_dir, "nyc_airbnb_latest.csv")
        df.to_csv(latest_csv, index=False)
        
        # Create metadata file
        self._save_metadata(df, output_dir, timestamp)
        
        return csv_path, parquet_path
    
    def _save_metadata(self, df, output_dir, timestamp):
        """Save processing metadata"""
        metadata = {
            'timestamp': timestamp,
            'rows': len(df),
            'columns': len(df.columns),
            'columns_list': list(df.columns),
            'data_types': dict(df.dtypes.astype(str))
        }
        
        metadata_path = os.path.join(output_dir, f"metadata_{timestamp}.txt")
        with open(metadata_path, 'w') as f:
            for key, value in metadata.items():
                f.write(f"{key}: {value}\n")
        
        logger.info(f"Metadata saved: {metadata_path}")
    
    def run(self, zip_path, output_dir="data/processed"):
        """
        Run complete pipeline
        
        Args:
            zip_path: Path to ZIP file
            output_dir: Output directory
            
        Returns:
            DataFrame with processed data
        """
        logger.info("=" * 60)
        logger.info("STARTING ETL PIPELINE - NYC AIRBNB")
        logger.info("=" * 60)
        
        try:
            # 1. EXTRACTION
            raw_data = self.extract(zip_path)
            
            # 2. TRANSFORMATION
            processed_data = self.transform(raw_data)
            
            # 3. LOADING
            csv_path, parquet_path = self.load(processed_data, output_dir)
            
            # 4. SUMMARY
            logger.info("=" * 60)
            logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 60)
            logger.info(f"Summary:")
            logger.info(f"  - Raw data: {raw_data.shape[0]} rows, {raw_data.shape[1]} columns")
            logger.info(f"  - Processed data: {processed_data.shape[0]} rows, {processed_data.shape[1]} columns")
            logger.info(f"  - Generated files:")
            logger.info(f"      • CSV: {csv_path}")
            logger.info(f"      • Parquet: {parquet_path}")
            logger.info("=" * 60)
            
            return processed_data
            
        except Exception as e:
            logger.error(f"Pipeline error: {str(e)}")
            raise
