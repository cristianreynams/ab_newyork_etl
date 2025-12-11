"""
Transformation module for NYC Airbnb data
"""
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
from datetime import datetime
from loguru import logger

class DataTransformer:
    """Handles data transformation and cleaning"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.transform_config = config['transformation']
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the raw data
        
        Args:
            df: Raw DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        logger.info("Starting data cleaning")
        df_clean = df.copy()
        
        # 1. Handle column names
        df_clean.columns = [col.strip().lower().replace(' ', '_') 
                           for col in df_clean.columns]
        
        # 2. Remove duplicate rows
        initial_rows = len(df_clean)
        df_clean = df_clean.drop_duplicates()
        logger.info(f"Removed {initial_rows - len(df_clean)} duplicate rows")
        
        # 3. Handle missing values
        missing_threshold = self.transform_config['missing_threshold']
        
        # Calculate missing percentage
        missing_percent = df_clean.isnull().mean()
        
        # Drop columns with too many missing values
        cols_to_drop = missing_percent[missing_percent > missing_threshold].index.tolist()
        if cols_to_drop:
            logger.info(f"Dropping columns with >{missing_threshold*100}% missing: {cols_to_drop}")
            df_clean = df_clean.drop(columns=cols_to_drop)
        
        # 4. Clean numeric columns
        numeric_cols = self.transform_config.get('numeric_cols', [])
        for col in numeric_cols:
            if col in df_clean.columns:
                # Convert to numeric, coerce errors
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
                
                # Remove negative prices
                if col == 'price':
                    df_clean = df_clean[df_clean[col] >= 0]
                
                # Handle outliers using IQR method
                if self.transform_config.get('outlier_method') == 'iqr':
                    Q1 = df_clean[col].quantile(0.25)
                    Q3 = df_clean[col].quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - 1.5 * IQR
                    upper_bound = Q3 + 1.5 * IQR
                    
                    # Cap outliers instead of removing
                    df_clean[col] = df_clean[col].clip(lower_bound, upper_bound)
        
        # 5. Clean categorical columns
        categorical_cols = self.transform_config.get('categorical_cols', [])
        for col in categorical_cols:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(str).str.strip()
                # Replace empty strings with NaN
                df_clean[col] = df_clean[col].replace(['', 'nan', 'NaN', 'None'], np.nan)
        
        # 6. Clean date columns
        date_cols = self.transform_config.get('date_cols', [])
        for col in date_cols:
            if col in df_clean.columns:
                df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
        
        logger.info(f"Data cleaning complete. Shape: {df_clean.shape}")
        return df_clean
    
    def engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create new features from existing data
        
        Args:
            df: Cleaned DataFrame
            
        Returns:
            DataFrame with new features
        """
        logger.info("Engineering new features")
        df_features = df.copy()
        
        features_to_create = self.transform_config.get('create_features', [])
        
        if 'price_per_night' in features_to_create:
            if 'price' in df_features.columns and 'minimum_nights' in df_features.columns:
                df_features['price_per_night'] = df_features['price'] / df_features['minimum_nights'].clip(lower=1)
        
        if 'has_availability' in features_to_create:
            if 'availability_365' in df_features.columns:
                df_features['has_availability'] = df_features['availability_365'] > 0
        
        if 'review_recency' in features_to_create:
            if 'last_review' in df_features.columns:
                df_features['review_recency'] = (
                    datetime.now() - df_features['last_review']
                ).dt.days
                df_features['review_recency'] = df_features['review_recency'].fillna(-1)
        
        if 'is_superhost' in features_to_create:
            if 'host_name' in df_features.columns and 'number_of_reviews' in df_features.columns:
                # Simple heuristic for superhost (could be enhanced)
                df_features['is_superhost'] = (
                    (df_features['number_of_reviews'] > 50) &
                    (df_features['calculated_host_listings_count'] <= 3)
                )
        
        logger.info(f"Feature engineering complete. New columns: {list(df_features.columns)}")
        return df_features
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Main transformation method
        
        Args:
            df: Raw DataFrame
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Starting data transformation")
        
        # Clean data
        df_cleaned = self.clean_data(df)
        
        # Engineer features
        df_transformed = self.engineer_features(df_cleaned)
        
        logger.info(f"Transformation complete. Final shape: {df_transformed.shape}")
        return df_transformed
