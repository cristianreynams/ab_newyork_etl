"""
Utility functions for ETL pipeline
"""
import yaml
from pathlib import Path
from typing import Dict, Any
import logging
from loguru import logger
import sys

def setup_logging(config: Dict[str, Any]) -> None:
    """
    Configure logging for the pipeline
    
    Args:
        config: Configuration dictionary
    """
    log_config = config.get('logging', {})
    
    # Remove default handler
    logger.remove()
    
    # Add console handler
    logger.add(
        sys.stdout,
        level=log_config.get('level', 'INFO'),
        format=log_config.get('format', '{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}')
    )
    
    # Add file handler
    log_path = Path(config['paths']['logs'])
    log_path.mkdir(exist_ok=True)
    
    logger.add(
        log_path / "etl_pipeline.log",
        level=log_config.get('level', 'INFO'),
        format=log_config.get('format', '{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}'),
        rotation=log_config.get('rotation', '10 MB'),
        retention=log_config.get('retention', '30 days')
    )

def load_config(config_path: Union[str, Path] = "config.yaml") -> Dict[str, Any]:
    """
    Load configuration from YAML file
    
    Args:
        config_path: Path to config file
        
    Returns:
        Configuration dictionary
    """
    config_path = Path(config_path)
    
    if not config_path.exists():
        # Create default config if doesn't exist
        default_config = {
            'paths': {
                'raw_data': 'data/raw/',
                'processed_data': 'data/processed/',
                'logs': 'logs/'
            }
        }
        return default_config
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return config

def save_config(config: Dict[str, Any], config_path: Union[str, Path] = "config.yaml") -> None:
    """
    Save configuration to YAML file
    
    Args:
        config: Configuration dictionary
        config_path: Path to config file
    """
    config_path = Path(config_path)
    config_path.parent.mkdir(exist_ok=True)
    
    with open(config_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)

def get_sample_data(df: pd.DataFrame, n: int = 5) -> pd.DataFrame:
    """
    Get sample data for testing
    
    Args:
        df: DataFrame to sample
        n: Number of samples
        
    Returns:
        Sampled DataFrame
    """
    return df.sample(min(n, len(df)))

def memory_usage(df: pd.DataFrame) -> str:
    """
    Calculate memory usage of DataFrame
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        Formatted memory usage string
    """
    memory_mb = df.memory_usage(deep=True).sum() / 1024 ** 2
    return f"{memory_mb:.2f} MB"
