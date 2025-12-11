"""
Tests for extraction module
"""
import pytest
import pandas as pd
from pathlib import Path
import tempfile
import zipfile
from src.extract import DataExtractor

@pytest.fixture
def sample_config():
    return {
        'paths': {
            'raw_data': 'test_raw_data'
        },
        'extraction': {
            'encoding': 'utf-8',
            'compression': 'infer'
        }
    }

@pytest.fixture
def sample_csv_data():
    return pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Listing 1', 'Listing 2', 'Listing 3'],
        'price': [100, 200, 150]
    })

def test_extractor_initialization(sample_config):
    extractor = DataExtractor(sample_config)
    assert extractor.config == sample_config
    assert Path('test_raw_data').exists()

def test_extract_from_csv(sample_config, sample_csv_data):
    # Create temporary CSV file
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as f:
        sample_csv_data.to_csv(f.name, index=False)
        temp_path = f.name
    
    try:
        extractor = DataExtractor(sample_config)
        df = extractor.extract_from_csv(temp_path)
        
        assert len(df) == 3
        assert list(df.columns) == ['id', 'name', 'price']
        assert df['price'].sum() == 450
    finally:
        Path(temp_path).unlink()

def test_extract_from_zip(sample_config, sample_csv_data):
    # Create temporary ZIP with CSV
    with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as zip_file:
        temp_zip = zip_file.name
    
    try:
        # Create ZIP file
        with zipfile.ZipFile(temp_zip, 'w') as zipf:
            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False, mode='w') as csv_file:
                sample_csv_data.to_csv(csv_file.name, index=False)
                zipf.write(csv_file.name, 'airbnb_data.csv')
                Path(csv_file.name).unlink()
        
        extractor = DataExtractor(sample_config)
        df = extractor.extract_from_zip(temp_zip)
        
        assert len(df) == 3
        assert 'id' in df.columns
        assert 'price' in df.columns
        
    finally:
        Path(temp_zip).unlink()
