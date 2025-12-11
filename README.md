# NYC Airbnb Data ETL Pipeline

A complete ETL pipeline for New York City Airbnb Open Data, designed to run on Google Colab and locally.

## Features
- **Extract**: Load data from multiple sources (CSV, API, Google Drive)
- **Transform**: Clean, normalize, and engineer features from raw Airbnb data
- **Load**: Store processed data in various formats (CSV, Parquet, database)
- **Monitoring**: Logging and error handling throughout the pipeline

## Quick Start

### Google Colab
```python
# Clone the repository
!git clone https://github.com/cristianreynams/ab_newyork_etl.git
%cd ab_newyork_etl

# Install dependencies
!pip install -r requirements.txt

# Mount Google Drive
from google.colab import drive
drive.mount('/content/drive')

# Run the pipeline
!python scripts/run_pipeline.py --source /content/drive/MyDrive/Datasets/ab_newyork.zip
