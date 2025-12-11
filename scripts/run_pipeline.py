#!/usr/bin/env python3
"""
Script to run the ETL pipeline from command line
"""
import argparse
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from pipeline import ETLPipeline

def main():
    parser = argparse.ArgumentParser(description='Run NYC Airbnb ETL Pipeline')
    parser.add_argument('--source', type=str, required=True,
                       help='Path to data source (ZIP or CSV file)')
    parser.add_argument('--source-type', type=str, choices=['zip', 'csv', 'auto'],
                       default='auto', help='Type of source file')
    parser.add_argument('--config', type=str, default='config.yaml',
                       help='Path to configuration file')
    parser.add_argument('--output-dir', type=str,
                       help='Override output directory')
    
    args = parser.parse_args()
    
    try:
        # Initialize and run pipeline
        pipeline = ETLPipeline(args.config)
        
        # Override output directory if specified
        if args.output_dir:
            pipeline.config['paths']['processed_data'] = args.output_dir
        
        # Run the pipeline
        result = pipeline.run(args.source, args.source_type)
        
        print("\n" + "="*50)
        print("ETL Pipeline completed successfully!")
        print(f"Processed {len(result)} rows")
        print(f"Output saved to: {pipeline.config['paths']['processed_data']}")
        print("="*50)
        
    except Exception as e:
        print(f"Error running pipeline: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
