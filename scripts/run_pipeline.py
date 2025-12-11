#!/usr/bin/env python3
"""
Script to run the ETL pipeline
"""
import sys
import os
import subprocess
from pathlib import Path

# Add src to path
current_dir = Path(__file__).parent
src_dir = current_dir.parent / "src"
sys.path.insert(0, str(src_dir))

def search_zip_files():
    """Search for ZIP files in Google Drive"""
    print("\n📂 Searching Google Drive...")
    try:
        # Use subprocess to run shell commands
        result = subprocess.run(
            ['find', '/content/drive/MyDrive', '-name', '*.zip', '-type', 'f'],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0 and result.stdout:
            files = result.stdout.strip().split('\n')
            for file in files[:10]:  # Show only first 10
                if file:
                    print(f"  • {file}")
        else:
            print("  No ZIP files found")
    except Exception as e:
        print(f"  Error searching for files: {e}")

try:
    from src.pipeline import NYC_Airbnb_ETL
    print("✅ Modules imported correctly")
except Exception as e:
    print(f"❌ Error importing: {e}")
    sys.exit(1)

def main():
    # Mount Google Drive if we're in Colab
    try:
        from google.colab import drive
        drive.mount('/content/drive', force_remount=True)
        print("✅ Google Drive mounted")
    except ImportError:
        print("⚠️  Could not mount Google Drive (maybe not in Colab)")
    
    # File path
    zip_path = "/content/drive/MyDrive/Datasets/ab_newyork.zip"
    print(f"\n🔍 Searching for: {zip_path}")
    
    if not os.path.exists(zip_path):
        print(f"❌ ERROR: File not found")
        search_zip_files()
        sys.exit(1)
    
    print(f"✅ File found ({os.path.getsize(zip_path)/1024/1024:.2f} MB)")
    print("\n🚀 Running pipeline...")
    
    try:
        pipeline = NYC_Airbnb_ETL()
        data, csv_path, parquet_path = pipeline.run(zip_path)
        
        print("\n" + "="*60)
        print("✅ PIPELINE COMPLETED SUCCESSFULLY")
        print("="*60)
        print(f"📊 Records processed: {len(data):,}")
        print(f"📁 CSV: {csv_path}")
        print(f"📁 Parquet: {parquet_path}")
        
        # Show sample
        print("\n📄 First rows:")
        print(data.head())
        
        # Basic summary
        print("\n📈 Basic summary:")
        print(f"  • Columns: {len(data.columns)}")
        print(f"  • Rows: {len(data)}")
        
        if 'price' in data.columns:
            print(f"  • Average price: ${data['price'].mean():.2f}")
            print(f"  • Minimum price: ${data['price'].min():.2f}")
            print(f"  • Maximum price: ${data['price'].max():.2f}")
        
    except Exception as e:
        print(f"\n❌ Error in pipeline: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
