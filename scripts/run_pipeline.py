#!/usr/bin/env python3
"""
Script para ejecutar el pipeline ETL
"""
import sys
import os
import subprocess
from pathlib import Path

# Añadir src al path
current_dir = Path(__file__).parent
src_dir = current_dir.parent / "src"
sys.path.insert(0, str(src_dir))

def buscar_archivos_zip():
    """Busca archivos ZIP en Google Drive"""
    print("\\n📂 Explorando Google Drive...")
    try:
        # Usar subprocess para ejecutar comandos de shell
        result = subprocess.run(
            ['find', '/content/drive/MyDrive', '-name', '*.zip', '-type', 'f'],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0 and result.stdout:
            files = result.stdout.strip().split('\\n')
            for file in files[:10]:  # Mostrar solo los primeros 10
                if file:
                    print(f"  • {file}")
        else:
            print("  No se encontraron archivos ZIP")
    except Exception as e:
        print(f"  Error al buscar archivos: {e}")

try:
    from src.pipeline import NYC_Airbnb_ETL
    print("✅ Módulos importados correctamente")
except Exception as e:
    print(f"❌ Error importando: {e}")
    sys.exit(1)

def main():
    # Montar Google Drive si estamos en Colab
    try:
        from google.colab import drive
        drive.mount('/content/drive', force_remount=True)
        print("✅ Google Drive montado")
    except ImportError:
        print("⚠️  No se pudo montar Google Drive (quizás no estamos en Colab)")
    
    # Ruta al archivo
    zip_path = "/content/drive/MyDrive/Datasets/ab_newyork.zip"
    print(f"\\n🔍 Buscando: {zip_path}")
    
    if not os.path.exists(zip_path):
        print(f"❌ ERROR: Archivo no encontrado")
        buscar_archivos_zip()
        sys.exit(1)
    
    print(f"✅ Archivo encontrado ({os.path.getsize(zip_path)/1024/1024:.2f} MB)")
    print("\\n🚀 Ejecutando pipeline...")
    
    try:
        pipeline = NYC_Airbnb_ETL()
        data, csv_path, parquet_path = pipeline.run(zip_path)
        
        print("\\n" + "="*60)
        print("✅ PIPELINE COMPLETADO EXITOSAMENTE")
        print("="*60)
        print(f"📊 Registros procesados: {len(data):,}")
        print(f"📁 CSV: {csv_path}")
        print(f"📁 Parquet: {parquet_path}")
        
        # Mostrar muestra
        print("\\n📄 Primeras filas:")
        print(data.head())
        
        # Resumen básico
        print("\\n📈 Resumen básico:")
        print(f"  • Columnas: {len(data.columns)}")
        print(f"  • Filas: {len(data)}")
        
        if 'price' in data.columns:
            print(f"  • Precio promedio: ${data['price'].mean():.2f}")
            print(f"  • Precio mínimo: ${data['price'].min():.2f}")
            print(f"  • Precio máximo: ${data['price'].max():.2f}")
        
    except Exception as e:
        print(f"\\n❌ Error en el pipeline: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
