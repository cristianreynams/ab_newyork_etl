#!/usr/bin/env python3
"""
Script simplificado para ejecutar en Google Colab
"""

import sys
import os

# Añadir src al path
sys.path.insert(0, '/content/ab_newyork_etl/src')

try:
    from pipeline import NYC_Airbnb_ETL
    print("✅ Módulos importados correctamente")
except ImportError as e:
    print(f"❌ Error importando módulos: {e}")
    print("💡 Ejecuta primero: !git clone https://github.com/cristianreynams/ab_newyork_etl.git")
    sys.exit(1)

def run_in_colab():
    """Función para ejecutar en Google Colab"""
    
    # Ruta al archivo en Google Drive
    zip_path = "/content/drive/MyDrive/Datasets/ab_newyork.zip"
    
    print("=" * 60)
    print("ETL PIPELINE - NYC AIRBNB (Google Colab)")
    print("=" * 60)
    print(f"Archivo fuente: {zip_path}")
    
    # Verificar que el archivo existe
    if not os.path.exists(zip_path):
        print(f"\n❌ Error: El archivo no existe: {zip_path}")
        print("\n💡 Soluciones:")
        print("1. Monta Google Drive:")
        print("   from google.colab import drive")
        print("   drive.mount('/content/drive')")
        print("\n2. Verifica la ruta correcta:")
        print("   !ls /content/drive/MyDrive/Datasets/")
        return
    
    print(f"✅ Archivo encontrado ({os.path.getsize(zip_path)/1024/1024:.2f} MB)")
    
    try:
        # Crear pipeline
        pipeline = NYC_Airbnb_ETL()
        
        # Ejecutar
        print("\n🚀 Iniciando procesamiento...")
        data = pipeline.run(zip_path)
        
        # Mostrar resultados
        print("\n" + "=" * 60)
        print("✅ PROCESAMIENTO COMPLETADO")
        print("=" * 60)
        print(f"📊 Total registros: {len(data):,}")
        print(f"📁 Datos guardados en: data/processed/")
        print("\n📄 Primeras 5 filas:")
        print(data.head())
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_in_colab()
