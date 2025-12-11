#!/usr/bin/env python3
"""
Script principal para ejecutar el pipeline ETL
"""

import sys
import os
import argparse
from pathlib import Path

# Añadir el directorio src al path
current_dir = Path(__file__).parent
src_dir = current_dir.parent / "src"
sys.path.insert(0, str(src_dir))

def main():
    parser = argparse.ArgumentParser(description='ETL Pipeline para datos de Airbnb NYC')
    parser.add_argument(
        '--source', 
        type=str, 
        default='/content/drive/MyDrive/Datasets/ab_newyork.zip',
        help='Ruta al archivo ZIP con los datos (default: /content/drive/MyDrive/Datasets/ab_newyork.zip)'
    )
    parser.add_argument(
        '--output', 
        type=str, 
        default='data/processed',
        help='Directorio de salida para datos procesados (default: data/processed)'
    )
    parser.add_argument(
        '--log-level', 
        type=str, 
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Nivel de logging (default: INFO)'
    )
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("ETL PIPELINE - NYC AIRBNB DATA")
    print("=" * 70)
    print(f"Source: {args.source}")
    print(f"Output: {args.output}")
    print(f"Log level: {args.log_level}")
    print("=" * 70)
    
    try:
        # Importar después de configurar el path
        from pipeline import NYC_Airbnb_ETL
        
        # Configurar nivel de logging
        import logging
        logging.getLogger().setLevel(getattr(logging, args.log_level))
        
        # Crear y ejecutar pipeline
        pipeline = NYC_Airbnb_ETL()
        
        print("\n🚀 Iniciando pipeline...")
        processed_data = pipeline.run(args.source, args.output)
        
        # Mostrar resumen en consola
        print("\n" + "=" * 70)
        print("✅ PIPELINE COMPLETADO EXITOSAMENTE")
        print("=" * 70)
        print(f"📊 Datos procesados: {len(processed_data):,} filas, {len(processed_data.columns)} columnas")
        print(f"📁 Directorio de salida: {args.output}")
        
        # Listar archivos generados
        if os.path.exists(args.output):
            print("\n📄 Archivos generados:")
            for file in os.listdir(args.output):
                file_path = os.path.join(args.output, file)
                size = os.path.getsize(file_path) / 1024  # KB
                print(f"  • {file} ({size:.1f} KB)")
        
        # Mostrar información básica del dataset
        print("\n📈 Información del dataset:")
        print(f"  - Columnas: {list(processed_data.columns)}")
        
        if 'price' in processed_data.columns:
            print(f"  - Precio promedio: ${processed_data['price'].mean():.2f}")
            print(f"  - Rango de precios: ${processed_data['price'].min():.2f} - ${processed_data['price'].max():.2f}")
        
        if 'neighbourhood_group' in processed_data.columns:
            neighborhoods = processed_data['neighbourhood_group'].value_counts()
            print(f"  - Distribución por barrio:")
            for neighborhood, count in neighborhoods.head().items():
                print(f"      {neighborhood}: {count:,} ({count/len(processed_data)*100:.1f}%)")
        
        print("=" * 70)
        print("✨ Pipeline finalizado. Revisa el archivo etl_pipeline.log para más detalles.")
        print("=" * 70)
        
    except ImportError as e:
        print(f"\n❌ Error de importación: {e}")
        print("💡 Asegúrate de que:")
        print("   1. El archivo src/pipeline.py existe")
        print("   2. La clase NYC_Airbnb_ETL está definida en pipeline.py")
        sys.exit(1)
        
    except FileNotFoundError as e:
        print(f"\n❌ Archivo no encontrado: {e}")
        print("💡 Verifica que:")
        print("   1. Google Drive está montado (en Colab)")
        print("   2. La ruta al archivo ZIP es correcta")
        print("   3. El archivo existe en la ubicación especificada")
        sys.exit(1)
        
    except Exception as e:
        print(f"\n❌ Error durante la ejecución: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
