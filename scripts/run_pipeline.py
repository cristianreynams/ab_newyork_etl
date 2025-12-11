#!/usr/bin/env python3
"""
Script simple para ejecutar el ETL de NYC Airbnb
"""
import sys
import os
import subprocess

# Intentar importar el pipeline
try:
    from simple_pipeline import ETLPipeline
    print("✅ Módulos importados correctamente")
except ImportError as e:
    print(f"❌ Error importando módulos: {e}")
    print("💡 Asegúrate de que simple_pipeline.py existe")
    sys.exit(1)

def main():
    # Ruta al archivo ZIP en Google Drive
    zip_path = "/content/drive/MyDrive/Datasets/ab_newyork.zip"
    
    print(f"🔍 Buscando archivo: {zip_path}")
    
    if not os.path.exists(zip_path):
        print(f"❌ ERROR: Archivo no encontrado: {zip_path}")
        print("\\n💡 Posibles soluciones:")
        print("1. Verifica que Google Drive está montado")
        print("2. Verifica la ruta exacta")
        print("3. Lista el contenido de Datasets:")
        
        # Usar subprocess en lugar de !
        try:
            result = subprocess.run(['ls', '-la', '/content/drive/MyDrive/Datasets/'], 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                print(result.stdout)
            else:
                print("   (No se pudo acceder a la carpeta)")
        except:
            print("   (Error al listar el directorio)")
        
        sys.exit(1)
    
    file_size = os.path.getsize(zip_path) / (1024 * 1024)
    print(f"✅ Archivo encontrado: {file_size:.2f} MB")
    print("\\n🚀 Iniciando pipeline ETL...")
    
    try:
        # Crear y ejecutar pipeline
        pipeline = ETLPipeline()
        data, summary = pipeline.run(zip_path)
        
        print("\\n" + "="*60)
        print("🎉 ¡ETL COMPLETADO CON ÉXITO!")
        print("="*60)
        print(f"📊 RESUMEN:")
        print(f"   • Filas originales: {summary['raw_rows']:,}")
        print(f"   • Filas procesadas: {summary['processed_rows']:,}")
        print(f"   • Columnas originales: {summary['raw_columns']}")
        print(f"   • Columnas procesadas: {summary['processed_columns']}")
        print(f"   • CSV generado: {summary['csv_path']}")
        if summary['parquet_path']:
            print(f"   • Parquet generado: {summary['parquet_path']}")
        
        print("\\n📈 ESTADÍSTICAS BÁSICAS:")
        print(f"   • Tipos de datos:")
        for dtype, count in data.dtypes.value_counts().items():
            print(f"      - {dtype}: {count}")
        
        if 'price' in data.columns:
            print(f"\\n   • Estadísticas de precio:")
            print(f"      - Mínimo: ${data['price'].min():.2f}")
            print(f"      - Máximo: ${data['price'].max():.2f}")
            print(f"      - Promedio: ${data['price'].mean():.2f}")
            print(f"      - Mediana: ${data['price'].median():.2f}")
        
        if 'room_type' in data.columns:
            print(f"\\n   • Distribución de tipos de habitación:")
            for room_type, count in data['room_type'].value_counts().items():
                print(f"      - {room_type}: {count}")
        
        # Mostrar primeras filas
        print("\\n📄 MUESTRA DE DATOS (primeras 5 filas):")
        print(data.head().to_string())
        
        # Guardar resumen en archivo
        with open('etl_summary.txt', 'w') as f:
            for key, value in summary.items():
                f.write(f"{key}: {value}\\n")
        print("\\n💾 Resumen guardado en: etl_summary.txt")
        
    except Exception as e:
        print(f"\\n❌ ERROR en el pipeline: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
