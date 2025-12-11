"""
ETL Pipeline para datos de Airbnb NYC
"""

import pandas as pd
import zipfile
import os
from pathlib import Path
import logging
from datetime import datetime

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('etl_pipeline.log')
    ]
)
logger = logging.getLogger(__name__)


class NYC_Airbnb_ETL:
    """
    Clase principal para el pipeline ETL de datos de Airbnb NYC
    """
    
    def __init__(self):
        """Inicializar el pipeline"""
        self.data = None
        self.processed_data = None
        logger.info("Pipeline ETL inicializado")
    
    def extract(self, zip_path):
        """
        Extrae datos del archivo ZIP
        
        Args:
            zip_path: Ruta al archivo ZIP
            
        Returns:
            DataFrame con los datos extraídos
        """
        logger.info(f"Extrayendo datos de: {zip_path}")
        
        try:
            # Verificar que el archivo existe
            if not os.path.exists(zip_path):
                raise FileNotFoundError(f"Archivo no encontrado: {zip_path}")
            
            # Abrir el archivo ZIP
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Listar archivos en el ZIP
                file_list = zip_ref.namelist()
                logger.info(f"Archivos en el ZIP: {file_list}")
                
                # Buscar archivos CSV
                csv_files = [f for f in file_list if f.lower().endswith('.csv')]
                
                if not csv_files:
                    raise ValueError("No se encontraron archivos CSV en el ZIP")
                
                # Tomar el primer archivo CSV
                csv_file = csv_files[0]
                logger.info(f"Procesando archivo: {csv_file}")
                
                # Extraer y leer el CSV
                with zip_ref.open(csv_file) as f:
                    df = pd.read_csv(f)
                
                logger.info(f"Datos extraídos exitosamente - Filas: {df.shape[0]}, Columnas: {df.shape[1]}")
                self.data = df
                return df
                
        except Exception as e:
            logger.error(f"Error en extracción: {str(e)}")
            raise
    
    def transform(self, df):
        """
        Transforma y limpia los datos
        
        Args:
            df: DataFrame con datos crudos
            
        Returns:
            DataFrame con datos transformados
        """
        logger.info("Iniciando transformación de datos...")
        
        # Hacer una copia para no modificar el original
        df_clean = df.copy()
        
        # 1. Limpiar nombres de columnas
        df_clean.columns = [
            col.strip().lower().replace(' ', '_').replace('-', '_')
            for col in df_clean.columns
        ]
        logger.info(f"Columnas después de limpieza: {list(df_clean.columns)}")
        
        # 2. Eliminar filas completamente vacías
        initial_rows = len(df_clean)
        df_clean = df_clean.dropna(how='all')
        logger.info(f"Filas eliminadas (completamente vacías): {initial_rows - len(df_clean)}")
        
        # 3. Eliminar duplicados
        df_clean = df_clean.drop_duplicates()
        logger.info(f"Filas después de eliminar duplicados: {len(df_clean)}")
        
        # 4. Manejar valores faltantes
        missing_percent = (df_clean.isnull().sum() / len(df_clean)) * 100
        logger.info("Porcentaje de valores faltantes por columna:")
        for col, percent in missing_percent.items():
            if percent > 0:
                logger.info(f"  {col}: {percent:.2f}%")
        
        # 5. Limpiar columna de precio
        if 'price' in df_clean.columns:
            logger.info("Limpiando columna 'price'...")
            # Convertir a string, eliminar símbolos y convertir a float
            df_clean['price'] = (
                df_clean['price']
                .astype(str)
                .str.replace(r'[\$,]', '', regex=True)
                .astype(float, errors='coerce')
            )
            
            # Filtrar precios válidos
            valid_prices = df_clean['price'].between(0, 10000)
            df_clean = df_clean[valid_prices]
            logger.info(f"Precios válidos (0-10000): {valid_prices.sum()} filas")
        
        # 6. Convertir fechas
        date_columns = ['last_review', 'host_since']
        for col in date_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
                logger.info(f"Convertida columna de fecha: {col}")
        
        # 7. Crear nuevas características
        if 'last_review' in df_clean.columns:
            # Días desde la última revisión
            df_clean['days_since_last_review'] = (
                datetime.now() - df_clean['last_review']
            ).dt.days
            df_clean['days_since_last_review'] = df_clean['days_since_last_review'].fillna(-1)
        
        if 'price' in df_clean.columns and 'minimum_nights' in df_clean.columns:
            # Precio por noche
            df_clean['price_per_night'] = df_clean['price'] / df_clean['minimum_nights'].clip(lower=1)
        
        if 'availability_365' in df_clean.columns:
            # Disponibilidad booleana
            df_clean['is_available'] = df_clean['availability_365'] > 0
        
        logger.info(f"Transformación completada - Filas: {df_clean.shape[0]}, Columnas: {df_clean.shape[1]}")
        self.processed_data = df_clean
        return df_clean
    
    def load(self, df, output_dir="data/processed"):
        """
        Guarda los datos procesados
        
        Args:
            df: DataFrame con datos procesados
            output_dir: Directorio de salida
            
        Returns:
            Tupla con rutas a los archivos guardados
        """
        logger.info(f"Guardando datos procesados en: {output_dir}")
        
        # Crear directorio si no existe
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Generar timestamp para nombres únicos
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Guardar como CSV
        csv_filename = f"nyc_airbnb_processed_{timestamp}.csv"
        csv_path = os.path.join(output_dir, csv_filename)
        df.to_csv(csv_path, index=False)
        logger.info(f"CSV guardado: {csv_path}")
        
        # Guardar como Parquet (más eficiente)
        parquet_filename = f"nyc_airbnb_processed_{timestamp}.parquet"
        parquet_path = os.path.join(output_dir, parquet_filename)
        df.to_parquet(parquet_path, index=False)
        logger.info(f"Parquet guardado: {parquet_path}")
        
        # Guardar también un archivo sin timestamp para referencia
        latest_csv = os.path.join(output_dir, "nyc_airbnb_latest.csv")
        df.to_csv(latest_csv, index=False)
        
        # Crear archivo de metadatos
        self._save_metadata(df, output_dir, timestamp)
        
        return csv_path, parquet_path
    
    def _save_metadata(self, df, output_dir, timestamp):
        """Guarda metadatos del procesamiento"""
        metadata = {
            'timestamp': timestamp,
            'rows': len(df),
            'columns': len(df.columns),
            'columns_list': list(df.columns),
            'data_types': dict(df.dtypes.astype(str))
        }
        
        metadata_path = os.path.join(output_dir, f"metadata_{timestamp}.txt")
        with open(metadata_path, 'w') as f:
            for key, value in metadata.items():
                f.write(f"{key}: {value}\n")
        
        logger.info(f"Metadatos guardados: {metadata_path}")
    
    def run(self, zip_path, output_dir="data/processed"):
        """
        Ejecuta el pipeline completo
        
        Args:
            zip_path: Ruta al archivo ZIP
            output_dir: Directorio de salida
            
        Returns:
            DataFrame con datos procesados
        """
        logger.info("=" * 60)
        logger.info("INICIANDO PIPELINE ETL - NYC AIRBNB")
        logger.info("=" * 60)
        
        try:
            # 1. EXTRACCIÓN
            raw_data = self.extract(zip_path)
            
            # 2. TRANSFORMACIÓN
            processed_data = self.transform(raw_data)
            
            # 3. CARGA
            csv_path, parquet_path = self.load(processed_data, output_dir)
            
            # 4. RESUMEN
            logger.info("=" * 60)
            logger.info("PIPELINE COMPLETADO EXITOSAMENTE")
            logger.info("=" * 60)
            logger.info(f"Resumen:")
            logger.info(f"  - Datos crudos: {raw_data.shape[0]} filas, {raw_data.shape[1]} columnas")
            logger.info(f"  - Datos procesados: {processed_data.shape[0]} filas, {processed_data.shape[1]} columnas")
            logger.info(f"  - Archivos generados:")
            logger.info(f"      • CSV: {csv_path}")
            logger.info(f"      • Parquet: {parquet_path}")
            logger.info("=" * 60)
            
            return processed_data
            
        except Exception as e:
            logger.error(f"Error en el pipeline: {str(e)}")
            raise
