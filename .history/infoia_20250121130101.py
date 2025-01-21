import os
from google.cloud import bigquery
from google.cloud import aiplatform
from datetime import datetime, timedelta

# Configuración (Reemplaza con tus valores)
PROJECT_ID = "website-401719"
BQ_DATASET = "db_informacion"
BQ_INPUT_TABLE = "Info"
BQ_OUTPUT_TABLE = "info_detalle"
MODEL_NAME = "gemini-pro"
VERTEX_AI_LOCATION = "us-central1"

# Ruta CORRECTA al archivo JSON de credenciales de la cuenta de servicio
CREDENTIALS_FILE = r"C:\traProyectos\banano\iaInfo\credentials.json" # ¡Archivo .json!

# Establecer la variable de entorno para las credenciales
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_FILE

# Inicializar clientes
bq_client = bigquery.Client(project=PROJECT_ID)
aiplatform.init(project=PROJECT_ID, location=VERTEX_AI_LOCATION)

def verify_bigquery_resources():
    """Verifica la existencia y acceso a recursos de BigQuery"""
    try:
        # Verificar credenciales
        if not os.path.exists(CREDENTIALS_FILE):
            raise Exception(f"Archivo de credenciales no encontrado en: {CREDENTIALS_FILE}")
        
        # Verificar dataset
        try:
            bq_client.get_dataset(f"{PROJECT_ID}.{BQ_DATASET}")
        except Exception:
            raise Exception(f"Dataset '{BQ_DATASET}' no encontrado o sin acceso")
        
        # Verificar tabla de entrada
        try:
            bq_client.get_table(f"{PROJECT_ID}.{BQ_DATASET}.{BQ_INPUT_TABLE}")
        except Exception:
            raise Exception(f"Tabla '{BQ_INPUT_TABLE}' no encontrada o sin acceso")
        
        # Verificar tabla de salida
        try:
            bq_client.get_table(f"{PROJECT_ID}.{BQ_DATASET}.{BQ_OUTPUT_TABLE}")
        except Exception:
            print(f"ADVERTENCIA: Tabla '{BQ_OUTPUT_TABLE}' no encontrada. Creándola...")
            # Crear tabla si no existe
            schema = [
                bigquery.SchemaField("id_original", "STRING"),
                bigquery.SchemaField("titulo", "STRING"),
                bigquery.SchemaField("analisis", "STRING"),
            ]
            table = bigquery.Table(f"{PROJECT_ID}.{BQ_DATASET}.{BQ_OUTPUT_TABLE}", schema=schema)
            bq_client.create_table(table, exists_ok=True)
            
        return True
    except Exception as e:
        print(f"Error en la verificación: {str(e)}")
        return False

def analyze_banana_labels(event=None, context=None):
    try:
        if not verify_bigquery_resources():
            raise Exception("Falló la verificación de recursos de BigQuery")

        # Consulta simplificada para diagnóstico
        query = """
            SELECT *
            FROM `website-401719.db_informacion.Info`
            LIMIT 1
        """
        
        print("Ejecutando consulta de prueba...")
        query_job = bq_client.query(query)
        
        # Imprimir información de diagnóstico
        print("\nInformación de la tabla:")
        for row in query_job:
            for key, value in row.items():
                print(f"{key}: {value}")
            break

        # ... resto del código ...

# Esquema para la tabla info_detalle simplificada:
# id_original (STRING), titulo (STRING), analisis (STRING)

# Para crear la tabla info_detalle, ejecuta esta consulta en la consola de BigQuery:
"""
CREATE OR REPLACE TABLE `website-401719.db_informacion.info_detalle` (
  id_original STRING,
  titulo STRING,
  analisis STRING
);
"""

# Agregar este código para ejecutar localmente
if __name__ == "__main__":
    print("Iniciando análisis de etiquetas...")
    print("\nVerificando recursos de BigQuery...")
    try:
        analyze_banana_labels(None, None)
        print("\nAnálisis completado exitosamente.")
    except Exception as e:
        print(f"\nError durante la ejecución: {str(e)}")
        print("\nPasos para solucionar:")
        print("1. Verificar que el archivo de credenciales es válido y tiene los permisos necesarios")
        print("2. Confirmar que el proyecto, dataset y tablas existen y son accesibles")
        print("3. Revisar los permisos del usuario en BigQuery")
        print(f"4. Verificar que la tabla '{BQ_INPUT_TABLE}' existe y tiene datos")