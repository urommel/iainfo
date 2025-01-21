import os
from google.cloud import bigquery
from vertexai.preview.generative_models import GenerativeModel
import vertexai
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
vertexai.init(project=PROJECT_ID, location=VERTEX_AI_LOCATION)

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
            # Crear tabla si no existe con campo analisis de tamaño máximo
            schema = [
                bigquery.SchemaField("id_original", "STRING"),
                bigquery.SchemaField("titulo", "STRING"),
                bigquery.SchemaField("analisis", "STRING", max_length="1048576")  # Máximo tamaño permitido
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

        # Consulta simple para obtener todos los registros
        query = """
            SELECT Id, Titulo, Comentario
            FROM `website-401719.db_informacion.Info`
        """
        
        print("Consultando registros de la tabla Info...")
        query_job = bq_client.query(query)
        rows = list(query_job.result())
        
        print(f"Se encontraron {len(rows)} registros para analizar")

        # Inicializar el modelo Gemini correctamente
        model = GenerativeModel("gemini-pro")

        for row in rows:
            titulo = row['Titulo']
            comentario = row['Comentario']
            print(f"\nAnalizando registro con título: {titulo}")

            # Construir el prompt
            prompt = f"""
            Dado un [Titulo] y [Comentario] sobre una observación relacionada con las etiquetas de banano destinadas a la exportación a Japón, realizar un análisis exhaustivo y adaptativo que explore todos los aspectos posibles. El objetivo principal es identificar las causas del problema de legibilidad de las etiquetas por los lectores en Japón. Este análisis debe:

            1. Analizar el problema central
            2. Explorar el impacto en la codificación de información
            3. Investigar problemas relacionados al material/tinta
            4. Detectar problemas del proceso de impresión
            5. Evaluar causas posibles
            6. Proponer acciones de mejora
            
            La escritura tiene que estar bien redactada,

            [Titulo]: {titulo} 
            [Comentario]: {comentario}
            """

            # Generar análisis con Gemini y manejar la respuesta
            response = model.generate_content(prompt)
            if response.candidates:
                analysis = response.candidates[0].text
            else:
                analysis = response.text

            # Guardar en info_detalle
            output_row = {
                "id_original": str(row["Id"]),
                "titulo": titulo,
                "analisis": analysis
            }
            
            table_ref = bq_client.dataset(BQ_DATASET).table(BQ_OUTPUT_TABLE)
            errors = bq_client.insert_rows_json(table_ref, [output_row])
            
            if errors:
                print(f"Error al guardar análisis: {errors}")
            else:
                print(f"✓ Análisis guardado para ID: {row['Id']}")

    except Exception as e:
        print(f"Error general: {str(e)}")
        raise e

# Esquema para la tabla info_detalle simplificada:
# id_original (STRING), titulo (STRING), analisis (STRING)

# Para crear la tabla info_detalle, ejecuta esta consulta en la consola de BigQuery:
"""
CREATE OR REPLACE TABLE `website-401719.db_informacion.info_detalle` (
  id_original STRING,
  titulo STRING,
  analisis STRING(MAX)  -- Usar STRING(MAX) para máxima capacidad
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