import os
from google.cloud import bigquery
from vertexai.preview.generative_models import GenerativeModel
import vertexai
from datetime import datetime, timedelta
import time  # Agregar esta importación

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
        
        print("✓ Credenciales verificadas")
        
        # Verificar dataset
        try:
            dataset = bq_client.get_dataset(f"{PROJECT_ID}.{BQ_DATASET}")
            print(f"✓ Dataset '{BQ_DATASET}' encontrado")
        except Exception:
            raise Exception(f"Dataset '{BQ_DATASET}' no encontrado o sin acceso")
        
        # Verificar tabla de entrada
        try:
            input_table = bq_client.get_table(f"{PROJECT_ID}.{BQ_DATASET}.{BQ_INPUT_TABLE}")
            print(f"✓ Tabla de entrada '{BQ_INPUT_TABLE}' encontrada")
        except Exception:
            raise Exception(f"Tabla '{BQ_INPUT_TABLE}' no encontrada o sin acceso")
        
        # Crear o verificar tabla de salida
        table_id = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_OUTPUT_TABLE}"
        
        # Intentar eliminar la tabla si existe
        try:
            bq_client.delete_table(table_id)
            print(f"Tabla existente '{BQ_OUTPUT_TABLE}' eliminada")
        except Exception:
            pass  # La tabla no existía, lo cual está bien
        
        # Crear nueva tabla
        print(f"Creando tabla '{BQ_OUTPUT_TABLE}'...")
        schema = [
            bigquery.SchemaField("id_original", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("titulo", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("analisis", "STRING", mode="REQUIRED")
        ]
        
        table = bigquery.Table(table_id, schema=schema)
        table = bq_client.create_table(table, exists_ok=True)  # Cambiar a exists_ok=True
        print(f"✓ Tabla '{BQ_OUTPUT_TABLE}' creada exitosamente")
        
        # Esperar a que la tabla esté disponible
        print("Esperando a que la tabla esté disponible...")
        time.sleep(5)  # Esperar 5 segundos
        
        # Verificar que la tabla existe y está accesible
        try:
            bq_client.get_table(table_id)
            print("✓ Tabla verificada y lista para usar")
        except Exception as e:
            raise Exception(f"No se puede acceder a la tabla después de crearla: {str(e)}")
            
        return True
    except Exception as e:
        print(f"Error en la verificación: {str(e)}")
        return False

def insert_with_retry(table_ref, rows_to_insert, max_retries=3):
    """Función auxiliar para intentar insertar con reintentos"""
    for attempt in range(max_retries):
        try:
            errors = bq_client.insert_rows_json(table_ref, rows_to_insert)
            if not errors:
                return None
        except Exception as e:
            if attempt == max_retries - 1:
                raise e
            time.sleep(2 ** attempt)  # Espera exponencial
    return errors

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
            
            La escritura tiene que estar bien redactada, con coherencia y cohesión. Se debe utilizar un lenguaje técnico y profesional. NO SE COLOCA #, ##, ### o cualquier otro tipo de formato. SOLO POR ESPACIADO PARA SEPARAR PÁRRAFOS.

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
            try:
                errors = insert_with_retry(table_ref, [output_row])
                if errors:
                    print(f"Error al guardar análisis: {errors}")
                else:
                    print(f"✓ Análisis guardado para ID: {row['Id']}")
            except Exception as e:
                print(f"Error al intentar guardar el análisis: {str(e)}")
                raise e

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