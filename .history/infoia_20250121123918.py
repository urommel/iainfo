import os
from google.cloud import bigquery
from google.cloud import aiplatform
from datetime import datetime, timedelta

# Configuración (Reemplaza con tus valores)
PROJECT_ID = "website-401719"
BQ_DATASET = "db_informacion"
BQ_INPUT_TABLE = "Info"
BQ_OUTPUT_TABLE = "info_detalle"
MODEL_NAME = "text-bison@002"  # o "gemini-protext-bison@002"  # Reemplaza con el modelo que vas a usar.
VERTEX_AI_LOCATION = "us-central1"  # Reemplaza con la región donde está el modelo

# Ruta al archivo JSON de credenciales de la cuenta de servicio
CREDENTIALS_FILE = "/ruta/a/tu/archivo/de/credenciales.json"

# Establecer la variable de entorno para las credenciales
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_FILE

# Inicializar clientes
bq_client = bigquery.Client(project=PROJECT_ID)
aiplatform.init(project=PROJECT_ID, location=VERTEX_AI_LOCATION)

def analyze_banana_labels(event, context):
    """
    Función de Cloud Functions que se activa al insertar datos en BigQuery.
    (También puedes adaptarla para ejecutarla localmente o con Cloud Scheduler)
    """
    try:
        # 1. Obtener los nuevos datos desde BigQuery (últimas 24 horas)
        yesterday = datetime.now() - timedelta(days=1)
        query = f"""
            SELECT Id, Titulo, Comentario
            FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_INPUT_TABLE}`
            WHERE DATE(Fecha) >= DATE('{yesterday.strftime('%Y-%m-%d')}')
        """
        query_job = bq_client.query(query)
        rows = query_job.result()

        for row in rows:
            # 2. Formatear datos y construir el prompt
            titulo = row['Titulo']
            comentario = row['Comentario']
            prompt = f"""
            Dado un [Titulo] y [Comentario] sobre una observación relacionada con las etiquetas de banano destinadas a la exportación a Japón, realizar un análisis exhaustivo y adaptativo que explore todos los aspectos posibles. El objetivo principal es identificar las causas del problema de legibilidad de las etiquetas por los lectores en Japón. Este análisis debe:

            1. Analizar el problema central: Usar el [Titulo] y [Comentario] proporcionados como punto de partida para identificar el problema central en relación con la lectura de las etiquetas en Japón.

            2. Explorar el impacto en la codificación de información: Investigar si el problema se debe al tipo de codificación utilizada (por ejemplo, tipo de código de barras, tamaño, resolución) en las etiquetas, y su compatibilidad con los sistemas de lectura en Japón.

            3. Investigar problemas relacionados al material/tinta: Analizar si el problema podría estar relacionado con el tipo de material utilizado en las etiquetas o la tinta de impresión y cómo esto podría afectar la calidad y durabilidad, y la lectura en Japon.

            4. Detectar problemas del proceso de impresión: Evaluar el proceso de impresión de las etiquetas, incluyendo la resolución de impresión, la calidad de la tinta, y los ajustes de la maquinaria, y si estos se adecuan a los estándares de Japon.

            5. Evaluar causas posibles: Explorar posibles razones para el problema, incluyendo diferencias en estándares de codificación, calidad de impresión, materiales, gestión con proveedores, o cualquier otro factor que pudiera contribuir a la falta de legibilidad en Japón.

            6. Proponer acciones de mejora: Sugerir acciones de mejora específicas y adaptadas al problema identificado, incluyendo ajustes en la codificación, estándares de impresión, selección de materiales, y la comunicación con los proveedores para garantizar la legibilidad en Japón.

            El análisis debe ser adaptativo y aplicar esta estructura a cualquier [Titulo] y [Comentario] proporcionado, sin hacer suposiciones previas sobre la naturaleza específica del problema. Considerar siempre el contexto de la evaluación de etiquetas de banano de dos proveedores distintos, destinadas a la exportación a Japón.

            [Titulo]: {titulo}
            [Comentario]: {comentario}
            """

            # 3. Enviar el prompt a Vertex AI
            model = aiplatform.GenerativeModel(model_name=MODEL_NAME)
            responses = model.generate_content(prompt)
            analysis = responses.text

            # 4. Almacenar los resultados en BigQuery (tabla info_detalle)
            # SIMPLIFICADO: Solo se guarda id_original, titulo y analisis
            output_row = {
                "id_original": row["Id"],  # ID de la tabla Info
                "titulo": titulo,
                "analisis": analysis
            }
            table_ref = bq_client.dataset(BQ_DATASET).table(BQ_OUTPUT_TABLE)
            errors = bq_client.insert_rows_json(table_ref, [output_row])
            if errors:
                print(f"Errores al insertar en BigQuery: {errors}")
            else:
                print(f"Análisis de ID {row['Id']} guardado en {BQ_OUTPUT_TABLE}")

    except Exception as e:
        print(f"Error general: {e}")

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

# analyze_banana_labels(None, None)