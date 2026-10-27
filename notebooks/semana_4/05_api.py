# api.py
import os
import sys
from flask import Flask, request, jsonify

# =============================================================================
# --- CONFIGURACIÓN CRÍTICA DE ENTORNO PARA SPARK ---
# =============================================================================

# Especifica la ruta a tu ejecutable de Python.
# ¡¡¡DEBES AJUSTAR ESTA RUTA A TU PROPIA INSTALACIÓN!!!
# Puedes encontrarla ejecutando en tu terminal: where python
import sys as _sys
PYTHON_EXECUTABLE_PATH = _sys.executable  # autodetección (corregido)

if not os.path.exists(PYTHON_EXECUTABLE_PATH):
    raise FileNotFoundError(f"El ejecutable de Python no se encontró en: {PYTHON_EXECUTABLE_PATH}\n"
                            "Por favor, ajusta la variable PYTHON_EXECUTABLE_PATH en el script api.py.")

os.environ['PYSPARK_PYTHON'] = PYTHON_EXECUTABLE_PATH
os.environ['PYSPARK_DRIVER_PYTHON'] = PYTHON_EXECUTABLE_PATH

# Si Spark no está en el PATH del sistema, también puedes añadir esto:
# os.environ['SPARK_HOME'] = r'C:\BDP\spark'
# sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
# sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-0.10.9.7-src.zip'))


from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, expr, lit, sum as _sum
from pyspark.ml.classification import GBTClassificationModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import IntegerType, DoubleType, LongType, FloatType
from sedona.spark import SedonaContext
import pyproj

# =============================================================================
# 1. INICIALIZACIÓN GLOBAL DE LA API Y SPARK
# =============================================================================
print("--- INICIANDO SERVIDOR DE PREDICCIÓN ---")

app = Flask(__name__)

print("» Paso 1: Configurando la sesión de Spark (esto puede tardar)...")

HDFS_NAMENODE_HOST = "localhost" # Ajustado para un entorno local común
HDFS_NAMENODE_PORT = "9000"

spark = (
    SparkSession.builder.appName("API_PrediccionOXXO")
    .config("spark.driver.memory", "2g")  # BDP v7: recorte de un estado
    .config("spark.locality.wait", "0")
    # BDP v7: Sedona 1.9.1 ya viene pre-cargado en el laboratorio (sin descargas de Maven)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
    .config("spark.sql.extensions", "org.apache.sedona.sql.SedonaSqlExtensions")
    .config("spark.hadoop.fs.defaultFS", f"hdfs://{HDFS_NAMENODE_HOST}:{HDFS_NAMENODE_PORT}")
    .config("spark.sql.ansi.enabled", "false").getOrCreate()
)
sedona = SedonaContext.create(spark)
print("✅ Sesión de Spark iniciada.")

# --- Cargar todos los recursos necesarios a la memoria ---
print("\n» Paso 2: Cargando modelos y datos base a memoria...")

CENSO_PATH_HDFS = "/data/raw/geodatos_mexico/censo_2020_nacional.parquet"
DENUE_PATH_HDFS = "/data/raw/geodatos_mexico/denue_nacional.parquet"
MODEL_PATH_HDFS = "/models/gbt_oxxo_model"
FEATURES_LIST_PATH = "/models/gbt_oxxo_model_features"

# Cargar dataframes
g_censo_sdf = spark.read.format("geoparquet").load(CENSO_PATH_HDFS)
g_denue_raw_sdf = spark.read.format("geoparquet").load(DENUE_PATH_HDFS)

# Cargar modelo
g_gbt_model = GBTClassificationModel.load(MODEL_PATH_HDFS)

# Cargar lista de features
g_training_feature_cols = spark.read.text(FEATURES_LIST_PATH).rdd.map(lambda row: row.value).collect()

# Pre-procesar dataframes base
print("   Pre-procesando DENUE y Censo...")
g_denue_sdf = g_denue_raw_sdf.withColumn("codigo_act_2c", F.substring(col("codigo_act"), 1, 2)) \
    .withColumn("est_per_ocu",
        F.when(col("per_ocu").contains("0 a 5"), 2.5)
        .when(col("per_ocu").contains("6 a 10"), 8.0)
        .when(col("per_ocu").contains("11 a 30"), 20.5)
        .when(col("per_ocu").contains("31 a 50"), 40.5)
        .when(col("per_ocu").contains("51 a 100"), 75.5)
        .when(col("per_ocu").contains("101 a 250"), 175.5)
        .when(col("per_ocu").contains("251 y más"), 350.0)
        .otherwise(1.0)
    )
numeric_censo_cols = [f.name for f in g_censo_sdf.schema.fields if isinstance(f.dataType, (IntegerType, DoubleType, LongType, FloatType))]
g_censo_sdf = g_censo_sdf.na.fill(0, subset=numeric_censo_cols)
print("✅ Recursos cargados y listos.")


# =============================================================================
# 2. FUNCIONES HELPERS (sin cambios)
# =============================================================================
def aggregate_denue_features(base_df, denue_df, buffer_col_name, suffix):
    joined = base_df.join(denue_df.alias("d"), expr(f"ST_Intersects({buffer_col_name}, d.geometry)"))
    agg_epo = joined.groupBy(base_df.id).agg(_sum("d.est_per_ocu").alias(f"epo_{suffix}"))
    act_codes = ['51', '54', '11', '22', '52', '71', '43', '31', '61', '46', '23', '55', '93', '53', '81', '33', '48', '32', '56', '49', '62', '21', '72']
    agg_pivot = joined.groupBy(base_df.id).pivot("d.codigo_act_2c", act_codes).agg(_sum("d.est_per_ocu"))
    for code in act_codes:
        agg_pivot = agg_pivot.withColumnRenamed(code, f"act_{code}_{suffix}")
    return agg_epo.join(agg_pivot, "id", "outer")

def aggregate_censo_features(base_df, censo_df, buffer_col_name, suffix):
    joined = base_df.join(censo_df.alias("c"), expr(f"ST_Intersects({buffer_col_name}, c.geometry)"))
    numeric_censo_cols = [f.name for f in censo_df.schema.fields if isinstance(f.dataType, (IntegerType, DoubleType, LongType, FloatType))]
    agg_exprs = [_sum(f"c.{col}").alias(f"censo_{col}_{suffix}") for col in numeric_censo_cols]
    return joined.groupBy(base_df.id).agg(*agg_exprs)

def convertir_4326_a_6372(lon, lat):
    crs_origen = pyproj.CRS("EPSG:4326")
    crs_destino = pyproj.CRS("EPSG:6372")
    transformador = pyproj.Transformer.from_crs(crs_origen, crs_destino, always_xy=True)
    x_6372, y_6372 = transformador.transform(lon, lat)
    return (x_6372, y_6372)

# =============================================================================
# 3. ENDPOINT DE LA API (sin cambios)
# =============================================================================
@app.route("/predict", methods=["POST"])
def predict():
    """Endpoint que recibe coordenadas y devuelve una predicción."""
    data = request.get_json()
    if not data or 'latitude' not in data or 'longitude' not in data:
        return jsonify({"error": "Petición inválida. Se requiere 'latitude' y 'longitude' en el cuerpo JSON."}), 400

    lat_4326 = data['latitude']
    lon_4326 = data['longitude']

    try:
        print(f"Recibida petición para: Lat={lat_4326}, Lon={lon_4326}")
        
        x_proj, y_proj = convertir_4326_a_6372(lon_4326, lat_4326)
        point_df = spark.createDataFrame([(1, x_proj, y_proj)], ["id", "x", "y"]) \
                        .withColumn("geometry", expr("ST_Point(x, y)"))

        point_with_buffers = point_df.withColumn("buffer_100m", expr("ST_Buffer(geometry, 100)")).withColumn("buffer_500m", expr("ST_Buffer(geometry, 500)"))
        denue_agg_100 = aggregate_denue_features(point_with_buffers, g_denue_sdf, "buffer_100m", "100")
        denue_agg_500 = aggregate_denue_features(point_with_buffers, g_denue_sdf, "buffer_500m", "500")
        censo_agg_100 = aggregate_censo_features(point_with_buffers, g_censo_sdf, "buffer_100m", "100")
        censo_agg_500 = aggregate_censo_features(point_with_buffers, g_censo_sdf, "buffer_500m", "500")

        features_df = point_df.select("id") \
            .join(denue_agg_100, "id", "left_outer") \
            .join(denue_agg_500, "id", "left_outer") \
            .join(censo_agg_100, "id", "left_outer") \
            .join(censo_agg_500, "id", "left_outer") \
            .na.fill(0)
        
        existing_cols = features_df.columns
        for col_name in g_training_feature_cols:
            if col_name not in existing_cols:
                features_df = features_df.withColumn(col_name, lit(0))

        assembler = VectorAssembler(inputCols=g_training_feature_cols, outputCol="features")
        final_df = assembler.transform(features_df)
        prediction_result = g_gbt_model.transform(final_df)
        
        result_row = prediction_result.select("prediction", "probability").first()
        prediction_val = result_row['prediction']
        probability = result_row['probability']
        
        clase = "OXXO" if prediction_val == 1.0 else "Abarrotes"
        confianza = probability[int(prediction_val)]

        response = {
            "input_coordinates": {
                "latitude": lat_4326,
                "longitude": lon_4326
            },
            "prediction": clase,
            "confidence": round(confianza * 100, 2)
        }
        
        print(f"Predicción exitosa: {response}")
        return jsonify(response)

    except Exception as e:
        print(f"ERROR: Ha ocurrido un error procesando la petición: {e}")
        return jsonify({"error": "Error interno del servidor.", "details": str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001, debug=False)

# ============================================================================
# 💡 MEJORA FUTURA (nota para el alumno)
# Cada POST /predict recalcula las features espaciales contra el DENUE y el Censo
# NACIONAL -> por eso necesita ~6 GB de driver y tarda. En producción se PRE-CALCULAN
# las features de una malla/zona una sola vez (en HDFS o una BD) y /predict solo busca
# el punto más cercano: responde en milisegundos. Ese es el salto a un servicio que escala.
# ============================================================================
