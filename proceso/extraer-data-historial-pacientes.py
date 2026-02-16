# Databricks notebook source
# MAGIC %md
# MAGIC # Extraer datos historial pacientes

# COMMAND ----------

# Eliminar widgets existentes
dbutils.widgets.removeAll()

# COMMAND ----------

# Importacion de librerias y funciones
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, DateType
from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear widgets

# COMMAND ----------

# Crear widget para catálogo
dbutils.widgets.text("catalogo", "catalogo_clinica")

# COMMAND ----------

# Crear widget para Cosmos DB Account Name
dbutils.widgets.text("cosmos_account", "codbbrscceu2d01")

# COMMAND ----------

# Crear widget para Cosmos DB Scope
dbutils.widgets.text("cosmos_scope", "accessScopeforCosmosDB")

# COMMAND ----------

# Crear widget para Cosmos DB Secret
dbutils.widgets.text("cosmos_secret", "cosmosdbKey")

# COMMAND ----------

# Crear widget para Cosmos DB Database
dbutils.widgets.text("cosmos_database", "clinica")

# COMMAND ----------

# Crear widget para Cosmos DB Container
dbutils.widgets.text("cosmos_container", "raw")

# COMMAND ----------

# Crear widget para esquema bronze
dbutils.widgets.text("bronze_schema", "bronze")

# COMMAND ----------

# Crear widget para tabla bronze
dbutils.widgets.text("bronze_table", "historial_pacientes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Obtener valores de widgets

# COMMAND ----------

# Obtener valores de widgets
catalogo_name = dbutils.widgets.get("catalogo")
cosmos_account = dbutils.widgets.get("cosmos_account")
cosmos_scope = dbutils.widgets.get("cosmos_scope")
cosmos_secret = dbutils.widgets.get("cosmos_secret")
cosmos_key = dbutils.secrets.get(scope=f"{cosmos_scope}", key=f"{cosmos_secret}")
cosmos_database = dbutils.widgets.get("cosmos_database")
cosmos_container = dbutils.widgets.get("cosmos_container")
bronze_schema = dbutils.widgets.get("bronze_schema")
bronze_table = dbutils.widgets.get("bronze_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configurar conexión a Cosmos DB

# COMMAND ----------

# Configurar conexión a Cosmos DB
cosmos_endpoint = f"https://{cosmos_account}.documents.azure.com:443/"

# COMMAND ----------

# Construir nombre completo de la tabla bronze
bronze_table_name = f"{catalogo_name}.{bronze_schema}.{bronze_table}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leer datos desde Cosmos DB

# COMMAND ----------

# Leer datos desde Cosmos DB
df_raw = spark.read \
  .format("cosmos.oltp") \
  .option("spark.cosmos.accountEndpoint", cosmos_endpoint) \
  .option("spark.cosmos.accountKey", cosmos_key) \
  .option("spark.cosmos.database", cosmos_database) \
  .option("spark.cosmos.container", cosmos_container) \
  .option("spark.cosmos.read.inferSchema.enabled", "false") \
  .load()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformar datos

# COMMAND ----------

# Parsear JSON desde _rawBody
from pyspark.sql.functions import from_json

schema_historial = StructType([
    StructField("identificacion", IntegerType(), True),
    StructField("nombre", StringType(), True),
    StructField("medicamentos", ArrayType(StructType([
        StructField("codigo", StringType(), True),
        StructField("fecha", StringType(), True)
    ])), True),
    StructField("cirugias", ArrayType(StructType([
        StructField("codigo", StringType(), True),
        StructField("fecha", StringType(), True)
    ])), True)
])

df_raw_transformed = df_raw.select(
    from_json(col("_rawBody"), schema_historial).alias("rb")
).select(
    col("rb.identificacion"),
    col("rb.nombre"),
    col("rb.medicamentos"),
    col("rb.cirugias")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escribir en tabla bronze

# COMMAND ----------

# Escribir en tabla bronze
df_raw_transformed.write.mode("overwrite").format("delta").insertInto(bronze_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificar tabla bronze

# COMMAND ----------

# Verificar datos en tabla bronze
display(spark.table(bronze_table_name))