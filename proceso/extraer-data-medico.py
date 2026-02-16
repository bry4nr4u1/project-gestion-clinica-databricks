# Databricks notebook source
# MAGIC %md
# MAGIC # Extraer datos médicos

# COMMAND ----------

# Eliminar widgets existentes
dbutils.widgets.removeAll()

# COMMAND ----------

# Importacion de librerias y funciones
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear widgets

# COMMAND ----------

# Crear widget para catálogo
dbutils.widgets.text("catalogo", "catalogo_clinica")

# COMMAND ----------

# Crear widget para esquema raw (volumes)
dbutils.widgets.text("raw_datalake", "dtlkbrscceu2d01")

# COMMAND ----------

# Crear widget para volumen raw
dbutils.widgets.text("raw_container", "raw")

# COMMAND ----------

# Crear widget para nombre del archivo CSV
dbutils.widgets.text("raw_file", "medico.csv")

# COMMAND ----------

# Crear widget para esquema bronze
dbutils.widgets.text("bronze_schema", "bronze")

# COMMAND ----------

# Crear widget para tabla bronze
dbutils.widgets.text("bronze_table", "medico")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Obtener valores de widgets

# COMMAND ----------

# Obtener valores de widgets
catalogo = dbutils.widgets.get("catalogo")
raw_datalake = dbutils.widgets.get("raw_datalake")
raw_container = dbutils.widgets.get("raw_container")
raw_file = dbutils.widgets.get("raw_file")
bronze_schema = dbutils.widgets.get("bronze_schema")
bronze_table = dbutils.widgets.get("bronze_table")

# COMMAND ----------

# Construir ruta completa del archivo CSV
csv_path = f"abfss://{raw_container}@{raw_datalake}.dfs.core.windows.net/{raw_file}"

print(f"Ruta Archivo: {csv_path}")

# COMMAND ----------

# Construir nombre completo de la tabla bronze
bronze_table_name = f"{catalogo}.{bronze_schema}.{bronze_table}"
print(f"Nombre Tabla Bronze: {bronze_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definir esquema del archivo CSV

# COMMAND ----------

# Definir el esquema del CSV medicos_profesionales
medicos_schema = StructType([
    StructField("id_medico", StringType(), False),
    StructField("nombre", StringType(), True),
    StructField("apellido", StringType(), True),
    StructField("especialidad", StringType(), True),
    StructField("numero_licencia", StringType(), True),
    StructField("hospital", StringType(), True),
    StructField("ciudad", StringType(), True),
    StructField("telefono", StringType(), True),
    StructField("email", StringType(), True),
    StructField("estado", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leer Archivo CSV

# COMMAND ----------

# Leer el archivo CSV con el esquema definido
df_raw = spark.read.option("header", "true").schema(medicos_schema).csv(csv_path)

# COMMAND ----------

# Verificar datos leídos
df_raw.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escribir en tabla bronze

# COMMAND ----------

# Escribir en tabla bronze
df_raw.write.mode("overwrite").format("delta").insertInto(bronze_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificar tabla bronze

# COMMAND ----------

# Verificar datos en tabla bronze
spark.table(bronze_table_name).display()