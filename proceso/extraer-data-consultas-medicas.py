# Databricks notebook source
# MAGIC %md
# MAGIC # Extraer datos consultas medicas

# COMMAND ----------

# Eliminar widgets existentes
dbutils.widgets.removeAll()

# COMMAND ----------

# Importacion de librerias y funciones
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
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
dbutils.widgets.text("raw_file", "consultas_medicas.csv")

# COMMAND ----------

# Crear widget para esquema bronze
dbutils.widgets.text("bronze_schema", "bronze")

# COMMAND ----------

# Crear widget para tabla bronze
dbutils.widgets.text("bronze_table", "consultas_medicas")

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

# Definir el esquema del CSV consultas_medicas
consultas_schema = StructType([
    StructField("codigo_consulta", StringType(), False),
    StructField("id_paciente", IntegerType(), True),
    StructField("fecha_consulta", DateType(), True),
    StructField("id_medico", StringType(), True),
    StructField("especialidad", StringType(), True),
    StructField("motivo", StringType(), True),
    StructField("diagnostico", StringType(), True),
    StructField("costo_consulta", IntegerType(), True),
    StructField("estado", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leer Archivo CSV

# COMMAND ----------

# Leer el archivo CSV con el esquema definido
df_raw = spark.read.option("header", "true").schema(consultas_schema).csv(csv_path)

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