# Databricks notebook source
# MAGIC %md
# MAGIC # Transformar datos clinica

# COMMAND ----------

# Eliminar widgets existentes
dbutils.widgets.removeAll()

# COMMAND ----------

# Importacion de librerias y funciones
from pyspark.sql.functions import explode_outer, col, trim, upper, lower, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget catalogo

# COMMAND ----------

# Crear widget para catálogo
dbutils.widgets.text("catalogo", "catalogo_clinica")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets tablas bronze

# COMMAND ----------

# Crear widget para esquema bronze
dbutils.widgets.text("bronze_schema", "bronze")

# COMMAND ----------

# Crear widget para tabla bronze - paciente
dbutils.widgets.text("bronze_pacientes", "paciente")

# COMMAND ----------

# Crear widget para tabla bronze - medico
dbutils.widgets.text("bronze_medicos", "medico")

# COMMAND ----------

# Crear widget para tabla bronze - medicamento
dbutils.widgets.text("bronze_medicamentos", "medicamento")

# COMMAND ----------

# Crear widget para tabla bronze - cirugia
dbutils.widgets.text("bronze_cirugias", "cirugia")

# COMMAND ----------

# Crear widget para tabla bronze - consultas medicas
dbutils.widgets.text("bronze_consultas_medicas", "consultas_medicas")

# COMMAND ----------

# Crear widget para tabla bronze - historial pacientes
dbutils.widgets.text("bronze_historial_paciente", "historial_pacientes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets tablas silver

# COMMAND ----------

# Crear widget para esquema silver
dbutils.widgets.text("silver_schema", "silver")

# COMMAND ----------

# Crear widget para tabla silver - paciente
dbutils.widgets.text("silver_pacientes", "paciente")

# COMMAND ----------

# Crear widget para tabla silver - medico
dbutils.widgets.text("silver_medicos", "medico")

# COMMAND ----------

# Crear widget para tabla silver - medicamento
dbutils.widgets.text("silver_medicamentos", "medicamento")

# COMMAND ----------

# Crear widget para tabla silver - cirugia
dbutils.widgets.text("silver_cirugias", "cirugia")

# COMMAND ----------

# Crear widget para tabla silver - consultas medicas
dbutils.widgets.text("silver_consultas_medicas", "consultas_medicas")

# COMMAND ----------

# Crear widget para tabla silver - historial pacientes medicamentos
dbutils.widgets.text("silver_historial_pacientes_medicamentos", "historial_pacientes_medicamentos")

# COMMAND ----------

# Crear widget para tabla silver - historial pacientes cirugias
dbutils.widgets.text("silver_historial_pacientes_cirugias", "historial_pacientes_cirugias")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Obtener valores de widgets

# COMMAND ----------

# Obtener valores de widgets
catalogo_name = dbutils.widgets.get("catalogo")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")

# Tablas - Pacientes
bronze_pacientes = dbutils.widgets.get("bronze_pacientes")
silver_pacientes = dbutils.widgets.get("silver_pacientes")

# Tablas - Medicos
bronze_medicos = dbutils.widgets.get("bronze_medicos")
silver_medicos = dbutils.widgets.get("silver_medicos")

# Tablas - Medicamentos
bronze_medicamentos = dbutils.widgets.get("bronze_medicamentos")
silver_medicamentos = dbutils.widgets.get("silver_medicamentos")

# Tablas - Cirugias
bronze_cirugias = dbutils.widgets.get("bronze_cirugias")
silver_cirugias = dbutils.widgets.get("silver_cirugias")

# Tablas - Consultas medicas
bronze_consultas_medicas = dbutils.widgets.get("bronze_consultas_medicas")
silver_consultas_medicas = dbutils.widgets.get("silver_consultas_medicas")

# Tablas - Historial pacientes
bronze_historial_paciente = dbutils.widgets.get("bronze_historial_paciente")
silver_historial_pacientes_medicamentos = dbutils.widgets.get("silver_historial_pacientes_medicamentos")
silver_historial_pacientes_cirugias = dbutils.widgets.get("silver_historial_pacientes_cirugias")

# COMMAND ----------

# Construir nombres completos de tablas

# Nombre completo tabla - pacientes
bronze_pacientes_table = f"{catalogo_name}.{bronze_schema}.{bronze_pacientes}"
silver_pacientes_table = f"{catalogo_name}.{silver_schema}.{silver_pacientes}"

# Nombre completo tabla - medicos
bronze_medicos_table = f"{catalogo_name}.{bronze_schema}.{bronze_medicos}"
silver_medicos_table = f"{catalogo_name}.{silver_schema}.{silver_medicos}"

# Nombre completo tabla - medicamentos
bronze_medicamentos_table = f"{catalogo_name}.{bronze_schema}.{bronze_medicamentos}"
silver_medicamentos_table = f"{catalogo_name}.{silver_schema}.{silver_medicamentos}"

# Nombre completo tabla - cirugias
bronze_cirugias_table = f"{catalogo_name}.{bronze_schema}.{bronze_cirugias}"
silver_cirugias_table = f"{catalogo_name}.{silver_schema}.{silver_cirugias}"

# Nombre completo tabla - consultas medicas
bronze_consultas_medicas_table = f"{catalogo_name}.{bronze_schema}.{bronze_consultas_medicas}"
silver_consultas_medicas_table = f"{catalogo_name}.{silver_schema}.{silver_consultas_medicas}"

# Nombre completo tabla - historial pacientes
bronze_historial_paciente_table = f"{catalogo_name}.{bronze_schema}.{bronze_historial_paciente}"
silver_historial_pacientes_medicamentos_table = f"{catalogo_name}.{silver_schema}.{silver_historial_pacientes_medicamentos}"
silver_historial_pacientes_cirugias_table = f"{catalogo_name}.{silver_schema}.{silver_historial_pacientes_cirugias}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformar tablas - Bronze a Silver

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformar pacientes

# COMMAND ----------

# Leer tabla bronze pacientes
df_bronze_pacientes = spark.read.format("delta").table(bronze_pacientes_table)

# COMMAND ----------

# Verificar datos de pacientes
df_bronze_pacientes.display()

# COMMAND ----------

# Transformar y limpiar pacientes
df_bronze_pacientes_transform = df_bronze_pacientes.select(
    col("identificacion"),
    trim(lower(col("nombre"))).alias("nombre"),
    trim(lower(col("apellido"))).alias("apellido"),
    col("fecha_nacimiento"),
    trim(upper(col("genero"))).alias("genero"),
    trim(lower(col("email"))).alias("email"),
    trim(col("telefono")).alias("telefono"),
    trim(col("ciudad")).alias("ciudad"),
    trim(col("pais")).alias("pais"),
    trim(upper(col("tipo_sangre"))).alias("tipo_sangre")
).withColumn("fecha_carga", current_timestamp())

# COMMAND ----------

# Verificar pacientes transformados
df_bronze_pacientes_transform.display()

# COMMAND ----------

# Escribir en silver
df_bronze_pacientes_transform.write.mode("overwrite").format("delta").insertInto(silver_pacientes_table)

# COMMAND ----------

# Verificar tabla pacientes en silver
spark.table(silver_pacientes_table).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformar médicos

# COMMAND ----------

# Leer tabla bronze medicos
df_bronze_medicos = spark.read.format("delta").table(bronze_medicos_table)

# COMMAND ----------

# Verificar datos de medicos
df_bronze_medicos.display()

# COMMAND ----------

# Transformar y limpiar medicos
df_bronze_medicos_transform = df_bronze_medicos.select(
    trim(upper(col("id_medico"))).alias("id_medico"),
    trim(lower(col("nombre"))).alias("nombre"),
    trim(lower(col("apellido"))).alias("apellido"),
    trim(col("especialidad")).alias("especialidad"),
    trim(upper(col("numero_licencia"))).alias("numero_licencia"),
    trim(col("hospital")).alias("hospital"),
    trim(col("ciudad")).alias("ciudad"),
    trim(col("telefono")).alias("telefono"),
    trim(lower(col("email"))).alias("email"),
    trim(col("estado")).alias("estado")
).withColumn("fecha_carga", current_timestamp())

# COMMAND ----------

# Verificar medicos transformados
df_bronze_medicos_transform.display()

# COMMAND ----------

# Escribir en tabla silver
df_bronze_medicos_transform.write.mode("overwrite").format("delta").insertInto(silver_medicos_table)

# COMMAND ----------

# Verificar tabla medico en silver
spark.table(silver_medicos_table).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformar medicamentos

# COMMAND ----------

# Leer tabla bronze medicamentos
df_bronze_medicamentos = spark.read.format("delta").table(bronze_medicamentos_table)

# COMMAND ----------

# Verificar datos de medicamentos
df_bronze_medicamentos.display()

# COMMAND ----------

# Transformar y limpiar medicamentos
df_bronze_medicamentos_transform = df_bronze_medicamentos.select(
    trim(upper(col("codigo_medicamento"))).alias("codigo_medicamento"),
    trim(col("nombre_medicamento")).alias("nombre_medicamento"),
    trim(col("principio_activo")).alias("principio_activo"),
    col("dosis"),
    trim(lower(col("unidad"))).alias("unidad"),
    trim(col("especialidad")).alias("especialidad"),
    trim(col("fabricante")).alias("fabricante"),
    col("precio_unitario"),
    trim(col("estado")).alias("estado")
).withColumn("fecha_carga", current_timestamp())

# COMMAND ----------

# Verificar medicamentos transformados
df_bronze_medicamentos_transform.display()

# COMMAND ----------

# Escribir en tabla silver
df_bronze_medicamentos_transform.write.mode("overwrite").format("delta").insertInto(silver_medicamentos_table)

# COMMAND ----------

# Verificar tabla medicamentos en silver
spark.table(silver_medicamentos_table).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformar cirugías

# COMMAND ----------

# Leer tabla bronze cirugias
df_bronze_cirugia = spark.read.format("delta").table(bronze_cirugias_table)

# COMMAND ----------

# Verificar datos de cirugias
df_bronze_cirugia.display()

# COMMAND ----------

# Transformar y limpiar cirugias
df_bronze_cirugia_transform = df_bronze_cirugia.select(
    trim(upper(col("codigo_cirugia"))).alias("codigo_cirugia"),
    trim(col("nombre_cirugia")).alias("nombre_cirugia"),
    trim(col("especialidad")).alias("especialidad"),
    col("duracion_promedio_minutos"),
    trim(col("complejidad")).alias("complejidad"),
    col("costo_base"),
    trim(col("riesgo_nivel")).alias("riesgo_nivel"),
    trim(col("estado")).alias("estado")
).withColumn("fecha_carga", current_timestamp())

# COMMAND ----------

# Verificar cirugias transformados
df_bronze_cirugia_transform.display()

# COMMAND ----------

# Escribir en silver
df_bronze_cirugia_transform.write.mode("overwrite").format("delta").insertInto(silver_cirugias_table)

# COMMAND ----------

# Verificar tabla cirugias en silver
spark.table(silver_cirugias_table).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformar consultas médicas

# COMMAND ----------

# Leer tabla bronze consultas medicas
df_bronze_consultas_medicas = spark.read.format("delta").table(bronze_consultas_medicas_table)

# COMMAND ----------

# Verificar datos de consultas medicas
df_bronze_consultas_medicas.display()

# COMMAND ----------

# Transformar y limpiar consultas medicas
df_bronze_consultas_medicas_transform = df_bronze_consultas_medicas.select(
    trim(upper(col("codigo_consulta"))).alias("codigo_consulta"),
    col("id_paciente"),
    col("fecha_consulta"),
    trim(upper(col("id_medico"))).alias("id_medico"),
    trim(col("especialidad")).alias("especialidad"),
    trim(col("motivo")).alias("motivo"),
    trim(col("diagnostico")).alias("diagnostico"),
    col("costo_consulta"),
    trim(col("estado")).alias("estado")
).withColumn("fecha_carga", current_timestamp())

# COMMAND ----------

# Verificar consultas medicas transformadas
df_bronze_consultas_medicas_transform.display()

# COMMAND ----------

# Escribir en silver
df_bronze_consultas_medicas_transform.write.mode("overwrite").format("delta").insertInto(silver_consultas_medicas_table)

# COMMAND ----------

# Verificar tabla consultas medicas en silver
spark.table(silver_consultas_medicas_table).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformar historial pacientes

# COMMAND ----------

# Leer tabla bronze
df_bronze_historial_pacientes = spark.read.format("delta").table(bronze_historial_paciente_table)

# COMMAND ----------

# Verificar datos tabla historial pacientes
df_bronze_historial_pacientes.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explotar array medicamentos

# COMMAND ----------

# Explosión y limpieza de medicamentos
df_bronze_historial_pacientes_medicamentos = df_bronze_historial_pacientes.withColumn("medicamento_obj", explode_outer(col("medicamentos"))) \
    .select(
        trim(col("identificacion")).alias("identificacion"),
        trim(lower(col("nombre"))).alias("nombre"),
        col("medicamento_obj").getField("fecha").alias("fecha"),
        trim(col("medicamento_obj").getField("codigo")).alias("codigo_medicamento")
    )

# COMMAND ----------

# Verificar datos de historial pacientes medicamentos
df_bronze_historial_pacientes_medicamentos.display()

# COMMAND ----------

# Agregar timestamp de carga al historial de pacientes de medicamentos
df_bronze_historial_pacientes_medicamentos_final = df_bronze_historial_pacientes_medicamentos.withColumn("fecha_carga", current_timestamp())

# COMMAND ----------

# Verificar historial de pacientes de medicamentos con fecha de carga 
df_bronze_historial_pacientes_medicamentos_final.display()

# COMMAND ----------

# Escribir tabla historial pacientes medicamentos en silver
df_bronze_historial_pacientes_medicamentos_final.write.mode("overwrite").format("delta").insertInto(silver_historial_pacientes_medicamentos_table)

# COMMAND ----------

# Verificar tabla historial pacientes medicamentos en silver
spark.table(silver_historial_pacientes_medicamentos_table).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explotar array cirugías

# COMMAND ----------

# Explosión y limpieza de cirugías
df_bronze_historial_pacientes_cirugias = df_bronze_historial_pacientes.withColumn("cirugia_obj", explode_outer(col("cirugias"))) \
    .select(
        trim(col("identificacion")).alias("identificacion"),
        trim(lower(col("nombre"))).alias("nombre"),
        col("cirugia_obj").getField("fecha").alias("fecha"),
        trim(col("cirugia_obj").getField("codigo")).alias("codigo_cirugia")
    )

# COMMAND ----------

# Verificar datos de historial pacientes cirugias
df_bronze_historial_pacientes_cirugias.display()

# COMMAND ----------

# Agregar timestamp de carga al historial de pacientes de cirugías
df_bronze_historial_pacientes_cirugias_final = df_bronze_historial_pacientes_cirugias.withColumn("fecha_carga", current_timestamp())

# COMMAND ----------

# Verificar historial de pacientes de cirugías con fecha de carga 
df_bronze_historial_pacientes_cirugias_final.display()

# COMMAND ----------

# Escribir tabla historial pacientes cirugías en silver
df_bronze_historial_pacientes_cirugias_final.write.mode("overwrite").format("delta").insertInto(silver_historial_pacientes_cirugias_table)

# COMMAND ----------

# Verificar tabla historial pacientes cirugías en silver
spark.table(silver_historial_pacientes_cirugias_table).display()