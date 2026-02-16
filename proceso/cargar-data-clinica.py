# Databricks notebook source
# MAGIC %md
# MAGIC # Cargar datos clinica

# COMMAND ----------

# Eliminar widgets existentes
dbutils.widgets.removeAll()

# COMMAND ----------

# Importacion de librerias y funciones
from pyspark.sql.functions import count, current_timestamp, coalesce, lit, col, sum, avg, countDistinct, round, collect_set, max, greatest

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widget Catálogo

# COMMAND ----------

# Crear widget para catálogo
dbutils.widgets.text("catalogo", "catalogo_clinica")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets tablas silver

# COMMAND ----------

# Crear widget para esquema silver
dbutils.widgets.text("silver_schema", "silver")

# COMMAND ----------

# Crear widget para primera tabla silver (historial medicamentos)
dbutils.widgets.text("silver_hist_pac_med_table", "historial_pacientes_medicamentos")

# COMMAND ----------

# Crear widget para segunda tabla silver (historial cirugias)
dbutils.widgets.text("silver_hist_pac_cir_table", "historial_pacientes_cirugias")

# COMMAND ----------

# Crear widget para tercera tabla silver (pacientes)
dbutils.widgets.text("silver_paciente_table", "paciente")

# COMMAND ----------

# Crear widget para cuarta tabla silver (medicos)
dbutils.widgets.text("silver_medico_table", "medico")

# COMMAND ----------

# Crear widget para quinta tabla silver (consultas)
dbutils.widgets.text("silver_consultas_medicas_table", "consultas_medicas")

# COMMAND ----------

# Crear widget para sexta tabla silver (medicamentos catalogoo)
dbutils.widgets.text("silver_medicamento_table", "medicamento")

# COMMAND ----------

# Crear widget para septima tabla silver (cirugias catalogoo)
dbutils.widgets.text("silver_cirugia_table", "cirugia")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets tablas gold

# COMMAND ----------

# Crear widget para esquema gold
dbutils.widgets.text("gold_schema", "gold")

# COMMAND ----------

# Crear widget para primera tabla gold (perfil clinico)
dbutils.widgets.text("gold_pac_per_cli_table", "paciente_perfil_clinico")

# COMMAND ----------

# Crear widget para segunda tabla gold (consulta por medico)
dbutils.widgets.text("gold_consulta_por_medico_table", "consulta_por_medico")

# COMMAND ----------

# Crear widget para tercera tabla gold (ingresos especialidad)
dbutils.widgets.text("gold_ingreso_por_especialidad_table", "ingresos_por_especialidad")

# COMMAND ----------

# Crear widget para cuarta tabla gold (medicamentos consumo)
dbutils.widgets.text("gold_medicamentos_consumo_table", "medicamentos_consumo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Obtener valores de widgets

# COMMAND ----------

# Obtener valores de widgets - Catálogo
catalogo_name = dbutils.widgets.get("catalogo")

# COMMAND ----------

# Obtener valores de widgets - Capa silver
silver_schema = dbutils.widgets.get("silver_schema")
silver_hist_pac_med_table = dbutils.widgets.get("silver_hist_pac_med_table")
silver_hist_pac_cir_table = dbutils.widgets.get("silver_hist_pac_cir_table")
silver_paciente_table = dbutils.widgets.get("silver_paciente_table")
silver_medico_table = dbutils.widgets.get("silver_medico_table")
silver_consultas_medicas_table = dbutils.widgets.get("silver_consultas_medicas_table")
silver_medicamento_table = dbutils.widgets.get("silver_medicamento_table")
silver_cirugia_table = dbutils.widgets.get("silver_cirugia_table")

# COMMAND ----------

# Obtener valores de widgets - Capa Gold
gold_schema = dbutils.widgets.get("gold_schema")
gold_pac_per_cli_table = dbutils.widgets.get("gold_pac_per_cli_table")
gold_consulta_por_medico_table = dbutils.widgets.get("gold_consulta_por_medico_table")
gold_ingreso_por_especialidad_table = dbutils.widgets.get("gold_ingreso_por_especialidad_table")
gold_medicamentos_consumo_table = dbutils.widgets.get("gold_medicamentos_consumo_table")

# COMMAND ----------

# Construir nombres completos de tablas silver
silver_hist_pac_med_table_name = f"{catalogo_name}.{silver_schema}.{silver_hist_pac_med_table}"
silver_hist_pac_cir_table_name = f"{catalogo_name}.{silver_schema}.{silver_hist_pac_cir_table}"
silver_paciente_table_name = f"{catalogo_name}.{silver_schema}.{silver_paciente_table}"
silver_medico_table_name = f"{catalogo_name}.{silver_schema}.{silver_medico_table}"
silver_consultas_medicas_table_name = f"{catalogo_name}.{silver_schema}.{silver_consultas_medicas_table}"
silver_medicamento_table_name = f"{catalogo_name}.{silver_schema}.{silver_medicamento_table}"
silver_cirugia_table_name = f"{catalogo_name}.{silver_schema}.{silver_cirugia_table}"

# COMMAND ----------

# Construir nombres completos de tablas Gold
gold_pac_per_cli_table_name = f"{catalogo_name}.{gold_schema}.{gold_pac_per_cli_table}"
gold_consulta_por_medico_table_name = f"{catalogo_name}.{gold_schema}.{gold_consulta_por_medico_table}"
gold_ingreso_por_especialidad_table_name = f"{catalogo_name}.{gold_schema}.{gold_ingreso_por_especialidad_table}"
gold_medicamentos_consumo_table_name = f"{catalogo_name}.{gold_schema}.{gold_medicamentos_consumo_table}"

# COMMAND ----------

# MAGIC %md
# MAGIC # Tabla Silver - Lectura de tablas

# COMMAND ----------

# Lectura tablas silver - historial pacientes medicamentos
df_silver_hist_pac_med = spark.read.format("delta").table(silver_hist_pac_med_table_name)

# COMMAND ----------

# Verificar datos de historial de pacientes medicamentos
df_silver_hist_pac_med.display()

# COMMAND ----------

# Lectura tablas silver - historial pacientes cirugías
df_silver_hist_pac_cir = spark.read.format("delta").table(silver_hist_pac_cir_table_name)

# COMMAND ----------

# Verificar datos de historial de pacientes cirugías
df_silver_hist_pac_cir.display()

# COMMAND ----------

# Lectura tablas silver - medicamentos
df_silver_medicamento = spark.read.format("delta").table(silver_medicamento_table_name)

# COMMAND ----------

# Verificar datos de medicamentos
df_silver_medicamento.display()

# COMMAND ----------

# Lectura tablas silver - cirugías
df_silver_cirugia = spark.read.format("delta").table(silver_cirugia_table_name)

# COMMAND ----------

# Verificar datos de cirugías
df_silver_cirugia.display()

# COMMAND ----------

# Lectura tablas silver - pacientes
df_pacientes = spark.read.format("delta").table(silver_paciente_table_name)

# COMMAND ----------

# Verificar datos de pacientes
df_pacientes.display()

# COMMAND ----------

# Lectura tablas silver - consultas médicas
df_consultas_medicas = spark.read.format("delta").table(silver_consultas_medicas_table_name)

# COMMAND ----------

# Verificar datos de consultas médicas
df_consultas_medicas.display()

# COMMAND ----------

# Lectura tablas silver - médicos
df_medicos = spark.read.format("delta").table(silver_medico_table_name)

# COMMAND ----------

# Verificar datos de médicos
df_medicos.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Tabla Gold 1: Perfil clínico del paciente

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agrupar Medicamentos

# COMMAND ----------

# Agregar conteo de medicamentos por paciente
df_medicamentos_agrupado = df_silver_hist_pac_med.groupBy("identificacion") \
    .agg(count("codigo_medicamento").alias("total_medicamentos"))

# COMMAND ----------

# Verificar agregación de medicamentos
df_medicamentos_agrupado.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agrupar Cirugias

# COMMAND ----------

# Agregar conteo de cirugías por paciente
df_cirugias_agrupado = df_silver_hist_pac_cir.groupBy("identificacion") \
    .agg(count("codigo_cirugia").alias("total_cirugias"))

# COMMAND ----------

# Verificar agregación de cirugías
df_cirugias_agrupado.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Obtener especialidades tratadas

# COMMAND ----------

# Obtener especialidades únicas por paciente desde medicamentos
df_esp_pac_med = df_silver_hist_pac_med.alias("h").join(
    df_silver_medicamento.alias("m"),
    col("h.codigo_medicamento") == col("m.codigo_medicamento"),
    "inner"
).select(
    col("h.identificacion"),
    col("m.especialidad")
).distinct()

# COMMAND ----------

# Obtener especialidades únicas por paciente desde cirugías
df_esp_pac_cir = df_silver_hist_pac_cir.alias("h").join(
    df_silver_cirugia.alias("c"),
    col("h.codigo_cirugia") == col("c.codigo_cirugia"),
    "inner"
).select(
    col("h.identificacion"),
    col("c.especialidad")
).distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Obtener lista de especialidades tratadas por paciente

# COMMAND ----------

# Unir especialidades de medicamentos y cirugías
df_especialidades = df_esp_pac_med.union(df_esp_pac_cir) \
    .groupBy("identificacion") \
    .agg(collect_set("especialidad").alias("especialidades_tratadas"))

# COMMAND ----------

df_especialidades.display()

# COMMAND ----------

# Obtener fecha más reciente de medicamentos
df_fecha_reciente_med = df_silver_hist_pac_med.groupBy("identificacion") \
    .agg(max("fecha").alias("fecha_ultimo_med"))

# COMMAND ----------

# Obtener fecha más reciente de cirugías
df_fecha_reciente_cir = df_silver_hist_pac_cir.groupBy("identificacion") \
    .agg(max("fecha").alias("fecha_ultimo_cir"))

# COMMAND ----------

# Combinar fechas y obtener la máxima fecha por paciente
df_fecha_ultimo = df_fecha_reciente_med.join(df_fecha_reciente_cir, "identificacion", "outer") \
    .select(
        col("identificacion"),
        coalesce(
            greatest(col("fecha_ultimo_med"), col("fecha_ultimo_cir")),
            col("fecha_ultimo_med"),
            col("fecha_ultimo_cir")
        ).alias("fecha_ultimo_evento")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unir agregaciones con pacientes

# COMMAND ----------

# Unir todas las agregaciones
df_gold_paciente_perfil_clinico = df_pacientes.alias("p") \
    .join(df_medicamentos_agrupado.alias("m"), col("p.identificacion") == col("m.identificacion"), "left") \
    .join(df_cirugias_agrupado.alias("c"), col("p.identificacion") == col("c.identificacion"), "left") \
    .join(df_especialidades.alias("e"), col("p.identificacion") == col("e.identificacion"), "left") \
    .join(df_fecha_ultimo.alias("f"), col("p.identificacion") == col("f.identificacion"), "left") \
    .select(
        col("p.identificacion"),
        col("p.nombre"),
        col("p.apellido"),
        col("p.fecha_nacimiento"),
        col("p.genero"),
        col("p.tipo_sangre"),
        coalesce(col("m.total_medicamentos"), lit(0)).alias("total_medicamentos"),
        coalesce(col("c.total_cirugias"), lit(0)).alias("total_cirugias"),
        coalesce(col("e.especialidades_tratadas"), lit(None)).alias("especialidades_tratadas"),
        col("f.fecha_ultimo_evento")
    ).withColumn("fecha_carga", current_timestamp())

# COMMAND ----------

# Verificar resultado del join
df_gold_paciente_perfil_clinico.display()

# COMMAND ----------

# Escribir en tabla Gold
df_gold_paciente_perfil_clinico.write.mode("overwrite").format("delta").insertInto(gold_pac_per_cli_table_name)

# COMMAND ---------

# Verificar tabla Gold
spark.table(gold_pac_per_cli_table_name).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Tabla Gold 2: Consulta por médico

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregar consultas por médico

# COMMAND ----------

# Obtener agrupamiento por médico
df_consultas_medicas_agrupada = df_consultas_medicas.groupBy("id_medico").agg(
    count("codigo_consulta").alias("total_consultas"),
    countDistinct("id_paciente").alias("pacientes_unicos"),
    sum("costo_consulta").alias("total_ingresos"),
    avg("costo_consulta").alias("costo_promedio"),
    round(avg("costo_consulta"), 2).alias("costo_promedio_redondeado")
)

# COMMAND ----------

# Verificar resultado de agrupamiento
df_consultas_medicas_agrupada.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unir consultas médicas con información del médico

# COMMAND ----------

# Unir consultas médicas con información del médico
df_gold_consulta_por_medico = df_medicos.alias("m").join(
    df_consultas_medicas_agrupada.alias("c"),
    col("m.id_medico") == col("c.id_medico"),
    "inner"
).select(
    col("m.id_medico"),
    col("m.nombre").alias("nombre_medico"),
    col("m.apellido").alias("apellido_medico"),
    col("m.especialidad"),
    col("m.hospital"),
    col("c.total_consultas"),
    col("c.total_ingresos"),
    col("c.pacientes_unicos"),
    col("c.costo_promedio_redondeado").alias("costo_promedio")
).withColumn("fecha_carga", current_timestamp())

# COMMAND ----------

# Verificar resultado del join
df_gold_consulta_por_medico.display()

# COMMAND ----------

# Escribir en tabla Gold
df_gold_consulta_por_medico.write.mode("overwrite").format("delta").insertInto(gold_consulta_por_medico_table_name)

# COMMAND ----------

# Verificar tabla Gold
spark.table(gold_consulta_por_medico_table_name).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Tabla Gold 3: Ingresos por especialidad

# COMMAND ----------

# Agregar ingresos por especialidad
df_gold_ingresos_por_especialidad = df_consultas_medicas.groupBy("especialidad").agg(
    count("codigo_consulta").alias("total_consultas"),
    sum("costo_consulta").alias("costo_total_consultas"),
    round(avg("costo_consulta"), 2).alias("costo_promedio_consulta"),
    countDistinct("id_paciente").alias("pacientes_unicos"),
    countDistinct("id_medico").alias("medicos_participantes")
).withColumn("fecha_carga", current_timestamp()) \
.orderBy(col("costo_total_consultas").desc())

# COMMAND ----------

df_gold_ingresos_por_especialidad.display()

# COMMAND ----------

# Escribir en Gold
df_gold_ingresos_por_especialidad.write.mode("overwrite").format("delta").insertInto(gold_ingreso_por_especialidad_table_name)

# COMMAND ----------

# Verificar tabla Gold
spark.table(gold_ingreso_por_especialidad_table_name).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Tabla Gold 4: Medicamentos consumidos

# COMMAND ----------

# Agrupar medicamentos consumidos
df_medicamentos_consumidos = df_silver_hist_pac_med.groupBy("codigo_medicamento").agg(
    count("codigo_medicamento").alias("veces_prescrito"),
    countDistinct("identificacion").alias("total_pacientes")
)

# COMMAND ----------

# Verificar resultado de agrupamiento
df_medicamentos_consumidos.display()

# COMMAND ----------

# Obtener información de medicamentos consumidos
df_gold_medicamento_consumidos = df_medicamentos_consumidos.alias("c").join(
    df_silver_medicamento.alias("m"),
    col("c.codigo_medicamento") == col("m.codigo_medicamento"),
    "inner"
).select(
    col("m.codigo_medicamento"),
    col("m.nombre_medicamento"),
    col("m.especialidad"),
    col("m.fabricante"),
    col("c.total_pacientes"),
    col("c.veces_prescrito").alias("total_unidades_consumidas"),
    col("m.precio_unitario").alias("costo_unitario"),
    (col("c.veces_prescrito") * col("m.precio_unitario")).alias("costo_total")
).withColumn("fecha_carga", current_timestamp()) \
.orderBy(col("total_unidades_consumidas").desc())

# COMMAND ----------

# Verificar resultado agrupamiento
df_gold_medicamento_consumidos.display()

# COMMAND ----------

# Escribir en Gold
df_gold_medicamento_consumidos.write.mode("overwrite").format("delta").insertInto(gold_medicamentos_consumo_table_name)

# COMMAND ----------

# Verificar tabla Gold
spark.table(gold_medicamentos_consumo_table_name).display()