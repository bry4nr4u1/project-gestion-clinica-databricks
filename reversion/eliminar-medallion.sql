-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("nombre_container","unit-catalog-clinica")
-- MAGIC dbutils.widgets.text("nombre_storage","adlsbrscceu2d01")
-- MAGIC dbutils.widgets.text("catalogo","catalogo_clinica")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC nombre_container = dbutils.widgets.get("nombre_container")
-- MAGIC nombre_storage = dbutils.widgets.get("nombre_storage")
-- MAGIC catalogo = dbutils.widgets.get("catalogo")
-- MAGIC
-- MAGIC ruta = f"abfss://{nombre_container}@{nombre_storage}.dfs.core.windows.net"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Eliminacion tablas Bronze

-- COMMAND ----------

-- DROP TABLES
DROP TABLE IF EXISTS ${catalogo}.bronze.paciente;
DROP TABLE IF EXISTS ${catalogo}.bronze.medico;
DROP TABLE IF EXISTS ${catalogo}.bronze.medicamento;
DROP TABLE IF EXISTS ${catalogo}.bronze.cirugia;
DROP TABLE IF EXISTS ${catalogo}.bronze.consultas_medicas;
DROP TABLE IF EXISTS ${catalogo}.bronze.historial_pacientes;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## REMOVE DATA (Bronze)
-- MAGIC dbutils.fs.rm(f"{ruta}/bronze", True)
-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Eliminacion tablas Silver

-- COMMAND ----------

-- DROP TABLES
DROP TABLE IF EXISTS ${catalogo}.silver.paciente;
DROP TABLE IF EXISTS ${catalogo}.silver.medico;
DROP TABLE IF EXISTS ${catalogo}.silver.medicamento;
DROP TABLE IF EXISTS ${catalogo}.silver.cirugia;
DROP TABLE IF EXISTS ${catalogo}.silver.consultas_medicas;
DROP TABLE IF EXISTS ${catalogo}.silver.historial_pacientes_medicamentos;
DROP TABLE IF EXISTS ${catalogo}.silver.historial_pacientes_cirugias;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # REMOVE DATA (Silver)
-- MAGIC dbutils.fs.rm(f"{ruta}/silver", True)

-- MAGIC dbutils.fs.rm(f"{ruta}/silver/tablas/historial_pacientes_cirugias", True)
-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Eliminacion tablas Gold

-- COMMAND ----------

-- DROP TABLES
DROP TABLE IF EXISTS ${catalogo}.gold.paciente_perfil_clinico;
DROP TABLE IF EXISTS ${catalogo}.gold.consulta_por_medico;
DROP TABLE IF EXISTS ${catalogo}.gold.ingresos_por_especialidad;
DROP TABLE IF EXISTS ${catalogo}.gold.medicamentos_consumo;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # REMOVE DATA (Gold)
-- MAGIC dbutils.fs.rm(f"{ruta}/gold", True)

-- COMMAND ----------

-- Eliminar catálogo si existe
DROP CATALOG IF EXISTS ${catalogo} CASCADE;