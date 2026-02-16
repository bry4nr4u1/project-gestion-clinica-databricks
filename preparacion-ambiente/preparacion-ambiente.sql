-- Databricks notebook source
-- MAGIC %python
-- MAGIC # Eliminar widgets
-- MAGIC dbutils.widgets.removeAll()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Crear widget "catalogo"
-- MAGIC dbutils.widgets.text("catalogo","catalogo_clinica")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Crear widget de datos de datalake del metastore
-- MAGIC dbutils.widgets.text("nombre_container","unit-catalog-clinica")
-- MAGIC dbutils.widgets.text("nombre_storage","adlsbrscceu2d01")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Crear widget de datos de la fuente raw de datalake
-- MAGIC dbutils.widgets.text("nombre_container_raw","raw")
-- MAGIC dbutils.widgets.text("nombre_storage_raw","dtlkbrscceu2d01")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ubicación externa para fuentes raw

-- COMMAND ----------

-- Crear ubicación externa para las fuentes de Data Lake
CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-unit-catalog-clinica`
URL 'abfss://${nombre_container}@${nombre_storage}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL credential)
COMMENT 'Ubicación externa para las fuentes de tablas almacenadas en el Data Lake';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ubicación externa para fuentes raw

-- COMMAND ----------

-- Crear ubicación externa para las fuentes de Data Lake
CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-raw-datalake`
URL 'abfss://${nombre_container_raw}@${nombre_storage_raw}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL credential)
COMMENT 'Ubicación externa para las fuentes del Data Lake';

-- COMMAND ----------

-- Crear catálogo principal
CREATE CATALOG IF NOT EXISTS ${catalogo};

-- COMMAND ----------

-- Crear esquemas por capas (bronze, silver, gold)
CREATE SCHEMA IF NOT EXISTS ${catalogo}.bronze;
CREATE SCHEMA IF NOT EXISTS ${catalogo}.silver;
CREATE SCHEMA IF NOT EXISTS ${catalogo}.gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Capa Bronze

-- COMMAND ----------

-- Crear tabla de pacientes en capa bronze
CREATE TABLE IF NOT EXISTS ${catalogo}.bronze.paciente( 
  identificacion INT, 
  nombre STRING, 
  apellido STRING, 
  fecha_nacimiento DATE, 
  genero STRING, 
  email STRING, 
  telefono STRING, 
  ciudad STRING, 
  pais STRING, 
  tipo_sangre STRING 
) USING DELTA
LOCATION 'abfss://${nombre_container}@${nombre_storage}.dfs.core.windows.net/bronze/tablas/paciente';

-- COMMAND ----------

-- Crear tabla de médicos en capa bronze
CREATE TABLE IF NOT EXISTS ${catalogo}.bronze.medico( 
  id_medico STRING, 
  nombre STRING, 
  apellido STRING, 
  especialidad STRING, 
  numero_licencia STRING, 
  hospital STRING, 
  ciudad STRING, 
  telefono STRING, 
  email STRING, 
  estado STRING 
) USING DELTA
LOCATION 'abfss://${nombre_container}@${nombre_storage}.dfs.core.windows.net/bronze/tablas/medico';

-- COMMAND ----------

-- Crear tabla de medicamentos en capa bronze
CREATE TABLE IF NOT EXISTS ${catalogo}.bronze.medicamento( 
  codigo_medicamento STRING, 
  nombre_medicamento STRING, 
  principio_activo STRING, 
  dosis INT, 
  unidad STRING, 
  especialidad STRING, 
  fabricante STRING, 
  precio_unitario INT, 
  estado STRING 
) USING DELTA
LOCATION 'abfss://${nombre_container}@${nombre_storage}.dfs.core.windows.net/bronze/tablas/medicamento';

-- COMMAND ----------

-- Crear tabla de cirugías en capa bronze
CREATE TABLE IF NOT EXISTS ${catalogo}.bronze.cirugia( 
  codigo_cirugia STRING, 
  nombre_cirugia STRING, 
  especialidad STRING, 
  duracion_promedio_minutos INT, 
  complejidad STRING, 
  costo_base INT, 
  riesgo_nivel STRING, 
  estado STRING 
) USING DELTA
LOCATION 'abfss://${nombre_container}@${nombre_storage}.dfs.core.windows.net/bronze/tablas/cirugia';

-- COMMAND ----------

-- Crear tabla de consultas médicas en capa bronze
CREATE TABLE IF NOT EXISTS ${catalogo}.bronze.consultas_medicas( 
  codigo_consulta STRING, 
  id_paciente INT, 
  fecha_consulta DATE, 
  id_medico STRING, 
  especialidad STRING, 
  motivo STRING, 
  diagnostico STRING, 
  costo_consulta INT, 
  estado STRING 
) USING DELTA
LOCATION 'abfss://${nombre_container}@${nombre_storage}.dfs.core.windows.net/bronze/tablas/consultas_medicas';

-- COMMAND ----------

-- Crear tabla de historial de pacientes en capa bronze
CREATE TABLE IF NOT EXISTS ${catalogo}.bronze.historial_pacientes( 
  identificacion INT, 
  nombre STRING,
  medicamentos ARRAY<STRUCT<codigo: STRING, fecha: DATE>>,
  cirugias ARRAY<STRUCT<codigo: STRING, fecha: DATE>>
) USING DELTA
LOCATION 'abfss://${nombre_container}@${nombre_storage}.dfs.core.windows.net/bronze/tablas/historial_pacientes';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Capa Silver

-- COMMAND ----------

-- Crear tabla de pacientes en capa silver
CREATE TABLE IF NOT EXISTS ${catalogo}.silver.paciente( 
  identificacion INT, 
  nombre STRING, 
  apellido STRING, 
  fecha_nacimiento DATE, 
  genero STRING, 
  email STRING, 
  telefono STRING, 
  ciudad STRING, 
  pais STRING, 
  tipo_sangre STRING, 
  fecha_carga TIMESTAMP 
) USING DELTA
LOCATION 'abfss://${nombre_container}@${nombre_storage}.dfs.core.windows.net/silver/tablas/paciente';

-- COMMAND ----------

-- Crear tabla de médicos en capa silver
CREATE TABLE IF NOT EXISTS ${catalogo}.silver.medico(
  id_medico STRING,
  nombre STRING,
  apellido STRING,
  especialidad STRING,
  numero_licencia STRING,
  hospital STRING,
  ciudad STRING,
  telefono STRING,
  email STRING,
  estado STRING,
  fecha_carga TIMESTAMP
) USING DELTA
LOCATION 'abfss://${nombre_container}@${nombre_storage}.dfs.core.windows.net/silver/tablas/medico';

-- COMMAND ----------

-- Crear tabla de medicamentos en capa silver
CREATE TABLE IF NOT EXISTS ${catalogo}.silver.medicamento(
  codigo_medicamento STRING,
  nombre_medicamento STRING,
  principio_activo STRING,
  dosis INT,
  unidad STRING,
  especialidad STRING,
  fabricante STRING,
  precio_unitario INT,
  estado STRING,
  fecha_carga TIMESTAMP
) USING DELTA
LOCATION 'abfss://${nombre_container}@${nombre_storage}.dfs.core.windows.net/silver/tablas/medicamento';

-- COMMAND ----------

-- Crear tabla de cirugías en capa silver
CREATE TABLE IF NOT EXISTS ${catalogo}.silver.cirugia(
  codigo_cirugia STRING,
  nombre_cirugia STRING,
  especialidad STRING,
  duracion_promedio_minutos INT,
  complejidad STRING,
  costo_base INT,
  riesgo_nivel STRING,
  estado STRING,
  fecha_carga TIMESTAMP
) USING DELTA
LOCATION 'abfss://${nombre_container}@${nombre_storage}.dfs.core.windows.net/silver/tablas/cirugia';

-- COMMAND ----------

-- Crear tabla de consultas médicas en capa silver
CREATE TABLE IF NOT EXISTS ${catalogo}.silver.consultas_medicas( 
  codigo_consulta STRING, 
  id_paciente INT, 
  fecha_consulta DATE, 
  id_medico STRING, 
  especialidad STRING, 
  motivo STRING, 
  diagnostico STRING, 
  costo_consulta INT, 
  estado STRING, 
  fecha_carga TIMESTAMP 
) USING DELTA
LOCATION 'abfss://${nombre_container}@${nombre_storage}.dfs.core.windows.net/silver/tablas/consultas_medicas';

-- COMMAND ----------

-- Crear tabla de historial paciente medicamentos en capa silver
CREATE TABLE IF NOT EXISTS ${catalogo}.silver.historial_pacientes_medicamentos( 
  identificacion STRING, 
  nombre STRING,
  fecha DATE, 
  codigo_medicamento STRING,
  fecha_carga TIMESTAMP 
) USING DELTA
LOCATION 'abfss://${nombre_container}@${nombre_storage}.dfs.core.windows.net/silver/tablas/historial_pacientes_medicamentos';

-- COMMAND ----------

-- Crear tabla de historial paciente cirugías en capa silver
CREATE TABLE IF NOT EXISTS ${catalogo}.silver.historial_pacientes_cirugias(
  identificacion STRING,
  nombre STRING,
  fecha DATE,
  codigo_cirugia STRING,
  fecha_carga TIMESTAMP
) USING DELTA
LOCATION 'abfss://${nombre_container}@${nombre_storage}.dfs.core.windows.net/silver/tablas/historial_pacientes_cirugias';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Capa Gold

-- COMMAND ----------

-- Crear tabla de perfil clínico de pacientes en capa gold
CREATE TABLE IF NOT EXISTS ${catalogo}.gold.paciente_perfil_clinico( 
  identificacion INT, 
  nombre STRING, 
  apellido STRING, 
  fecha_nacimiento DATE, 
  genero STRING, 
  tipo_sangre STRING, 
  total_medicamentos INT, 
  total_cirugias INT, 
  especialidades_tratadas ARRAY<STRING>, 
  fecha_ultimo_evento DATE, 
  fecha_carga TIMESTAMP 
) USING DELTA
LOCATION 'abfss://${nombre_container}@${nombre_storage}.dfs.core.windows.net/gold/tablas/paciente_perfil_clinico';

-- COMMAND ----------

-- Crear tabla de consultas agrupadas por médico en capa gold
CREATE TABLE IF NOT EXISTS ${catalogo}.gold.consulta_por_medico( 
  id_medico STRING, 
  nombre_medico STRING, 
  apellido_medico STRING, 
  especialidad STRING, 
  hospital STRING, 
  total_consultas INT, 
  total_ingresos INT, 
  pacientes_unicos INT, 
  costo_promedio INT, 
  fecha_carga TIMESTAMP 
) USING DELTA
LOCATION 'abfss://${nombre_container}@${nombre_storage}.dfs.core.windows.net/gold/tablas/consulta_por_medico';

-- COMMAND ----------

-- Crear tabla de ingresos agrupados por especialidad en capa gold
CREATE TABLE IF NOT EXISTS ${catalogo}.gold.ingresos_por_especialidad( 
  especialidad STRING, 
  total_consultas INT, 
  costo_total_consultas INT, 
  costo_promedio_consulta INT, 
  pacientes_unicos INT, 
  medicos_participantes INT, 
  fecha_carga TIMESTAMP 
) USING DELTA
LOCATION 'abfss://${nombre_container}@${nombre_storage}.dfs.core.windows.net/gold/tablas/ingresos_por_especialidad';

-- COMMAND ----------

-- Crear tabla de análisis de consumo de medicamentos en capa gold
CREATE TABLE IF NOT EXISTS ${catalogo}.gold.medicamentos_consumo(
  codigo_medicamento STRING,
  nombre_medicamento STRING,
  especialidad STRING,
  fabricante STRING,
  total_pacientes INT,
  total_unidades_consumidas INT,
  costo_unitario INT,
  costo_total INT,
  fecha_carga TIMESTAMP
) USING DELTA
LOCATION 'abfss://${nombre_container}@${nombre_storage}.dfs.core.windows.net/gold/tablas/medicamentos_consumo';