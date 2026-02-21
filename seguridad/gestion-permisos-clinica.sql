-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gestión de Permisos - Sistema Clínico
-- MAGIC 
-- MAGIC Este notebook administra permisos para el proyecto de gestión clínica.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Permisos en Catálogo

-- COMMAND ----------

-- Custodios técnicos: Permisos completos en el catálogo
GRANT ALL PRIVILEGES ON CATALOG catalogo_clinica TO `custodios_tecnicos`;

-- COMMAND ----------

-- Data Modelers: Permisos de uso y creación en el catálogo
GRANT USE CATALOG ON CATALOG catalogo_clinica TO `data_modeler`;
GRANT CREATE SCHEMA ON CATALOG catalogo_clinica TO `data_modeler`;

-- COMMAND ----------

-- Data Engineers: Permisos de uso del catálogo
GRANT USE CATALOG ON CATALOG catalogo_clinica TO `data_engineer`;

-- COMMAND ----------

-- QA: Solo permisos de uso del catálogo
GRANT USE CATALOG ON CATALOG catalogo_clinica TO `qa`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Permisos en Schemas (Bronze, Silver, Gold)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Schema Bronze

-- COMMAND ----------

-- Custodios técnicos: Permisos completos en Bronze
GRANT ALL PRIVILEGES ON SCHEMA catalogo_clinica.bronze TO `custodios_tecnicos`;

-- COMMAND ----------

-- Data Modelers: Solo lectura en Bronze (no necesitan crear en esta capa)
GRANT USE SCHEMA ON SCHEMA catalogo_clinica.bronze TO `data_modeler`;

-- COMMAND ----------

-- Data Engineers: Uso y creación en Bronze
GRANT USE SCHEMA ON SCHEMA catalogo_clinica.bronze TO `data_engineer`;
GRANT CREATE TABLE ON SCHEMA catalogo_clinica.bronze TO `data_engineer`;

-- COMMAND ----------

-- QA: Solo lectura en Bronze
GRANT USE SCHEMA ON SCHEMA catalogo_clinica.bronze TO `qa`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Schema Silver

-- COMMAND ----------

-- Custodios técnicos: Permisos completos en Silver
GRANT ALL PRIVILEGES ON SCHEMA catalogo_clinica.silver TO `custodios_tecnicos`;

-- COMMAND ----------

-- Data Modelers: Solo lectura en Silver (no necesitan crear en esta capa)
GRANT USE SCHEMA ON SCHEMA catalogo_clinica.silver TO `data_modeler`;

-- COMMAND ----------

-- Data Engineers: Uso y creación en Silver
GRANT USE SCHEMA ON SCHEMA catalogo_clinica.silver TO `data_engineer`;
GRANT CREATE TABLE ON SCHEMA catalogo_clinica.silver TO `data_engineer`;

-- COMMAND ----------

-- QA: Solo lectura en Silver
GRANT USE SCHEMA ON SCHEMA catalogo_clinica.silver TO `qa`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Schema Gold

-- COMMAND ----------

-- Custodios técnicos: Permisos completos en Gold
GRANT ALL PRIVILEGES ON SCHEMA catalogo_clinica.gold TO `custodios_tecnicos`;

-- COMMAND ----------

-- Data Modelers: Uso y creación en Gold
GRANT USE SCHEMA ON SCHEMA catalogo_clinica.gold TO `data_modeler`;
GRANT CREATE TABLE ON SCHEMA catalogo_clinica.gold TO `data_modeler`;

-- COMMAND ----------

-- Data Engineers: Uso y creación en Gold
GRANT USE SCHEMA ON SCHEMA catalogo_clinica.gold TO `data_engineer`;
GRANT CREATE TABLE ON SCHEMA catalogo_clinica.gold TO `data_engineer`;

-- COMMAND ----------

-- QA: Solo lectura en Gold
GRANT USE SCHEMA ON SCHEMA catalogo_clinica.gold TO `qa`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Permisos en Tablas Específicas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Tablas Bronze
-- MAGIC 
-- MAGIC Los custodios técnicos ya tienen ALL PRIVILEGES a nivel de schema.

-- COMMAND ----------

-- Data Modelers: Permisos SELECT en todas las tablas Bronze
GRANT SELECT ON SCHEMA catalogo_clinica.bronze TO `data_modeler`;

-- COMMAND ----------

-- Data Modelers: Permisos SELECT en todas las tablas Silver
GRANT SELECT ON SCHEMA catalogo_clinica.silver TO `data_modeler`;

-- COMMAND ----------

-- Data Engineers: Permisos SELECT y MODIFY en todas las tablas Bronze
GRANT SELECT ON SCHEMA catalogo_clinica.bronze TO `data_engineer`;
GRANT MODIFY ON SCHEMA catalogo_clinica.bronze TO `data_engineer`;

-- COMMAND ----------

-- QA: Permisos SELECT en todas las tablas Bronze
GRANT SELECT ON SCHEMA catalogo_clinica.bronze TO `qa`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Tablas Silver
-- MAGIC 
-- MAGIC Los data engineers ya tienen permisos de creación a nivel de schema.

-- COMMAND ----------

-- Data Engineers: Permisos SELECT y MODIFY en todas las tablas Silver
GRANT SELECT ON SCHEMA catalogo_clinica.silver TO `data_engineer`;
GRANT MODIFY ON SCHEMA catalogo_clinica.silver TO `data_engineer`;

-- COMMAND ----------

-- QA: Permisos SELECT en todas las tablas Silver
GRANT SELECT ON SCHEMA catalogo_clinica.silver TO `qa`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Tablas Gold

-- COMMAND ----------

-- Todos los grupos tienen SELECT en Gold
GRANT SELECT ON SCHEMA catalogo_clinica.gold TO `data_modeler`;
GRANT SELECT ON SCHEMA catalogo_clinica.gold TO `data_engineer`;
GRANT SELECT ON SCHEMA catalogo_clinica.gold TO `qa`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Permisos en External Locations

-- COMMAND ----------

-- Custodios técnicos: Todos los permisos en external locations
GRANT ALL PRIVILEGES ON EXTERNAL LOCATION `exlt-unit-catalog-clinica` TO `custodios_tecnicos`;
GRANT ALL PRIVILEGES ON EXTERNAL LOCATION `exlt-raw-datalake` TO `custodios_tecnicos`;

-- COMMAND ----------

-- Data Engineers: READ y WRITE en todas las capas
GRANT READ FILES ON EXTERNAL LOCATION `exlt-raw-datalake` TO `data_engineer`;
GRANT WRITE FILES ON EXTERNAL LOCATION `exlt-raw-datalake` TO `data_engineer`;
GRANT READ FILES ON EXTERNAL LOCATION `exlt-unit-catalog-clinica` TO `data_engineer`;
GRANT WRITE FILES ON EXTERNAL LOCATION `exlt-unit-catalog-clinica` TO `data_engineer`;

-- COMMAND ----------

-- Data Modelers: Solo necesitan READ del catálogo para análisis
GRANT READ FILES ON EXTERNAL LOCATION `exlt-unit-catalog-clinica` TO `data_modeler`;
