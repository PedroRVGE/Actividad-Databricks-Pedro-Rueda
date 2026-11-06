-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05_agregar_venta
-- MAGIC Se agrega una venta valida directamente a ventas_bronze (sin ejecutar el notebook de carga).

-- COMMAND ----------

INSERT INTO workspace.ingenieria_datos_pedro.ventas_bronze
VALUES (16, DATE'2026-10-09', 'Monitor 24 pulgadas', 'Electronica', 1, 480000.0)

-- COMMAND ----------

SELECT * FROM workspace.ingenieria_datos_pedro.ventas_bronze WHERE id_venta = 16

-- COMMAND ----------

SELECT COUNT(*) AS registros_bronze FROM workspace.ingenieria_datos_pedro.ventas_bronze
