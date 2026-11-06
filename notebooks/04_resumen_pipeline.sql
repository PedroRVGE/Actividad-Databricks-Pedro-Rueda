-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04_resumen_pipeline
-- MAGIC Consulta de los resultados publicados por el pipeline declarativo.

-- COMMAND ----------

SELECT categoria, numero_ventas, total_vendido
FROM workspace.ingenieria_datos_pedro.pipeline_ventas_por_categoria
ORDER BY total_vendido DESC

-- COMMAND ----------

SELECT COUNT(*) AS ventas_limpias, SUM(total_venta) AS total_general
FROM workspace.ingenieria_datos_pedro.pipeline_ventas_limpias
