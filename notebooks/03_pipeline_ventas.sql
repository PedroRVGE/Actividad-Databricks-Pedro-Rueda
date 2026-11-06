-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03_pipeline_ventas
-- MAGIC Pipeline declarativo (Lakeflow Spark Declarative Pipelines) sobre ventas_bronze.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW pipeline_ventas_limpias
COMMENT "Ventas sin duplicados, con producto, cantidad y precio validos, y total_venta calculado"
AS
SELECT DISTINCT
  id_venta,
  fecha,
  producto,
  categoria,
  cantidad,
  precio_unitario,
  cantidad * precio_unitario AS total_venta
FROM workspace.ingenieria_datos_pedro.ventas_bronze
WHERE producto IS NOT NULL
  AND trim(producto) <> ''
  AND cantidad > 0
  AND precio_unitario > 0;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW pipeline_ventas_por_categoria
COMMENT "Valor total vendido por categoria"
AS
SELECT
  categoria,
  COUNT(*) AS numero_ventas,
  ROUND(SUM(total_venta), 2) AS total_vendido
FROM pipeline_ventas_limpias
GROUP BY categoria;
