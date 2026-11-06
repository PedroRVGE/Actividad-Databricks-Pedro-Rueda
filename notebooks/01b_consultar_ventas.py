# Databricks notebook source
# MAGIC %md
# MAGIC # 01b_consultar_ventas
# MAGIC Lectura de ventas_bronze desde otro notebook usando el nombre completo catalogo.esquema.tabla.

# COMMAND ----------

df = spark.table("workspace.ingenieria_datos_pedro.ventas_bronze")
df.printSchema()
display(df)

# COMMAND ----------

print("Total de registros en ventas_bronze:", df.count())

# COMMAND ----------

from pyspark.sql.functions import col
display(df.filter(col("categoria") == "Electronica"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT categoria, COUNT(*) AS registros
# MAGIC FROM workspace.ingenieria_datos_pedro.ventas_bronze
# MAGIC GROUP BY categoria
# MAGIC ORDER BY categoria
