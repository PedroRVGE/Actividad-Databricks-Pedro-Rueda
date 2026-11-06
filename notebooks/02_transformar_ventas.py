# Databricks notebook source
# MAGIC %md
# MAGIC # 02_transformar_ventas
# MAGIC Limpieza de ventas_bronze y generacion de ventas_silver y ventas_por_categoria.

# COMMAND ----------

from pyspark.sql.functions import col, trim, sum as _sum, count, round as _round

ESQ = "workspace.ingenieria_datos_pedro"
bronze = spark.table(f"{ESQ}.ventas_bronze")
registros_antes = bronze.count()
print("Registros antes de la limpieza:", registros_antes)

# COMMAND ----------

sin_duplicados = bronze.dropDuplicates()
print("Registros despues de quitar duplicados:", sin_duplicados.count())

ventas_validas = sin_duplicados.filter(
    col("producto").isNotNull()
    & (trim(col("producto")) != "")
    & (col("cantidad") > 0)
    & (col("precio_unitario") > 0)
)

ventas_silver = ventas_validas.withColumn("total_venta", col("cantidad") * col("precio_unitario"))
print("Registros despues de la limpieza:", ventas_silver.count())
display(ventas_silver.orderBy("id_venta"))

# COMMAND ----------

# overwrite: cada ejecucion reemplaza el contenido, asi no se acumulan duplicados
ventas_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{ESQ}.ventas_silver")

ventas_por_categoria = (ventas_silver.groupBy("categoria")
    .agg(count("*").alias("numero_ventas"), _round(_sum("total_venta"), 2).alias("total_vendido"))
    .orderBy(col("total_vendido").desc()))
ventas_por_categoria.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{ESQ}.ventas_por_categoria")
display(spark.table(f"{ESQ}.ventas_por_categoria"))

# COMMAND ----------

print("ventas_bronze:", spark.table(f"{ESQ}.ventas_bronze").count())
print("ventas_silver:", spark.table(f"{ESQ}.ventas_silver").count())
print("ventas_por_categoria:", spark.table(f"{ESQ}.ventas_por_categoria").count())
