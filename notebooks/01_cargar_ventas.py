# Databricks notebook source
# MAGIC %md
# MAGIC # 01_cargar_ventas
# MAGIC Carga de ventas de la tienda en la capa bronze (tabla Delta).

# COMMAND ----------

CATALOGO = "workspace"
ESQUEMA = "ingenieria_datos_pedro"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{ESQUEMA}")
spark.sql(f"USE {CATALOGO}.{ESQUEMA}")

# COMMAND ----------

from datetime import date
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType, DoubleType

esquema = StructType([
    StructField("id_venta", IntegerType(), False),
    StructField("fecha", DateType(), True),
    StructField("producto", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("cantidad", IntegerType(), True),
    StructField("precio_unitario", DoubleType(), True),
])

ventas = [
    (1,  date(2026, 10, 1), "Audifonos",         "Electronica", 2,  85000.0),
    (2,  date(2026, 10, 1), "Camiseta",          "Ropa",        3,  45000.0),
    (3,  date(2026, 10, 2), "Licuadora",         "Hogar",       1, 220000.0),
    (4,  date(2026, 10, 2), "Arroz 5kg",         "Alimentos",   4,  18000.0),
    (5,  date(2026, 10, 3), "Mouse inalambrico", "Electronica", 2,  60000.0),
    (6,  date(2026, 10, 3), "Jean",              "Ropa",        1, 120000.0),
    (7,  date(2026, 10, 4), "Sarten",            "Hogar",       2,  75000.0),
    (8,  date(2026, 10, 4), "Cafe 500g",         "Alimentos",   6,  22000.0),
    (9,  date(2026, 10, 5), "Teclado",           "Electronica", 1, 150000.0),
    (10, date(2026, 10, 5), "Chaqueta",          "Ropa",        1, 250000.0),
    (11, date(2026, 10, 6), "Juego de toallas",  "Hogar",       2,  65000.0),
    (12, date(2026, 10, 6), "Aceite 1L",         "Alimentos",   3,  15000.0),
    (13, date(2026, 10, 7), "Cargador USB-C",    "Electronica", 3,  40000.0),
    (13, date(2026, 10, 7), "Cargador USB-C",    "Electronica", 3,  40000.0),   # venta duplicada
    (14, date(2026, 10, 8), None,                "Hogar",       2,  30000.0),   # venta sin producto
    (15, date(2026, 10, 8), "Medias",            "Ropa",       -2,  12000.0),   # cantidad negativa
]

df_ventas = spark.createDataFrame(ventas, schema=esquema)
display(df_ventas)

# COMMAND ----------

df_ventas.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOGO}.{ESQUEMA}.ventas_bronze")
print("Registros cargados en ventas_bronze:", spark.table(f"{CATALOGO}.{ESQUEMA}.ventas_bronze").count())
