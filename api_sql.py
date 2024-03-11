# Databricks notebook source
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

data: List = [("Diamant_1A", "TopDiamant", 300, "jaune"),
    ("Diamant_2B", "Diamants pour toujours", 45, "gris"),
    ("Diamant_3C", "Mes diamants préférés", 78, "gris"),
    ("Diamant_4D", "Diamants que j'aime", 90, "jaune"),
    ("Diamant_5E", "TopDiamant", 89, "violet"),
    ("Diamant_6F", "Mes diamants préférés", 500, "violet"),
    ("Diamant_7G", "TopDiamant", 250, "gris"),
    ("Diamant_8H", "Diamants que j'aime", 20, "jaune"),
  ]

schema: StructType = StructType([ \
    StructField("reference", StringType(), True), \
    StructField("marque", StringType(), True), \
    StructField("prix", IntegerType(), True), \
    StructField("couleur", StringType(), True)
  ])

dataframe: DataFrame = spark.createDataFrame(data=data,schema=schema)
  
dataframe.show(truncate=False)

# COMMAND ----------

dataframe.createOrReplaceTempView("diamants")

# COMMAND ----------

nouveau_dataframe: DataFrame = spark.sql("""
SELECT avg(prix) AS prix_moyen, couleur, marque
FROM diamants
GROUP BY couleur, marque
""")
  
nouveau_dataframe.show()

# COMMAND ----------

nouveau_dataframe: DataFrame = spark.sql("""
SELECT 
  prix, 
  reference, 
  couleur, 
  marque, 
  avg(prix) OVER (PARTITION BY couleur, marque) AS prix_moyen
FROM diamants
""")
  
nouveau_dataframe.show()

# COMMAND ----------


