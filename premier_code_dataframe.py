# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType

data = [("Nastasia", "Saby", "Ingénieur ML"),
    ("Tommy", "Rose", "Ingénieur informatique"),
    ("Megan", "Williams","Juriste"),
    ("Ismaël", "Jones", "Service à la personne"),
    ("Mathieu", "Brown","Ingénieur génie civil")
  ]

schema = StructType([ \
    StructField("prenom", StringType(), True), \
    StructField("nom", StringType(), True), \
    StructField("metier", StringType(), True) \
  ])
 
df = spark.createDataFrame(data=data,schema=schema)

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import lit

nouvelle_donnees = df.withColumn("age", lit(24))

# COMMAND ----------

nouvelle_donnees.show()

# COMMAND ----------


