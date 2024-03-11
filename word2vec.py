# Databricks notebook source
from pyspark.sql import DataFrame

phrases: DataFrame = spark.createDataFrame([
    "La chancelière présidait le conseil",
    "Un président présidait le conseil",
], "string").toDF("phrases")

# COMMAND ----------

from pyspark.ml.feature import Tokenizer

decoupage_en_mots: Tokenizer = Tokenizer(inputCol="phrases", outputCol="mots")
mots: DataFrame = decoupage_en_mots.transform(phrases)

# COMMAND ----------

from pyspark.ml.feature import Word2Vec, Word2VecModel

word2Vec: Word2Vec = Word2Vec(vectorSize=5, minCount=0, inputCol="mots", outputCol="features")
model: Word2VecModel = word2Vec.fit(mots)

resultat: DataFrame = model.transform(mots)

# COMMAND ----------

model.getVectors().show()

# COMMAND ----------

model.findSynonyms("président", 1).show()

# COMMAND ----------


