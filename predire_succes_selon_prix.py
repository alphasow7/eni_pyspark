# Databricks notebook source
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType

data: List = [(51, 0),
    (45, 1),
              (50, 1),
              (55, 0),
              (44, 1)
  ]

schema: StructType = StructType([ \
    StructField("prix", IntegerType(), True), \
    StructField("succes", IntegerType(), True)
  ])

diamants_pre_features: DataFrame = spark.createDataFrame(data=data,schema=schema)
  
diamants_pre_features.show()

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

diamants: DataFrame = VectorAssembler(inputCols=["prix"], outputCol="features").transform(diamants_pre_features)

# COMMAND ----------

from pyspark.ml.classification import DecisionTreeClassifier

createur_arbre_de_decision: DecisionTreeClassifier = DecisionTreeClassifier(labelCol="succes", featuresCol="features")
  
from pyspark.ml.classification import DecisionTreeClassificationModel

modele: DecisionTreeClassificationModel = createur_arbre_de_decision.fit(diamants)

predictions: DataFrame = modele.transform(diamants)

predictions.show()

# COMMAND ----------

(entrainements_donnees, test_donnees) = diamants.randomSplit([0.5, 0.5])

# COMMAND ----------

createur_arbre_de_decision: DecisionTreeClassifier = DecisionTreeClassifier(labelCol="succes", featuresCol="features")

modele: DecisionTreeClassificationModel = createur_arbre_de_decision.fit(entrainements_donnees)

# COMMAND ----------

predictions: DataFrame = modele.transform(test_donnees)

predictions.show()

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

evaluator: MulticlassClassificationEvaluator = MulticlassClassificationEvaluator(
    labelCol="succes", predictionCol="prediction", metricName="accuracy")

# COMMAND ----------

accuracy = evaluator.evaluate(predictions)

print(accuracy)

# COMMAND ----------


