// Databricks notebook source
import org.apache.spark.sql.Dataset

case class Diamant(
  prix: Int,
  couleur: String
)


// COMMAND ----------


val diamants: Dataset[Diamant] = Seq(
  Diamant(10, "jaune"), 
  Diamant(500, "bleu")
).toDS

// COMMAND ----------

case class Couleur(
  score: Int,
  couleur: String
)

val couleurs: Dataset[Couleur] = Seq(
  Couleur(10, "jaune"), 
  Couleur(2, "bleu")
).toDS

// COMMAND ----------

val datasets_joints: Dataset[(Diamant, Couleur)] = diamants.joinWith(couleurs, diamants("couleur") === couleurs("couleur"), "left_outer")

// COMMAND ----------

datasets_joints.show()

// COMMAND ----------

datasets_joints.printSchema

// COMMAND ----------


