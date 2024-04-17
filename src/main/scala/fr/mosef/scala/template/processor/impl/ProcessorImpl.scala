package fr.mosef.scala.template.processor.impl


import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class ProcessorImpl() extends Processor {

  def process(inputDF: DataFrame): DataFrame = {
    inputDF.groupBy("geoNetwork_country").count().orderBy(desc("count"))
  }

  def process2(inputDF: DataFrame): DataFrame = {

    // Supprimer la colonne "PAXCOUNT" du DataFrame résultant
    val resultWithoutColumn = inputDF.drop("PAXCOUNT")

    // Retourner le DataFrame résultant sans la colonne "TRIPTYPEDESC"
    resultWithoutColumn
  }

  def process3(inputDF: DataFrame): DataFrame = {
    // Ajouter une colonne "message" avec la valeur "ok"
    val resultWithMessageColumn = inputDF.withColumn("message", lit("ok"))

    // Retourner le DataFrame résultant avec la colonne ajoutée
    resultWithMessageColumn
  }

}
