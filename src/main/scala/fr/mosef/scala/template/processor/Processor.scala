package fr.mosef.scala.template.processor

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

trait Processor {

  def process(inputDF: DataFrame) : DataFrame

  def process2(inputDF: DataFrame): DataFrame

  def process3(inputDF: DataFrame): DataFrame

}