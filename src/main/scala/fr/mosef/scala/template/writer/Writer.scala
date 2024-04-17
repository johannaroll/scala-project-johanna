package fr.mosef.scala.template.writer

import org.apache.spark.sql.DataFrame
class Writer {

  def write(df: DataFrame, mode: String = "overwrite", path: String, separator: String = ","): Unit = {
    df
      .write
      .option("header", "true") // 1ere ligne = nom des colonnes
      .option("sep", separator)  // Spécifie le séparateur personnalisé
      .mode(mode)
      .csv(path)
  }

  def writeParquet(df: DataFrame, mode: String = "overwrite", path: String): Unit = {
    df.write
      .mode(mode)
      .parquet(path)
  }

  def writeTable(df: DataFrame, tableName: String, mode: String = "overwrite", tablePath: String): Unit = {
    df.write
      .mode(mode)
      .option("path", tablePath)
      .saveAsTable(tableName)
  }

}
