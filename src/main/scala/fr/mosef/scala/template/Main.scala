package fr.mosef.scala.template
import fr.mosef.scala.template.job.Job
import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.processor.impl.ProcessorImpl
import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.reader.impl.ReaderImpl
import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.mosef.scala.template.writer.Writer
import org.apache.spark.SparkConf
import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SaveMode

object Main extends App with Job {

  val cliArgs = args
  val MASTER_URL: String = try {
    cliArgs(0)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => "local[1]"
  }

  // Lire les Data
  val SRC_PATH: String = try {
    cliArgs(1)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./src/main/resources/AncillaryScoring_insurance.csv"
    }

  }
  val SRC_PATH_PARQUET: String = try {
    cliArgs(1)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./src/main/resources/AncillaryScoring_insurance.parquet"
    }

  }

  // Path des destinations
  val DST_PATH: String = try {
    cliArgs(2)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./default/output-writer-csv"
    }
  }

  val DST_PATH_PARQUET: String = try {
    cliArgs(2)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./default/output-writer-parquet"
    }
  }

  val conf = new SparkConf()
  conf.set("spark.driver.memory", "64M")

  val sparkSession = SparkSession
    .builder
    .master(MASTER_URL)
    .config(conf)
    .appName("Scala Template")
    .enableHiveSupport()
    .getOrCreate()

  sparkSession
    .sparkContext
    .hadoopConfiguration
    .setClass("fs.file.impl",  classOf[BareLocalFileSystem], classOf[FileSystem])

  // Initialisation :
  val reader: Reader = new ReaderImpl(sparkSession)
  val processor: Processor = new ProcessorImpl()
  val writer: Writer = new Writer()

  // Mettre dans une variable
  val src_path = SRC_PATH
  val src_path_parquet = SRC_PATH_PARQUET

  // Destinations des CSV et Parquet
  val dst_path = DST_PATH
  val dst_path_parquet = DST_PATH_PARQUET

  // Fonction Read (appel fonction) :
  val inputDF = reader.read(src_path)
  val inputDFparquet = reader.readParquet(src_path_parquet)

  // Application du processor (appel fonction)
  val processedDF = processor.process(inputDF)
  val processedDF_parquet = processor.process2(inputDFparquet)


  // Affichage du Rapport :

  // Spécifier où mettre les tables :

  writer.write(processedDF, "overwrite", dst_path)
  writer.writeParquet(processedDF_parquet, "overwrite", dst_path_parquet)


  // Hive :

  // 1) Mettre sous forme de table :
  val tableName = "tables"
  val tableLocation = "./src/main/resources/tables"
  processedDF.write
    .mode(SaveMode.Overwrite)
    .option("path", tableLocation)
    .saveAsTable(tableName) // mettre sous forme de table Hive

  // Lire la table Hive dans le dossier "tables" :
  val hiveTableName = "tables"
  val hiveTableLocation = "./src/main/resources"
  val hiveTableDF = reader.readTable(hiveTableName, hiveTableLocation)

  // Création colonne 0 :
  val columnName = "zero(col)"
  val processedDF_hive = processor.process3(hiveTableDF)

  // Ecriture de la table :
  val tableNametoload = "table_loaded"
  val tablePath = "./default/output-writer-hive"

  // Lecture de la table :
  writer.writeTable(processedDF_hive, tableNametoload, tablePath = tablePath)


}