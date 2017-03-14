/**
  * Created by jswortz on 3/13/2017.
  */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Etl extends App {

  Logger.getLogger("org").setLevel(Level.WARN)

  // When running spark app, indicate HDFS input path for analysis as the first argument

  val path = {
    if (args.length > 0)
      args(0)
    else
      "assets" // replace with runtime parameters
  }

  val spark = SparkSession.builder()
    .master("local[*]") //comment these out when deploying
    .config("spark.sql.warehouse.dir", "spark-warehouse") //comment these out when deploying
    .appName("West POC Cognitive Data ETL")
    .getOrCreate()

  val sc = spark.sparkContext

  val data = sc.textFile(path).map(_.split('|'))

  //helper function to get the value in a "KEY=VALUE" string

  def getValOnly(input: String): String = {
    input.split("=") match {
      case Array(x) => x
      case Array(x, y) => y
      case  _ => "error"
    }
  }

  def epochConverter(epochString: String): Double = {
    epochString.split(" ")(0).toDouble + epochString.split(" ")(1).toDouble
  }

  //Declaring case classes for different types of data
  case class VerbiageInput(date: Int, epoch: Double, server: String,
                           model: String, io: String, prompt: String,
                           channel: String, language: String)

  case class VerbiageOutput(date: Int, epoch: Double, server: String,
                           model: String, io: String, verb: String,
                            success: String, outJSON: String)

  //Matching where input data should utilize case classes

  def verbInOnly(line: Array[String]): Option[VerbiageInput] = line match {
    case Array(a, b, x,c,"INPUT",e,f,g) => new Some(VerbiageInput(a.toInt,epochConverter(b),
      x,c,"INPUT",e,f,g))
    case _ => None
  }

  def verbOutOnly(line: Array[String]): Option[VerbiageOutput] = line match {
     case Array(a, b, x,c,"OUTPUT",e,f,g) => new Some(VerbiageOutput(a.toInt,epochConverter(b),
      x,c,"OUTPUT",e,f,g))
    case _ => None
  }

  //creating final dataframes from caseclass homogenous datasets

  val VIOnly = data.map(verbInOnly).filter(_ != None).map(_.get)

  val VIOnlyDF = spark.createDataFrame(VIOnly)



}