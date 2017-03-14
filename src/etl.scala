/**
  * Created by jswortz on 3/13/2017.
  */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import play.api.libs.json._

object etl extends App {

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

  def jsonParser(input: String): play.api.libs.json.JsValue = {
    Json.parse(input)
  }

  //Declaring case classes for different types of data
  case class Verbiage(date: Int, epoch: Double, server: String,
                           model: String, io: String, prompt: String,
                           channelMatch: String, languageOutput: String)

  case class ConvEngineInput(date: Int, epoch: Double, server: String,
                           model: String, io: String, utterance: String,
                             toneEmotion: Option[String], toneScore: Option[Double],
                             timeOfDay: Option[String], channel: Option[String] )

  case class ConvEngineOutput(date: Int, epoch: Double, server: String,
                             model: String, io: String, responseCode: String,
                              response: String, intent: String, //entities: Option[String], /// will try to figure out parsing entities later
                              confidenceScore: Double)

  case class Dialog(date: Int, epoch: Double, server: String,
                              model: String, seqNo: Int, utterance: Option[String], translation: Option[String],
                              responseType: String, prompt: String)

  //Matching where input data should utilize case classes

  def verb(line: Array[String]): Option[Verbiage] = line match {
    case Array(a, b, x,c,"INPUT"|"OUTPUT",e,f,g) => new Some(Verbiage(a.toInt,epochConverter(b),
      x,c,"INPUT",e,f,g))
    case _ => None
  }

  def ceInput(line: Array[String]): Option[ConvEngineInput] = line match {
     case Array(a, b, x,c,"INPUT",e,f) => new Some(ConvEngineInput(a.toInt,epochConverter(b),
      x,c,"INPUT",e,(jsonParser(f) \ "context" \ "toneEmotion").asOpt[String], (jsonParser(f) \ "context" \ "toneScore").asOpt[Double],
       (jsonParser(f) \ "context" \ "timeOfDay").asOpt[String], (jsonParser(f) \ "context" \ "channel").asOpt[String]))
    case _ => None
  }

  def ceOutput(line: Array[String]): Option[ConvEngineOutput] = line match {
    case Array(a, b, x,c,"OUTPUT",e,f,g) => new Some(ConvEngineOutput(a.toInt,epochConverter(b),
      x,c,"OUTPUT",e,f,(jsonParser(g) \ "intents" \\ "intent").headOption.getOrElse("").toString, //(jsonParser(g) \ "entities").asOpt[String],
      (jsonParser(g) \ "intents" \\ "confidence").headOption.getOrElse(0.0).toString.toDouble))
    case _ => None
  }

  def dialog(line: Array[String]): Option[Dialog] = line match {
    case Array(a, b, x,c,y,e,f,g) => new Some(Dialog(a.toInt,epochConverter(b),
      x,c,y.toInt, Option(e.split(':')(0)), Option(e.split(':')(1)) ,f,g))
    case _ => None
  }


  //creating final dataframes from case-class homogenous datasets

  val verbOnly = data.map(verb).filter(_.isDefined).map(_.get)

  val verbDF = spark.createDataFrame(verbOnly)

  val ceInputOnly = data.map(ceInput).filter(_.isDefined).map(_.get)

  val ceInputDF = spark.createDataFrame(ceInputOnly)

  val ceOutputOnly = data.map(ceOutput).filter(_.isDefined).map(_.get)

  val ceOutputDF = spark.createDataFrame(ceOutputOnly)

  val dialogOnly = data.map(dialog).filter(_.isDefined).map(_.get)

  val dialogDF = spark.createDataFrame(dialogOnly)



}