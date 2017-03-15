/**
  * Created by jswortz on 3/13/2017.
  */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import play.api.libs.json._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

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

  def epochConverter(epochString: String): Double = epochString.split(" ") match{
    case Array(a, b) => a.toDouble + b.toDouble
    case Array(a) => a.toDouble
    case _ => 0.0
  }

  def jsonParser(input: String): play.api.libs.json.JsValue = {
    Json.parse(input)
  }


  def entityMapper(parssed: play.api.libs.json.JsValue): Map[String, String] = Option(parssed) match {
    case Some(x) => ((x \ "entities" \\ "entity").map(_.toString) zip
      (x \ "entities" \\ "value").map(_.toString)).toMap
    case _ => Map("" -> "")
  }

  def json2map(input: String): Map[String, Object] = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.readValue[Map[String, Object]](input).withDefaultValue("NA")
  }

  //Declaring case classes for different types of data
  case class Verbiage(callUUID: String, stepUUID: String, date: Option[Int], epoch: Option[Double], server: String,
                           model: String, io: String, prompt: String,
                           channelMatch: String, languageOutput: String)

  case class ConvEngineInput(callUUID: String, stepUUID: String, date: Option[Int], epoch: Option[Double], server: String,
                           model: String, io: String, utterance: String,
                             toneEmotion: Option[String], toneScore: Option[Double],
                             timeOfDay: Option[String], channel: Option[String] )

  case class ConvEngineOutput(callUUID: String, stepUUID: String, date: Option[Int], epoch: Option[Double], server: String,
                             model: String, io: String, responseCode: String,
                              response: String, intent: String, entities: Map[String,String], /// will try to figure out parsing entities later
                              confidenceScore: Double)

  case class Dialog(callUUID: String, stepUUID: String, date: Option[Int], epoch: Option[Double], server: String,
                              model: String, seqNo: Int, utterance: Option[String], translation: Option[String],
                              responseType: String, prompt: String)

  case class Context(callUUID: String, stepUUID: String, date: Option[Int], epoch: Option[Double], server: String,
                    model: String, contextAction: String, io: String, inputStatus: String,
                     inputMethod: String, custState: String, channel: String,
                     timeOfDay: String, iiDigits: String, channelChange: String,
                     toneEmotion: String, turnCount: String, nmCounter: String,
                     custServAddress: String, toneScore: String, firstTurn: String/*,
                     addressCount: Option[Int], firstName: String, isCell: String, maxFailNodeName: String,
                     custZip: String, lastNode: String, maxFailAction: String, askSMS: String,
                     custCity: String*/)
  // cant exceed 22 columns!



  //Matching where input data should utilize case classes

  def verb(line: Array[String]): Option[Verbiage] = line match {
    case Array("Voice", u1, u2 ,a, b, x,c,q,e,f,g) => new Some(Verbiage(u1, u2,
      Option(a.toInt),Option(epochConverter(b)),
      x,c,q,e,f,g))
    case _ => None
  }

  def ceInput(line: Array[String]): Option[ConvEngineInput] = line match {
     case Array("Conversation", u1, u2, a, b, x,c,"INPUT",e,f) => new Some(ConvEngineInput(u1, u2,
       Option(a.toInt),Option(epochConverter(b)),
      x,c,"INPUT",e,(jsonParser(f) \ "context" \ "toneEmotion").asOpt[String], (jsonParser(f) \ "context" \ "toneScore").asOpt[Double],
       (jsonParser(f) \ "context" \ "timeOfDay").asOpt[String], (jsonParser(f) \ "context" \ "channel").asOpt[String]))
    case _ => None
  }

  def ceOutput(line: Array[String]): Option[ConvEngineOutput] = line match {
    case Array("Conversation", u1, u2, a, b, x,c,"OUTPUT",e,f,g) => new Some(ConvEngineOutput(u1, u2,
      Option(a.toInt),Option(epochConverter(b)),
      x,c,"OUTPUT",e,f,(jsonParser(g) \ "intents" \\ "intent").headOption.getOrElse("").toString, entityMapper(jsonParser(g)),
      (jsonParser(g) \ "intents" \\ "confidence").headOption.getOrElse(0.0).toString.toDouble))
    case _ => None
  }

  def dialog(line: Array[String]): Option[Dialog] = line match {
    case Array("Dialog", u1, u2, a, b, x,c,y,e,f,g) => new Some(Dialog(u1, u2, Option(a.toInt),Option(epochConverter(b)),
      x,c,y.toInt, if(e.split(':').length > 0) Option(e.split(':')(0)) else Option(""),
      if(e.split(':').length > 1) Option(e.split(':')(1)) else Option("") ,f,g))
    case _ => None
  }

  def context(line: Array[String]): Option[Context] = line match {
    case Array("ContextService", u1, u2, a, b, x, c, d, e, f) => new Some(Context(u1, u2, Option(a.toInt),Option(epochConverter(b)),
      x,c, d, e, f, "", "", "", "", "", "", "", "", "", "", "", ""))
    case Array("ContextService", u1, u2, a, b, x, c, d, e, f, g, h) => new Some(Context(u1, u2, Option(a.toInt),Option(epochConverter(b)),
      x,c, d, e, f, g, json2map(h)("custState").toString, json2map(h)("channel").toString,
      json2map(h)("timeOfDay").toString,json2map(h)("iiDigits").toString,json2map(h)("channelChange").toString,
      json2map(h)("toneEmotion").toString,json2map(h)("turnCount").toString,json2map(h)("nmcounter").toString,
      json2map(h)("custServiceAddress").toString,json2map(h)("toneScore").toString,json2map(h)("firstTurn").toString/*,
      json2map(h)("addressCount").toString.toInt,json2map(h)("firstName").toString,json2map(h)("maxFailNodename").toString,
      json2map(h)("custZip").toString,json2map(h)("lastnode").toString,json2map(h)("maxFailAction").toString,
      json2map(h)("askSMS").toString, json2map(h)("custCity").toString*/))
    case _ => None
  }

  /* Wow - this is an excellent module that parses all json into a map!!
  import com.fasterxml.jackson.databind.ObjectMapper
  import com.fasterxml.jackson.module.scala.DefaultScalaModule
  import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  val obj = mapper.readValue[Map[String, Object]](jsonString)
  */

  //creating final dataframes from case-class homogenous datasets

  val verbOnly = data.map(verb).filter(_.isDefined).map(_.get)

  val verbDF = spark.createDataFrame(verbOnly)

  val ceInputOnly = data.map(ceInput).filter(_.isDefined).map(_.get)

  val ceInputDF = spark.createDataFrame(ceInputOnly)

  val ceOutputOnly = data.map(ceOutput).filter(_.isDefined).map(_.get)

  val ceOutputDF = spark.createDataFrame(ceOutputOnly)

  val dialogOnly = data.map(dialog).filter(_.isDefined).map(_.get)

  val dialogDF = spark.createDataFrame(dialogOnly)

  val contextOnly = data.map(context).filter(_.isDefined).map(_.get)

  val contextDF = spark.createDataFrame(contextOnly)

  //placeholder for writing parquet files to HDFS

  verbDF.write.mode("append").parquet("PLACEHOLDER")

  ceInputDF.write.mode("append").parquet("PLACEHOLDER")

  ceOutputDF.write.mode("append").parquet("assets")

  dialogDF.write.mode("append").parquet("PLACEHOLDER")



}