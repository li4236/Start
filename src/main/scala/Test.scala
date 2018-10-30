import java.util

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.{BasicHttpCredentials, RawHeader}
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import amqpMongo.Main.{client, theUpdates}
import org.mongodb.scala.MongoClient
import reactivemongo.api.MongoDriver
//import amqpMongo.Main.{createHistoryCollections, createMeteorCollections, historydb, ldcProtocolConversionGraph, meteordb, theUpdates, watchMoreCollectionsGraph}
import bean.RMQInfo
import org.json4s.jackson.Json
import org.json4s.{JField, JString}
import org.json4s.jackson.JsonMethods.{parse, render}
import shaded.google.common.reflect.TypeToken

import scala.collection.JavaConverters._
import com.google.gson.Gson
import java.util.ArrayList
import java.lang.reflect.Type

import scala.concurrent.Future
import scala.util.parsing.json.JSON
import scala.util.{Failure, Success}
import org.json4s._
object Test {
//  def main(args: Array[String]) {
//    println( "Returned Value : " + addInt(5,7) );
//  }
//
//  def addInt( a:Int, b:Int ) : Int = {
//    var sum:Int = 0
//    sum = a + b
//
//    return sum
//  }

  var factor = 3;

  val mul = (i:Int) => i * factor;


  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  // needed for the future flatMap/onComplete in the end
  implicit val executionContext = system.dispatcher

  val host_url = "http://192.168.11.121:15672" // this is a mock service
  val get_api = "api/queues"
  val post_api = "login"
  val post_username_param = "ldc_proj"
  val post_password_param = "lc56&49*pr"

  private val client = MongoClient(s"mongodb://172.18.0.5:27017")
  private val dbMeteor = client.getDatabase("meteor")
  private val dbfull = client.getDatabase("full")
  private val dbupdates = client.getDatabase("updates")
//  val driver = new MongoDriver
//  val connection = driver.connection(List("172.18.0.5:27017"))
//  val meteordb = connection.database("meteor")
//  val historydb = connection.database("full")
//  val historydb_updates = connection.database("updates")
  def main(args: Array[String]): Unit = {

    // test get
    getReq(host_url + "/" + get_api)
  }

  def initMongoDb():Unit ={
//    val driver = new MongoDriver
//    val connection = driver.connection(List("172.18.0.5:27017"))
//    val meteordb = connection.database("meteor")
//    val historydb = connection.database("full")
//    val historydb_updates = connection.database("updates")
  }



  // url can be rest or non-rest
  def getReq(url: String): Unit =
  {
    implicit val formats = DefaultFormats
    val responseFuture: Future[HttpResponse] = Http().singleRequest(HttpRequest(uri = url, method = HttpMethods.GET)
      .addCredentials(BasicHttpCredentials(post_username_param,post_password_param)))
    responseFuture.onComplete {
      case Success(res) => {
        println(res)


        Unmarshal(res.entity).to[String]
          .map(jsonString => parse(jsonString))
          .map(x => render((x removeField {
            _ == JField("name", JString("1023"))
          }) \ "name"))
          .map(_.extract[List[String]])
          .map(_.filter(_.endsWith("_reply")))
          .map(theUpdates(_))

//        Unmarshal(res.entity).to[String]
//          .map(jsonString => parse(jsonString))
//          .map(x => render((x removeField{ _ == JField("name", JString("1023"))})\ "name"))
////          .map(_.extract[List[String]])
//          .map(_.filter(_.endsWith("_reply")))
//          .map(theUpdates(_))
        Unmarshal(res.entity).to[String].map { json_str =>
          Right {
            // here we can deserialize json string to object or list of object
            // sth like val res_obj = Json.deserialize([Model])(json_str)
            // or val res_obj = Json.deserialize(Seq[Model])(json_str)
//            val res_obj = Json.deserialize([RMQInfo])(json_str)
            println("get result: ", json_str)
//            val gson : Gson = new Gson()
//
//
//            val listType : Type = new TypeToken[ArrayList[RMQInfo]]() {}.getType()
//            val tmp2 : ArrayList[RMQInfo] = (gson.fromJson(json_str, listType))

//            val q = tmp2.map(_.replaceAllLiterally("_reply", ""))


//            val b = JSON.parseFull(json_str)
//            b match {
//              // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
//              case Some(map: Map[String, Any]) =>  map(_.filter(_.endsWith("_reply")))
//              case None => println("Parsing failed")
//              case other => println("Unknown data structure: " + other)
//
            }
          }
//        }
      }
      case Failure(error) => println(error.getMessage)
    }
  }

  def theUpdates(ql: List[String]) {

    println("list result: ", ql)
    val q = ql.map(_.replaceAllLiterally("_reply", ""))
    println("list2 result: ", q)


    val test = dbMeteor.getCollection();
//    val meteorresult = meteordb.flatMap(x => x.collectionNames)

//    println(meteorresult);
//    val historyresult = historydb.flatMap(x => x.collectionNames);
//    //    val upresult = historydb_updates.flatMap(x => x.collectionNames)
//    //    val finalResult = ql.filterNot(result.contains(_))
//
//    //    println(result)
//    meteorresult onComplete {
//      case Success(r) =>
//        q.filterNot(r.contains(_))
//          .map(x => s"$x" + "_reply")
//          .map(ldcProtocolConversionGraph(_))
//        q.filterNot(r.contains(_))
//          .map(createMeteorCollections(_))
//        println("result: " + r)
//      case Failure(t) =>
//        println("fail: " + t)
//    }
//    historyresult onComplete {
//      case Success(r) =>
//        q.filterNot(r.contains(_)).map(createHistoryCollections(_))
//        q.filterNot(r.contains(_)).map(watchMoreCollectionsGraph(_))
//        println("result: " + r)
//      case Failure(t) =>
//        println("fail: " + t)
//    }
  }

  def postReq(url: String): Unit =
  {
    val responseFuture: Future[HttpResponse] = Http().singleRequest(HttpRequest(uri = url, method = HttpMethods.POST))
    responseFuture.onComplete {
      case Success(res) => {
        println(res)
        Unmarshal(res.entity).to[String].map { json_str =>
          Right {
            // here we can deserialize json string to object or list of object
            // sth like val res_obj = Json.deserialize([Model])(json_str)
            // or val res_obj = Json.deserialize(Seq[Model])(json_str)

            println("post result: ", json_str)
          }
        }
      }
      case Failure(error) => println("错误",error.getMessage)
    }
  }



}