import java.util

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.{BasicHttpCredentials, RawHeader}
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.alpakka.amqp.{IncomingMessage, NamedQueueSourceSettings}
import akka.stream.alpakka.amqp.scaladsl.AmqpSource
import akka.stream.alpakka.mongodb.scaladsl.{DocumentUpdate, MongoSink, MongoSource}
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, Partition, RestartSource, RunnableGraph, Sink, Source}
import akka.util.ByteString
import amqpMongo.Main.{amqpUri, client, dbMeteor, dbfull, dbupdates, theUpdates, updatetimestamp}
import com.mongodb.client.model.changestream.FullDocument
import org.mongodb.scala.{Document, MongoClient}
import org.mongodb.scala.model.{Aggregates, Filters, changestream}
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

  private val client = MongoClient(s"mongodb://192.168.11.121:27017")
  private val dbMeteor = client.getDatabase("meteor")
  private val dbfull = client.getDatabase("full")
  private val dbupdates = client.getDatabase("updates")
  val driver = new MongoDriver
  val connection = driver.connection(List("192.168.11.121:27017"))
  val meteordb = connection.database("meteor")
  val historydb = connection.database("full")
  val historydb_updates = connection.database("updates")
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
//        Unmarshal(res.entity).to[String].map { json_str =>
//          Right {
//            // here we can deserialize json string to object or list of object
//            // sth like val res_obj = Json.deserialize([Model])(json_str)
//            // or val res_obj = Json.deserialize(Seq[Model])(json_str)
////            val res_obj = Json.deserialize([RMQInfo])(json_str)
//            println("get result: ", json_str)
////            val gson : Gson = new Gson()
////
////
////            val listType : Type = new TypeToken[ArrayList[RMQInfo]]() {}.getType()
////            val tmp2 : ArrayList[RMQInfo] = (gson.fromJson(json_str, listType))
//
////            val q = tmp2.map(_.replaceAllLiterally("_reply", ""))
//
//
////            val b = JSON.parseFull(json_str)
////            b match {
////              // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
////              case Some(map: Map[String, Any]) =>  map(_.filter(_.endsWith("_reply")))
////              case None => println("Parsing failed")
////              case other => println("Unknown data structure: " + other)
////
//            }
//          }
//        }
      }
      case Failure(error) => println(error.getMessage)
    }
  }

  def theUpdates(ql: List[String]) {

//    println("list result: ", ql)
    val q = ql.map(_.replaceAllLiterally("_reply", ""))
    println("RabbitMQ Result: "+ q)


//    val test = dbMeteor.getCollection();
    val meteorresult = meteordb.flatMap(x => x.collectionNames)

//    println("你好："+meteorresult);
//    val historyresult = historydb.flatMap(x => x.collectionNames);
//    //    val upresult = historydb_updates.flatMap(x => x.collectionNames)
//    //    val finalResult = ql.filterNot(result.contains(_))
//
//    //    println(result)
    meteorresult onComplete {
      case Success(r) =>
        q.filterNot(r.contains(_))
          .map(x => s"$x" + "_reply")
          .map(ldcProtocolConversionGraph(_))
//        q.filterNot(r.contains(_))
//          .map(createMeteorCollections(_))
        println("result: " + r)
      case Failure(t) =>
        println("fail: " + t)
    }
//    historyresult onComplete {
//      case Success(r) =>
//        q.filterNot(r.contains(_)).map(createHistoryCollections(_))
//        q.filterNot(r.contains(_)).map(watchMoreCollectionsGraph(_))
//        println("result: " + r)
//      case Failure(t) =>
//        println("fail: " + t)
//    }
  }
  def ldcProtocolConversionGraph(queueName: String) = {

    println("输出QueueName:"+queueName);

    val collectionName = queueName.replaceAllLiterally("_reply", "")
    //取集合
    val mongoCollection = dbMeteor.getCollection(collectionName)

    val mongoHistCollection = dbfull.getCollection(collectionName)

    val mongoUpdatesCollection = dbupdates.getCollection(collectionName)

    println(collectionName)

    val sourceFull: Source[changestream.ChangeStreamDocument[Document], NotUsed] =
      MongoSource(mongoCollection
      .watch(List(Aggregates.`match`(Filters.and(Filters.eq("documentKey._id", "history"),
        Filters.in("operationType", "update")))))
      .fullDocument(FullDocument.UPDATE_LOOKUP))

    val sourceUpdates: Source[changestream.ChangeStreamDocument[Document], NotUsed] =
      MongoSource(mongoCollection
      .watch(List(Aggregates.`match`(Filters.and(Filters.ne("documentKey._id", "history"),
        //        Filters.exists("updateDescription.updatedFields.ts",false),
        Filters.in("operationType", "update")))))
      .fullDocument(FullDocument.UPDATE_LOOKUP))

    //插入数据
    val insertFullDoc = MongoSink.insertOne(1, mongoHistCollection)

    val insertUpdatesDoc = MongoSink.insertOne(1, mongoUpdatesCollection)

    val updateDoc: Sink[DocumentUpdate, Future[Done]] = MongoSink.updateMany(1, mongoCollection)

//    val tsFlow = Flow[IncomingMessage].map(msg => msg.properties
//      .getHeaders.get("timestamp_in_ms"))
//      .map(l => updatetimestamp(collectionName, l.asInstanceOf[Long]))

    val monitor = Flow[IncomingMessage]
      .map(msg => msg.bytes)
      .conflateWithSeed(_ ⇒ 0) { case (acc, _) ⇒ acc + 1 }
      .zip(Source.tick(2.seconds, 2.seconds, 1))
      .map(_._2)

    val counter = Source.tick(2.seconds, 2.seconds, 0)

    val rmongoSink = Sink.ignore

    val changeStreamGraph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      //      val bcast = builder.add(Broadcast[changestream.ChangeStreamDocument[Document]](2))

      sourceFull ~> docConv ~> insertFullDoc
      sourceUpdates ~> docConv ~> insertUpdatesDoc
      ClosedShape
    })

    val amqptoMongoGraph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val unixTime = System.currentTimeMillis / 1000L

      val ignoredValue = (ByteString(0xAA, 0x71, -1), unixTime)

      val modeStarterSource = Source.single(ignoredValue)

      val thesource = RestartSource.onFailuresWithBackoff(
        minBackoff = 1.seconds,
        maxBackoff = 30.seconds,
        randomFactor = 0.2
      ) { () => AmqpSource.atMostOnceSource(NamedQueueSourceSettings(amqpUri, queueName), 10)
      }

      val partition = builder.add(Partition[(ByteString, Any)](24, {
        case po if po._1(1) == 0x70 ⇒ 0
        case ele_m if ele_m._1(1) == 0x71 ⇒ 1
        case ele_s if ele_s._1(1) == 0x72 ⇒ 2
        case flr if flr._1(1) == 0x73 ⇒ 3
        case spd_ht if spd_ht._1(1) == 0x74 ⇒ 4
        case dir if dir._1(1) == 0x75 ⇒ 5
        case dr if dr._1(1) == 0x76 ⇒ 6
        case reg_lc if reg_lc._1(1) == 0x77 ⇒ 7
        case reg_cc if reg_cc._1(1) == 0x78 ⇒ 8
        case dr_btn if dr_btn._1(1) == 0x79 ⇒ 9

        case upls if upls._1.slice(1, 3) == ByteString(0x60, 0x01) => 10
        case sst1 if sst1._1.slice(1, 3) == ByteString(0x60, 0x02) => 11
        case sst2 if sst2._1.slice(1, 3) == ByteString(0x60, 0x04) => 12
        case lpls if lpls._1.slice(1, 3) == ByteString(0x60, 0x10) => 13
        case ssb1 if ssb1._1.slice(1, 3) == ByteString(0x60, 0x20) => 14
        case ssb2 if ssb2._1.slice(1, 3) == ByteString(0x60, 0x40) => 15

        case upls if upls._1.slice(1, 3) == ByteString(0x61, 0x01) => 16
        case sst1 if sst1._1.slice(1, 3) == ByteString(0x61, 0x02) => 17
        case sst2 if sst2._1.slice(1, 3) == ByteString(0x61, 0x04) => 18
        case unknown if unknown._1.slice(1, 3) == ByteString(0x61, -128) => 19

        case safetyckt if safetyckt._1(1) == 0x62 => 20
        case po_fl if po_fl._1(1) == 0x63 => 21
        case err if err._1(1) == -128 => 22
        case command if command._1(0) == -69 => 23
      }))

      val bcast = builder.add(Broadcast[IncomingMessage](2))

      val bcast_sc = builder.add(Broadcast[(ByteString, Any)](6))

      val merge_source = builder.add(Merge[(ByteString, Any)](2))
      val merge_sc = builder.add(Merge[DocumentUpdate](6))
      val merge_monitor = builder.add(Merge[Int](2))

      val sswitch = Sink.foreach[ByteString](apple => println("switch"))
      val ssafetyckt = Sink.foreach[ByteString](apple => println("safetyckt"))
      val scommand = Sink.foreach[(ByteString, Any)](apple => println("command"))


      thesource ~> bcast ~> flow1 ~> partition.in


      partition.out(0) ~> pdo_out_value ~> updateDoc

      partition.out(1) ~> merge_source ~> ele_mode_value ~> updateDoc
      modeStarterSource ~> merge_source

      partition.out(2) ~> ele_status_value ~> updateDoc

      partition.out(3) ~> floor_value ~> updateDoc

      partition.out(4) ~> speed_height_value ~> updateDoc

      partition.out(5) ~> direction_value ~> updateDoc

      partition.out(6) ~> door_status ~> updateDoc

      partition.out(7) ~> registered_landing_call_value ~> updateDoc

      partition.out(8) ~> registered_car_call_value ~> updateDoc

      partition.out(9) ~> door_button_value ~> updateDoc

      partition.out(10) ~> upls_activation ~> updateDoc
      partition.out(11) ~> sst1_activation ~> updateDoc
      partition.out(12) ~> sst2_activation ~> updateDoc
      partition.out(13) ~> lpls_activation ~> updateDoc
      partition.out(14) ~> ssb1_activation ~> updateDoc
      partition.out(15) ~> ssb2_activation ~> updateDoc

      partition.out(16) ~> dzs_activation ~> updateDoc
      partition.out(17) ~> usi_activation ~> updateDoc
      partition.out(18) ~> lsi_activation ~> updateDoc
      partition.out(19) ~> prinSink

      partition.out(20) ~> bcast_sc ~> sc1_1 ~> merge_sc ~> updateDoc
      bcast_sc ~> sc1_2 ~> merge_sc
      bcast_sc ~> sc1_3 ~> merge_sc
      bcast_sc ~> sc2 ~> merge_sc
      bcast_sc ~> sc3 ~> merge_sc
      bcast_sc ~> a3 ~> merge_sc

      partition.out(21) ~> run_floor_index_value ~> updateDoc
      partition.out(22) ~> err ~> updateDoc

      partition.out(23) ~> scommand

      bcast ~> monitor ~> merge_monitor ~> onoroff ~> updateDoc
      counter ~> merge_monitor
      ClosedShape
    })
    amqptoMongoGraph.run()
    changeStreamGraph.run()

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