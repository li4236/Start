package amqpMongo

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.BasicHttpCredentials
import akka.http.scaladsl.unmarshalling._
import akka.stream.alpakka.amqp._
import akka.stream.alpakka.amqp.scaladsl.AmqpSource
import akka.stream.alpakka.mongodb.scaladsl.{DocumentUpdate, MongoSink, MongoSource}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, Merge, Partition, RestartSource, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.util.ByteString
import akka.{Done, NotUsed}
import util.DealData
//import amqpMongo.BytesPerSecondActor.byteCounter
import com.mongodb.client.model.changestream.FullDocument
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.mongodb.scala.model.{Aggregates, Filters, Updates, changestream}
import org.mongodb.scala.{Document, MongoClient, MongoCollection}
import reactivemongo.api._
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.bson.{BSONArray, BSONDateTime, BSONDocument}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.{Failure, Success}

object Main extends App {

  implicit val formats = DefaultFormats
  implicit val actorSystem = ActorSystem()

  implicit val actorMaterializer = ActorMaterializer()

  private val client = MongoClient(s"mongodb://172.18.0.5:27017")
  private val dbMeteor = client.getDatabase("meteor")
  private val dbfull = client.getDatabase("full")
  private val dbupdates = client.getDatabase("updates")

  private val dealData = new DealData();


  val docConv = Flow[changestream.ChangeStreamDocument[Document]].map(p => p.getFullDocument)
    .map(_.filterKeys(key => key != "_id"))
  val printChangeStreamDoc = Sink.foreach[Any](println)

  def watchMoreCollectionsGraph(a: String) = {
    val full_collection: MongoCollection[Document] = dbfull.getCollection(a)
    val updates_collection: MongoCollection[Document] = dbupdates.getCollection(a)
    val full_mongoSink = MongoSink.insertOne(1, full_collection)
    val updates_mongoSink = MongoSink.insertOne(1, updates_collection)
    val mongoColl = dbMeteor.getCollection(a)

    val full_source: Source[changestream.ChangeStreamDocument[Document], NotUsed] = MongoSource(mongoColl
      .watch(List(Aggregates.`match`(Filters.and(Filters.eq("documentKey._id", "history"),
        //        Filters.exists("updateDescription.updatedFields.ts",false),
        Filters.in("operationType", "update")))))
      .fullDocument(FullDocument.UPDATE_LOOKUP))

    val updates_source: Source[changestream.ChangeStreamDocument[Document], NotUsed] = MongoSource(mongoColl
      .watch(List(Aggregates.`match`(Filters.and(Filters.ne("documentKey._id", "history"),
        //        Filters.exists("updateDescription.updatedFields.ts",false),
        Filters.in("operationType", "update")))))
      .fullDocument(FullDocument.UPDATE_LOOKUP))

    val graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      //      val bcast = builder.add(Broadcast[changestream.ChangeStreamDocument[Document]](2))

      full_source ~> docConv ~> full_mongoSink
      updates_source ~> docConv ~> updates_mongoSink
      ClosedShape
    })
    graph.run()
  }

  def ldcProtocolConversionGraph(queueName: String) = {

    val collectionName = queueName.replaceAllLiterally("_reply", "")
    val mongoCollection = dbMeteor.getCollection(collectionName)
    val mongoHistCollection = dbfull.getCollection(collectionName)
    val mongoUpdatesCollection = dbupdates.getCollection(collectionName)
    println(collectionName)

    val sourceFull: Source[changestream.ChangeStreamDocument[Document], NotUsed] = MongoSource(mongoCollection
      .watch(List(Aggregates.`match`(Filters.and(Filters.eq("documentKey._id", "history"),
        Filters.in("operationType", "update")))))
      .fullDocument(FullDocument.UPDATE_LOOKUP))

    val sourceUpdates: Source[changestream.ChangeStreamDocument[Document], NotUsed] = MongoSource(mongoCollection
      .watch(List(Aggregates.`match`(Filters.and(Filters.ne("documentKey._id", "history"),
        //        Filters.exists("updateDescription.updatedFields.ts",false),
        Filters.in("operationType", "update")))))
      .fullDocument(FullDocument.UPDATE_LOOKUP))

    val insertFullDoc = MongoSink.insertOne(1, mongoHistCollection)
    val insertUpdatesDoc = MongoSink.insertOne(1, mongoUpdatesCollection)
    val updateDoc: Sink[DocumentUpdate, Future[Done]] = MongoSink.updateMany(1, mongoCollection)

    val tsFlow = Flow[IncomingMessage].map(msg => msg.properties
      .getHeaders.get("timestamp_in_ms"))
      .map(l => updatetimestamp(collectionName, l.asInstanceOf[Long]))

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

  //
  def integer(hex: String) = {
    Integer.parseInt(hex, 16)
  }

  val connectionFlowStart: Flow[HttpRequest,
    HttpResponse,
    Future[Http.OutgoingConnection]] = Http().outgoingConnection("192.168.11.121", 15672)

  val connectionFlowMongoandRmq = Http().cachedHostConnectionPool[NotUsed]("192.168.11.121",
    15672).mapAsync(1) {

    // Able to reach the API.
    case (Success(HttpResponse(StatusCodes.OK, _, entity, _)), _) =>
      // Unmarshal the json response.
      Unmarshal(entity).to[String]
        .map(jsonString => parse(jsonString))
        .map(x => render((x removeField {
          _ == JField("name", JString("1023"))
        }) \ "name"))
        .map(_.extract[List[String]])
        .map(_.filter(_.endsWith("_reply")))
        .map(theUpdates(_))

    // Failed to reach the API.
    case (Success(HttpResponse(code, _, entity, _)), _) =>
      entity.discardBytes()
      Future.successful(code.toString())

    case (Failure(e), _) ⇒
      throw e
  }

  val authorization = headers.Authorization(BasicHttpCredentials("ldc_proj", "lc56&49*pr"))
  val responseFuture = Source.single(HttpRequest(HttpMethods.GET,
    uri = Uri("/api/queues"),
    headers = List(authorization)))
    .viaMat(connectionFlowStart)(Keep.right)
    .flatMapConcat(_.entity.dataBytes)
    .fold(ByteString.empty)(_ ++ _)
    .map(_.utf8String)
    .map(jsonString => parse(jsonString))
    .map(x => render((x removeField {
      _ == JField("name", JString("1023"))
    }) \ "name"))
    .map(_.extract[List[String]]).map(_.filter(_.endsWith("_reply")))

  val responseFuturerepeat = Source.tick(1 second,
    3 second,
    (HttpRequest(HttpMethods.GET, uri = Uri("/api/queues"), headers = List(authorization))))
    .map(_ → NotUsed)

  val uri = "amqp://ldc_proj:lc56&49*pr@172.18.0.2:5672/ldc_proj"
  val amqpUri = AmqpUriConnectionProvider(uri)


  val yeah = List("LDCE_Test_reply_status_L", "20180413101010_reply_status_L", "numbers", "pizza")

  val driver = new MongoDriver
  val connection = driver.connection(List("172.18.0.5:27017"))
  val meteordb = connection.database("meteor")
  val historydb = connection.database("full")
  val historydb_updates = connection.database("updates")


  def createMeteorCollections(col: String): Unit = {
    meteordb.map(x => x.collection(col).create())

    meteordb.map(x => x.collection[BSONCollection](col)
      .insert[BSONDocument](false)
      .many(documents))
  }

  def createHistoryCollections(col: String): Unit = {
    historydb.map(x => x.collection(col).create())
    historydb_updates.map(x => x.collection(col).create())
  }

  def theUpdates(ql: List[String]) {
    val q = ql.map(_.replaceAllLiterally("_reply", ""))
    val meteorresult = meteordb.flatMap(x => x.collectionNames)
    val historyresult = historydb.flatMap(x => x.collectionNames);
    //    val upresult = historydb_updates.flatMap(x => x.collectionNames)
    //    val finalResult = ql.filterNot(result.contains(_))

    //    println(result)
    meteorresult onComplete {
      case Success(r) =>
        q.filterNot(r.contains(_))
          .map(x => s"$x" + "_reply")
          .map(ldcProtocolConversionGraph(_))
        q.filterNot(r.contains(_))
          .map(createMeteorCollections(_))
        println("result: " + r)
      case Failure(t) =>
        println("fail: " + t)
    }
    historyresult onComplete {
      case Success(r) =>
        q.filterNot(r.contains(_)).map(createHistoryCollections(_))
        q.filterNot(r.contains(_)).map(watchMoreCollectionsGraph(_))
        println("result: " + r)
      case Failure(t) =>
        println("fail: " + t)
    }
  }

  def updatetimestamp(col: String, l: Long): Unit = {
    val modifier = BSONDocument(
      "$set" -> BSONDocument(
        "ts" -> BSONDateTime(l)))
    val query = BSONDocument("ts" -> BSONDocument("$exists" -> true))
    meteordb.map(x => x.collection[BSONCollection](col)
      .update(ordered = false).one(query, modifier, upsert = false, multi = false)
    )
  }

  val c_car_floor_index_value = Flow[ByteString].map(_.apply(2))
    .map(i =>
      DocumentUpdate(filter = Filters.exists("floor_index"),
        update = Updates.set("floor_index", i)))

  val c_door_value: PartialFunction[ByteString, String] = {
    case _ +: _ +: _ +: 0x07 +: _ => "all_doors";
    case _ +: _ +: _ +: 0x01 +: _ => "door_1";
    case _ +: _ +: _ +: 0x02 +: _ => "door_2";
    case _ +: _ +: _ +: 0x03 +: _ => "door_3";
  }
  val c_door_status = Flow[ByteString].collect(c_door_value)
    .map(s => DocumentUpdate(filter = Filters.exists("door_status"),
      update = Updates.set("door_status", s)))
  val c_activation_value: PartialFunction[Byte, Int] = {
    case 0x00 => 0;
    case 0x01 => 1;
  }

  val flow1 = Flow[IncomingMessage].map[(ByteString, Any)](msg => (msg.bytes,
    msg.properties
      .getHeaders
      .get("timestamp_in_ms")
      .asInstanceOf[Long])
  )

  val ele_mode_value = Flow[(ByteString, Any)]
    .sliding(2, 1).filter(bls => bls(0)._1(2) != bls(1)._1(2)).map(bls => DocumentUpdate(
    filter = Filters.and(
      Filters.exists("mode"),
      Filters.exists("ts")),
    update = Updates.combine(
      Updates.set("mode", java.lang.Byte.toUnsignedInt(bls(1)._1(2))),
      Updates.set("ts", bls(1)._2)
    )))

  val pdo_out_value = Flow[(ByteString, Any)]
    .map[DocumentUpdate](bl => DocumentUpdate(
    filter = Filters.and(
      Filters.exists("po_stat"),
      Filters.exists("ts")),
    update = Updates.combine(
      Updates.set("po_stat.po", java.lang.Byte.toUnsignedInt(bl._1(2))),
      Updates.set("po_stat.po_act", java.lang.Byte.toUnsignedInt(bl._1(3))),
      Updates.set("ts", bl._2)
    )))

  val ele_status_value = Flow[(ByteString, Any)]
    .map[DocumentUpdate](bl => DocumentUpdate(
    filter = Filters.and(
      Filters.exists("state"),
      Filters.exists("ts")),
    update = Updates.combine(
      Updates.set("state", java.lang.Byte.toUnsignedInt(bl._1(2))),
      Updates.set("ts", bl._2)
    )))

  val floor_value = Flow[(ByteString, Any)]
    .map[DocumentUpdate](bl => DocumentUpdate(
    filter = Filters.and(
      Filters.exists("flr"),
      Filters.exists("ts")),
    update = Updates.combine(
      Updates.set("flr.idx", java.lang.Byte.toUnsignedInt(bl._1(2))),
      Updates.set("flr.flr_n", bl._1.slice(4, 6).utf8String.trim),
      Updates.set("ts", bl._2)
    )))

  val speed_height_value = Flow[(ByteString, Any)]
    .map[DocumentUpdate](bl => DocumentUpdate(
    filter = Filters.and(
      Filters.exists("spd_ht"),
      Filters.exists("ts")),
    update = Updates.combine(
      Updates.set("spd_ht.spd", dealData.speed_value(bl._1)),
      Updates.set("spd_ht.ht", dealData.height_value(bl._1)),
      Updates.set("ts", bl._2)
    )))

  val direction_value = Flow[(ByteString, Any)]
    .map[DocumentUpdate](bl => DocumentUpdate(
    filter = Filters.and(
      Filters.exists("dir"),
      Filters.exists("ts")),
    update = Updates.combine(
      Updates.set("dir", lift_direction(bl._1)),
      Updates.set("ts", bl._2)
    )))

  val door_status = Flow[(ByteString, Any)]
    .map[DocumentUpdate](bl => DocumentUpdate(
    filter = Filters.and(
      Filters.exists("dr_stat"),
      Filters.exists("ts")),
    update = Updates.combine(
      Updates.set("dr_stat", door_value(bl._1)),
      Updates.set("ts", bl._2)
    )))

  val registered_landing_call_value = Flow[(ByteString, Any)]
    .map[DocumentUpdate](bl => DocumentUpdate(
    filter = Filters.and(
      Filters.exists("lc"),
      Filters.exists("ts")),
    update = Updates.combine(
      Updates.set("lc.lc_idx", java.lang.Byte.toUnsignedInt(bl._1(2))),
      Updates.set("lc.lc_stat", landing_call_value(bl._1(3))),
      Updates.set("ts", bl._2)
    )))

  val registered_car_call_value = Flow[(ByteString, Any)]
    .map[DocumentUpdate](bl => DocumentUpdate(
    filter = Filters.and(
      Filters.exists("cc"),
      Filters.exists("ts")),
    update = Updates.combine(
      Updates.set("cc.cc_idx", java.lang.Byte.toUnsignedInt(bl._1(2))),
      Updates.set("cc.cc_stat", car_call_value(bl._1(3))),
      Updates.set("ts", bl._2)
    )))

  val door_button_value = Flow[(ByteString, Any)]
    .map[DocumentUpdate](bl => DocumentUpdate(
    filter = Filters.and(
      Filters.exists("btn_stat"),
      Filters.exists("ts")),
    update = Updates.combine(
      Updates.set("btn_stat.btn_typ", button_value(bl._1(2))),
      Updates.set("btn_stat.btn_act", button_activation_value(bl._1(3))),
      Updates.set("ts", bl._2)
    )))

  val upls_activation = Flow[(ByteString, Any)]
    .map[DocumentUpdate](bl => DocumentUpdate(
    filter = Filters.and(
      Filters.exists("sw1.0"),
      Filters.exists("ts")),
    update = Updates.combine(
      Updates.set("sw1.0", switch2_value(bl._1(3))),
      Updates.set("ts", bl._2)
    )))

  val sst1_activation = Flow[(ByteString, Any)]
    .map[DocumentUpdate](bl => DocumentUpdate(
    filter = Filters.and(
      Filters.exists("sw1.1"),
      Filters.exists("ts")),
    update = Updates.combine(
      Updates.set("sw1.1", switch2_value(bl._1(3))),
      Updates.set("ts", bl._2)
    )))

  val sst2_activation = Flow[(ByteString, Any)]
    .map[DocumentUpdate](bl => DocumentUpdate(
    filter = Filters.and(
      Filters.exists("sw1.2"),
      Filters.exists("ts")),
    update = Updates.combine(
      Updates.set("sw1.2", switch2_value(bl._1(3))),
      Updates.set("ts", bl._2)
    )))

  val lpls_activation = Flow[(ByteString, Any)]
    .map[DocumentUpdate](bl => DocumentUpdate(
    filter = Filters.and(
      Filters.exists("sw1.3"),
      Filters.exists("ts")),
    update = Updates.combine(
      Updates.set("sw1.3", switch2_value(bl._1(3))),
      Updates.set("ts", bl._2)
    )))
  val documents = Stream(
    BSONDocument(
      "_id" -> "pdo_o",
      "po_stat" -> BSONDocument("po" -> -1,
        "po_act" -> -1),
      "ts" -> -1
    ),
    BSONDocument(
      "_id" -> "mode",
      "mode" -> -1,
      "ts" -> -1
    ),
    BSONDocument(
      "_id" -> "state",
      "state" -> -1,
      "ts" -> -1
    ),
    BSONDocument(
      "_id" -> "flr",
      "flr" -> BSONDocument(
        "idx" -> -1,
        "flr_n" -> ""
      ),
      "ts" -> -1
    ),
    BSONDocument(
      "_id" -> "spd_ht",
      "spd_ht" -> BSONDocument(
        "spd" -> -1,
        "ht" -> -1,
      ),
      "ts" -> -1
    ),
    BSONDocument(
      "_id" -> "dir",
      "dir" -> "",
      "ts" -> -1
    ),
    BSONDocument(
      "_id" -> "dr",
      "dr_stat" -> "",
      "ts" -> -1
    ),
    BSONDocument(
      "_id" -> "reg_lc",
      "lc" -> BSONDocument(
        "lc_idx" -> -1,
        "lc_stat" -> ""
      ),
      "ts" -> -1
    ),
    BSONDocument(
      "_id" -> "reg_cc",
      "cc" -> BSONDocument(
        "cc_idx" -> -1,
        "cc_stat" -> ""
      ),
      "ts" -> -1
    ),
    BSONDocument(
      "_id" -> "dr_btn",
      "btn_stat" -> BSONDocument("btn_typ" -> "",
        "btn_act" -> -1),
      "ts" -> -1
    ),
    BSONDocument(
      "_id" -> "sc",
      "sc" -> BSONArray(-1, -1, -1, -1, -1, -1),
      "ts" -> -1
    ),
    BSONDocument(
      "_id" -> "sw1",
      "sw1" -> BSONArray(-1, -1, -1, -1, -1, -1),
      "ts" -> -1
    ),
    BSONDocument(
      "_id" -> "sw2",
      "sw2" -> BSONArray(-1, -1, -1),
      "ts" -> -1
    ),

    BSONDocument(
      "_id" -> "run_fl",
      "run_fl" -> BSONDocument("st_idx" -> -1,
        "tgt_idx" -> -1),
      "ts" -> -1
    ),

    BSONDocument(
      "_id" -> "err",
      "err" -> BSONDocument("typ" -> -1, "driv" -> -1, "ld" -> -1, "ht" -> -1, "spd" -> -1),
      "ts" -> -1
    ),

    BSONDocument(
      "_id" -> "elev",
      "elev" -> -1,
      "ts" -> -1
    ),

    BSONDocument(

      "_id" -> "history",

      "po_stat" -> BSONDocument("po" -> -1,
        "po_act" -> -1),

      "mode" -> -1,

      "state" -> -1,

      "flr" -> BSONDocument(
        "idx" -> -1,
        "flr_n" -> ""
      ),

      "spd_ht" -> BSONDocument(
        "spd" -> -1,
        "ht" -> -1,
      ),

      "dir" -> "",

      "dr_stat" -> "",

      "lc" -> BSONDocument(
        "lc_idx" -> -1,
        "lc_stat" -> ""
      ),

      "cc" -> BSONDocument(
        "cc_idx" -> -1,
        "cc_stat" -> ""
      ),

      "btn_stat" -> BSONDocument("btn_typ" -> "",
        "btn_act" -> -1),


      "sc" -> BSONArray(-1, -1, -1, -1, -1, -1),

      "sw1" -> BSONArray(-1, -1, -1, -1, -1, -1),

      "sw2" -> BSONArray(-1, -1, -1),

      "run_fl" -> BSONDocument("st_idx" -> -1, "tgt_idx" -> -1),

      "err" -> BSONDocument("typ" -> -1, "driv" -> -1, "ld" -> -1, "ht" -> -1, "spd" -> -1),

      "elev" -> -1,

      "ts" -> -1
    )
  )
  val ssb1_activation = Flow[(ByteString, Any)]
    .map[DocumentUpdate](bl => DocumentUpdate(
    filter = Filters.and(
      Filters.exists("sw1.4"),
      Filters.exists("ts")),
    update = Updates.combine(
      Updates.set("sw1.4", switch2_value(bl._1(3))),
      Updates.set("ts", bl._2)
    )))

  val ssb2_activation = Flow[(ByteString, Any)]
    .map[DocumentUpdate](bl => DocumentUpdate(
    filter = Filters.and(
      Filters.exists("sw1.5"),
      Filters.exists("ts")),
    update = Updates.combine(
      Updates.set("sw1.5", switch2_value(bl._1(3))),
      Updates.set("ts", bl._2)
    )))

  val dzs_activation = Flow[(ByteString, Any)]
    .map[DocumentUpdate](bl => DocumentUpdate(
    filter = Filters.and(
      Filters.exists("sw2.0"),
      Filters.exists("ts")),
    update = Updates.combine(
      Updates.set("sw2.0", switch2_value(bl._1(3))),
      Updates.set("ts", bl._2)
    )))

  val usi_activation = Flow[(ByteString, Any)]
    .map[DocumentUpdate](bl => DocumentUpdate(
    filter = Filters.and(
      Filters.exists("sw2.1"),
      Filters.exists("ts")),
    update = Updates.combine(
      Updates.set("sw2.1", switch2_value(bl._1(3))),
      Updates.set("ts", bl._2)
    )))

  val lsi_activation = Flow[(ByteString, Any)]
    .map[DocumentUpdate](bl => DocumentUpdate(
    filter = Filters.and(
      Filters.exists("sw2.2"),
      Filters.exists("ts")),
    update = Updates.combine(
      Updates.set("sw2.2", switch2_value(bl._1(3))),
      Updates.set("ts", bl._2)
    )))

  val err = Flow[(ByteString, Any)]
    .map[DocumentUpdate](bl => DocumentUpdate(
    filter = Filters.and(
      Filters.exists("err"),
      Filters.exists("ts")),
    update = Updates.combine(
      Updates.set("err.typ", dealData.hex2decimal(dealData.byte2String(bl._1(2)) + dealData.byte2String(bl._1(3)))),
      Updates.set("err.driv", java.lang.Byte.toUnsignedInt(bl._1(4))),
      Updates.set("err.ld", java.lang.Byte.toUnsignedInt(bl._1(5))),
      Updates.set("err.spd", dealData.err_speed_value(bl._1)),
      Updates.set("err.ht", dealData.err_height_value(bl._1)),
      Updates.set("ts", bl._2)
    )))

  val run_floor_index_value = Flow[(ByteString, Any)]
    .map[DocumentUpdate](bl => DocumentUpdate(
    filter = Filters.and(
      Filters.exists("run_fl"),
      Filters.exists("ts")),
    update = Updates.combine(
      Updates.set("run_fl.st_idx", java.lang.Byte.toUnsignedInt(bl._1(2))),
      Updates.set("run_fl.tgt_idx", java.lang.Byte.toUnsignedInt(bl._1(3))),
      Updates.set("ts", bl._2)
    )))

  val lift_direction: PartialFunction[ByteString, String] = {
    case _ +: 0x75 +: 0x00 +: _ => "still";
    case _ +: 0x75 +: 0x01 +: _ => "up";
    case _ +: 0x75 +: 0x02 +: _ => "dwn";
  }

  val door_value: PartialFunction[ByteString, String] = {
    case _ +: 0x76 +: 0x00 +: _ => "clsg";
    case _ +: 0x76 +: 0x01 +: _ => "cl";
    case _ +: 0x76 +: 0x02 +: _ => "opng";
    case _ +: 0x76 +: 0x03 +: _ => "op";
    case _ +: 0x76 +: 0x04 +: _ => "rvsg";
    case _ +: 0x76 +: 0x05 +: _ => "err";
    case _ +: 0x76 +: 0x06 +: _ => "stop";
  }

  val landing_call_value: PartialFunction[Byte, String] = {
    case 0x00 => "up_1";
    case 0x01 => "up_0";
    case 0x10 => "dwn_1";
    case 0x11 => "dwn_0";
  }

  val car_call_value: PartialFunction[Byte, String] = {
    case 0x20 => "cc_1";
    case 0x21 => "cc_0";
  }

  val button_value: PartialFunction[Byte, String] = {
    case 0x00 => "op";
    case 0x01 => "cl";
    case 0x02 => "stop";
  }

  val button_activation_value: PartialFunction[Byte, Int] = {
    case 0x00 => 0;
    case 0x01 => 1;
  }

  val switch2_value: PartialFunction[Byte, Int] = {
    case 0x00 => 0;
    case 0x01 => 1;
  }

  val switch1_type_value = Flow[ByteString].map(_.apply(2)).map(java.lang.Byte.toUnsignedInt(_))
    .map(i => DocumentUpdate(filter = Filters.exists("sw_stat.1_typ"),
      update = Updates.set("sw_stat.1_typ", i)))

  val switch1_activation_value = Flow[ByteString].map(_.apply(3)).map(java.lang.Byte.toUnsignedInt(_))
    .map(i => DocumentUpdate(filter = Filters.exists("sw_stat.1_act"),
      update = Updates.set("sw_stat.1_act", i)))

  val sc1_1 = Flow[(ByteString, Any)]
    .map(bl => DocumentUpdate(
      filter = Filters.and(
        Filters.exists("sc.0"),
        Filters.exists("ts")),
      update = Updates.combine(
        Updates.set("sc.0", java.lang.Byte.toUnsignedInt(bl._1(2))),
        Updates.set("ts", bl._2)
      )))

  val sc1_2 = Flow[(ByteString, Any)]
    .map(bl => DocumentUpdate(
      filter = Filters.and(
        Filters.exists("sc.1"),
        Filters.exists("ts")),
      update = Updates.combine(
        Updates.set("sc.1", java.lang.Byte.toUnsignedInt(bl._1(3))),
        Updates.set("ts", bl._2)
      )))

  val sc1_3 = Flow[(ByteString, Any)]
    .map(bl => DocumentUpdate(
      filter = Filters.and(
        Filters.exists("sc.2"),
        Filters.exists("ts")),
      update = Updates.combine(
        Updates.set("sc.2", java.lang.Byte.toUnsignedInt(bl._1(4))),
        Updates.set("ts", bl._2)
      )))

  val sc2 = Flow[(ByteString, Any)]
    .map(bl => DocumentUpdate(
      filter = Filters.and(
        Filters.exists("sc.3"),
        Filters.exists("ts")),
      update = Updates.combine(
        Updates.set("sc.3", java.lang.Byte.toUnsignedInt(bl._1(5))),
        Updates.set("ts", bl._2)
      )))

  val sc3 = Flow[(ByteString, Any)]
    .map(bl => DocumentUpdate(
      filter = Filters.and(
        Filters.exists("sc.4"),
        Filters.exists("ts")),
      update = Updates.combine(
        Updates.set("sc.4", java.lang.Byte.toUnsignedInt(bl._1(6))),
        Updates.set("ts", bl._2)
      )))

  val a3 = Flow[(ByteString, Any)]
    .map(bl => DocumentUpdate(
      filter = Filters.and(
        Filters.exists("sc.5"),
        Filters.exists("ts")),
      update = Updates.combine(
        Updates.set("sc.5", java.lang.Byte.toUnsignedInt(bl._1(7))),
        Updates.set("ts", bl._2)
      )))

  val onoroff = Flow[Int]
    .sliding(5)
    .collect({ case Seq(0, 0, 0, 0, 0) => 0;
    case _ => 1
    }).map(i =>
    DocumentUpdate(filter = Filters.exists("elev"),
      update = Updates.set("elev", i)))

  val prinSink = Sink.foreach[Any](println)
  val sink = Sink.foreach[String](println)
  val sinkcoll = Sink.foreach[String](println)

  val sin = Sink.ignore
  val rmqtomongo = Sink.foreach[List[String]]((_.map(ldcProtocolConversionGraph(_))))
  val mongoCollections = Sink.foreach[List[String]](theUpdates(_))

  val graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._
    responseFuture ~> rmqtomongo
    responseFuturerepeat ~> connectionFlowMongoandRmq ~> sin
    ClosedShape
  })
  graph.run()
}
