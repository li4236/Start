package test

import amqpMongo.Main.{createMeteorCollections, ldcProtocolConversionGraph, meteordb}
import org.mongodb.scala.MongoClient
import reactivemongo.api.MongoDriver
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.WriteResult
import reactivemongo.bson._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import reactivemongo.bson.BSONDocument
import reactivemongo.api.commands.{MultiBulkWriteResult, WriteResult}
import reactivemongo.api.collections.bson.BSONCollection
object MongoDbTest {


  //  val client = MongoClient(s"mongodb://192.168.0.105:27017")
  //  val dbMeteor = client.getDatabase("meteor")
  //  val dbfull = client.getDatabase("full")
  //  val dbupdates = client.getDatabase("updates")
  def main(args: Array[String]): Unit = {


    initMongoDb()
    //    initMongo()

    println("测试")
  }


  def initMongoDb(): Unit = {
    val driver = new MongoDriver
    val connection = driver.connection(List("192.168.0.105:27017"))

    val meteordb = connection.database("abce")
    meteordb.map(x => x.collection("abce").create())

    val meteordb1 = connection.database("abcef")


    meteordb1.map(x => x.collection("ab").create())
    //创建一个新的文档，用来插入
    val newPlayerDoc = BSONDocument(
      "_id" -> BSONObjectID.generate, //ID类型

      "clientVersion" -> "1.1.1", //字符串类型
      "channel" -> "000255",
      "state" -> BSONDocument( //state是个内嵌文档
        "name" -> "Libai1",
        "gold" -> 1000 //整数类型
      )
    )

    meteordb1.map(x => x.collection[BSONCollection]("ab")
      .insert(newPlayerDoc))


    val query = BSONDocument("clientVersion" -> "1.1.1")

    val peopleOlderThanTwentySeven = meteordb1.map(x => x.collection[BSONCollection]("ab")
      .find(query));

    peopleOlderThanTwentySeven onComplete{
      case Success(r) =>

        println("result: " + r)
    }

    println("----"+peopleOlderThanTwentySeven);



//    val writeRes: Future[WriteResult] =
//    coll.insert[BSONDocument](ordered = false).one(document1)
//
//    writeRes.onComplete { // Dummy callbacks
//      case Failure(e) => e.printStackTrace()
//      case Success(writeResult) =>
//        println(s"successfully inserted document with result: $writeResult")
//    }
//
//    writeRes.map(_ => {}) // in this example, do nothing with the success
//



//    meteordb.insert(newPlayerDoc) map { result =>
//      println("create doc "+result.ok)
//    }
    //    val historydb = connection.database("full")
    //    val historydb_updates = connection.database("updates")
    println("测试")
  }

  def initMongo(): Unit = {
    val client = MongoClient(s"mongodb://192.168.0.105:27017")
    val dbMeteor = client.getDatabase("meteor")
    val dbfull = client.getDatabase("full")
    val dbupdates = client.getDatabase("updates")
  }
}

