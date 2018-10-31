


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
object MongoDb extends App {


  private val client = MongoClient(s"mongodb://172.18.0.5:27017")
  private val dbMeteor = client.getDatabase("meteor")
  private val dbfull = client.getDatabase("full")
  private val dbupdates = client.getDatabase("updates")
//  val driver = new MongoDriver
//  val connection = driver.connection(List("192.168.11.121:27017"))
//  val meteordb = connection.database("meteor")
//  val historydb = connection.database("full")
//  val historydb_updates = connection.database("updates")
}
