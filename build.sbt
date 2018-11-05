name := "Start"

version := "0.1"

scalaVersion := "2.12.7"
resolvers += "Sonatype Snapshots" at "http://repo.typesafe.com/typesafe/releases/"
libraryDependencies ++= Seq(
  "org.json4s" %% "json4s-jackson" % "3.5.4",
  "com.typesafe.akka" %% "akka-http"   % "10.1.1",
  "com.lightbend.akka" %% "akka-stream-alpakka-amqp" % "0.19",
  "com.lightbend.akka" %% "akka-stream-alpakka-mongodb" % "0.19",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.3.0",
  "org.reactivemongo" %% "reactivemongo" % "0.13.0",
  "com.github.sergeygrigorev" %% "gson-object-scala-syntax" % "0.3.2",
  "com.google.code.gson" % "gson" % "2.2.4",

)