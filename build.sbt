lazy val commonSettings = Seq(
  organization := "br.bireme",
  version := "0.1.0",
  scalaVersion := "2.12.1"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "DCDup"
  )

val luceneVersion = "6.5.0"
val jacksonVersion = "2.7.8"
val httpClientVersion = "4.5.2"
val mySQLVersion = "5.1.41"
//val mySQLVersion = "6.0.6"
val logBackVersion = "1.2.2"
val circeParserVersion = "0.7.1"

libraryDependencies ++= Seq(
  "org.apache.lucene" % "lucene-analyzers-common" % luceneVersion,
  "org.apache.lucene" % "lucene-core" % luceneVersion,
  "org.apache.lucene" % "lucene-queryparser" % luceneVersion,
  "org.apache.lucene" % "lucene-suggest" % luceneVersion,
  "org.apache.lucene" % "lucene-backward-codecs" % luceneVersion,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "org.apache.httpcomponents" % "httpclient" % httpClientVersion,
  "mysql" % "mysql-connector-java" % mySQLVersion,
  "ch.qos.logback" % "logback-classic" % logBackVersion,
  "io.circe" % "circe-parser_2.12" % circeParserVersion
)

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")
